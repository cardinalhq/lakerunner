-- name: ClaimMetricRollupWork :many
WITH
params AS (
  SELECT
    @worker_id::bigint                               AS worker_id,
    COALESCE(sqlc.narg(now_ts)::timestamptz, now())  AS now_ts,
    @default_target_records::bigint                  AS default_target_records,
    @max_age_seconds::integer                        AS max_age_seconds,
    @batch_count::integer                            AS batch_count
),

-- 1) Big single-row safety net
big_single AS (
  SELECT q.id
  FROM metric_rollup_queue q
  JOIN params p ON TRUE
  LEFT JOIN metric_pack_estimate e_org
         ON e_org.organization_id = q.organization_id
        AND e_org.frequency_ms    = q.frequency_ms
  LEFT JOIN metric_pack_estimate e_glob
         ON e_glob.organization_id = '00000000-0000-0000-0000-000000000000'::uuid
        AND e_glob.frequency_ms    = q.frequency_ms
  CROSS JOIN LATERAL (
    SELECT COALESCE(e_org.target_records, e_glob.target_records, p.default_target_records)::bigint AS target_records
  ) trg
  WHERE q.claimed_at IS NULL
    AND q.window_close_ts <= p.now_ts
    AND q.record_count >= trg.target_records
  ORDER BY q.priority DESC, q.queue_ts ASC, q.id ASC
  LIMIT 1
),

-- 2) One seed per group (org/dateint/freq/instance/slot/slot_count/rollup_group)
seeds_per_group AS (
  SELECT DISTINCT ON (organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, rollup_group)
         id AS seed_id, organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, rollup_group,
         priority, queue_ts, record_count
  FROM metric_rollup_queue
  WHERE claimed_at IS NULL
    AND window_close_ts <= (SELECT now_ts FROM params)
  ORDER BY organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, rollup_group,
           priority DESC, queue_ts ASC, id ASC
),

-- 3) Order groups globally
ordered_groups AS (
  SELECT s.*, 
         ROW_NUMBER() OVER (ORDER BY s.priority DESC, s.queue_ts ASC, s.seed_id ASC) AS seed_rank
  FROM seeds_per_group s
),

-- 4) Calculate group stats and eligibility criteria
group_analysis AS (
  SELECT
    og.organization_id, og.dateint, og.frequency_ms, og.instance_num,
    og.slot_id, og.slot_count, og.rollup_group,
    og.priority, og.queue_ts, og.seed_rank,
    ((p.now_ts - og.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old,
    COALESCE(e_org.target_records, e_glob.target_records, p.default_target_records)::bigint AS target_records,
    COALESCE(SUM(q.record_count), 0) AS total_available_records,
    COUNT(q.id) AS total_items,
    COALESCE(ps.pack_records, 0) AS packed_records,
    COALESCE(ps.pack_items, 0)   AS packed_items,
    e_org.target_records AS org_estimate,
    e_glob.target_records AS global_estimate,
    p.default_target_records AS default_estimate,
    CASE
      WHEN e_org.target_records IS NOT NULL THEN 'organization'
      WHEN e_glob.target_records IS NOT NULL THEN 'global'
      ELSE 'default'
    END AS estimate_source,
    p.batch_count,
    p.now_ts
  FROM ordered_groups og
  CROSS JOIN params p
  LEFT JOIN metric_pack_estimate e_org
         ON e_org.organization_id = og.organization_id
        AND e_org.frequency_ms    = og.frequency_ms
  LEFT JOIN metric_pack_estimate e_glob
         ON e_glob.organization_id = '00000000-0000-0000-0000-000000000000'::uuid
        AND e_glob.frequency_ms    = og.frequency_ms
  LEFT JOIN metric_rollup_queue q
         ON q.claimed_at IS NULL
        AND q.window_close_ts <= p.now_ts
        AND q.organization_id = og.organization_id
        AND q.dateint = og.dateint
        AND q.frequency_ms = og.frequency_ms
        AND q.instance_num = og.instance_num
        AND q.slot_id = og.slot_id
        AND q.slot_count = og.slot_count
        AND q.rollup_group = og.rollup_group
  LEFT JOIN LATERAL (
    SELECT
      COUNT(*) AS pack_items,
      COALESCE(SUM(record_count), 0) AS pack_records
    FROM (
      SELECT record_count,
             SUM(record_count) OVER (
               ORDER BY priority DESC, queue_ts ASC, id ASC
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             ) AS cum_records,
             ROW_NUMBER() OVER (
               ORDER BY priority DESC, queue_ts ASC, id ASC
             ) AS rn
      FROM metric_rollup_queue q2
      WHERE q2.claimed_at IS NULL
        AND q2.window_close_ts <= p.now_ts
        AND q2.organization_id = og.organization_id
        AND q2.dateint = og.dateint
        AND q2.frequency_ms = og.frequency_ms
        AND q2.instance_num = og.instance_num
        AND q2.slot_id = og.slot_id
        AND q2.slot_count = og.slot_count
        AND q2.rollup_group = og.rollup_group
    ) sub
    WHERE sub.rn <= p.batch_count
      AND sub.cum_records <= (COALESCE(e_org.target_records, e_glob.target_records, p.default_target_records)::bigint * 1.2)
  ) ps ON TRUE
  GROUP BY og.organization_id, og.dateint, og.frequency_ms, og.instance_num,
           og.slot_id, og.slot_count, og.rollup_group,
           og.priority, og.queue_ts, og.seed_rank, p.now_ts, p.max_age_seconds,
           e_org.target_records, e_glob.target_records, p.default_target_records,
           p.batch_count, ps.pack_records, ps.pack_items
),

-- 5) Determine group eligibility
eligible_groups AS (
  SELECT
    ga.*,
    CASE
      WHEN ga.is_old THEN true
      WHEN ga.packed_records >= ga.target_records THEN true
      ELSE false
    END AS is_eligible,
    CASE
      WHEN ga.is_old THEN 'old'
      WHEN ga.packed_records >= ga.target_records THEN 'full_batch'
      ELSE 'insufficient'
    END AS eligibility_reason
  FROM group_analysis ga
  WHERE ga.packed_records > 0
),

-- 6) Pick first eligible group
winner_group AS (
  SELECT eg.*
  FROM eligible_groups eg
  WHERE eg.is_eligible = true
  ORDER BY eg.seed_rank ASC
  LIMIT 1
),

-- 7) For winner group, get items in priority order and pack
group_items AS (
  SELECT
    q.id, q.organization_id, q.dateint, q.frequency_ms, q.instance_num,
    q.slot_id, q.slot_count, q.rollup_group,
    q.priority, q.queue_ts, q.record_count,
    wg.seed_rank, wg.is_old, wg.target_records, wg.batch_count,
    wg.org_estimate, wg.global_estimate, wg.default_estimate, wg.estimate_source,
    wg.eligibility_reason
  FROM metric_rollup_queue q
  JOIN winner_group wg
    ON q.claimed_at IS NULL
   AND q.window_close_ts <= wg.now_ts
   AND q.organization_id = wg.organization_id
   AND q.dateint = wg.dateint
   AND q.frequency_ms = wg.frequency_ms
   AND q.instance_num = wg.instance_num
   AND q.slot_id = wg.slot_id
   AND q.slot_count = wg.slot_count
   AND q.rollup_group = wg.rollup_group
  ORDER BY q.priority DESC, q.queue_ts ASC, q.id ASC
),

-- 8) Pack items greedily
pack AS (
  SELECT
    gi.*,
    SUM(gi.record_count) OVER (
      ORDER BY gi.priority DESC, gi.queue_ts ASC, gi.id ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_records,
    ROW_NUMBER() OVER (
      ORDER BY gi.priority DESC, gi.queue_ts ASC, gi.id ASC
    ) AS rn
  FROM group_items gi
),

-- 9) Apply limits based on eligibility reason with overshoot protection
pack_with_limits AS (
  SELECT p.*
  FROM pack p
  JOIN winner_group wg ON TRUE
  WHERE
    (wg.eligibility_reason = 'old' AND p.rn <= wg.batch_count)
    OR
    (wg.eligibility_reason = 'full_batch'
     AND p.cum_records <= (wg.target_records * 1.2)
     AND p.rn <= wg.batch_count)
),

-- 10) Drop last item if >20% overshoot (keep at least 1)
final_selection AS (
  SELECT pwl.*
  FROM pack_with_limits pwl
  JOIN winner_group wg ON TRUE
  WHERE
    wg.eligibility_reason = 'old'
    OR (
      wg.eligibility_reason = 'full_batch' AND (
        pwl.cum_records <= (wg.target_records * 1.2)
        OR (SELECT COUNT(*) FROM pack_with_limits) <= 1
        OR pwl.rn < (SELECT MAX(rn) FROM pack_with_limits)
      )
    )
),

-- 11) Final chosen IDs
chosen AS (
  SELECT id FROM big_single
  UNION ALL
  SELECT id FROM final_selection
  WHERE NOT EXISTS (SELECT 1 FROM big_single)
),

-- 12) Atomic optimistic claim
upd AS (
  UPDATE metric_rollup_queue q
  SET claimed_by = (SELECT worker_id FROM params),
      claimed_at = (SELECT now_ts FROM params),
      heartbeated_at = (SELECT now_ts FROM params)
  FROM chosen c
  WHERE q.id = c.id
    AND q.claimed_at IS NULL
  RETURNING q.*
)
SELECT
  upd.*,
  COALESCE(fs.target_records, 0) AS used_target_records,
  COALESCE(fs.org_estimate, 0) AS org_estimate,
  COALESCE(fs.global_estimate, 0) AS global_estimate,
  COALESCE(fs.default_estimate, 0) AS default_estimate,
  COALESCE(fs.estimate_source, 'unknown') AS estimate_source,
  COALESCE(fs.eligibility_reason, 'big_single') AS batch_reason
FROM upd
LEFT JOIN final_selection fs ON upd.id = fs.id
ORDER BY upd.priority DESC, upd.queue_ts ASC, upd.id ASC;

