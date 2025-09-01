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

-- 1) Big single-row safety net (with full batch logic)
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
    AND ((p.now_ts - q.queue_ts) > make_interval(secs => p.max_age_seconds)  -- old items
         OR (q.record_count >= trg.target_records AND q.record_count <= (trg.target_records * 1.2)))  -- fresh efficient items
  ORDER BY q.priority DESC, q.queue_ts ASC, q.id ASC
  LIMIT 1
),

-- 2) One seed per group (org, dateint, freq, instance, slot_id, slot_count, rollup_group)
seeds_per_group AS (
  SELECT DISTINCT ON (organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, rollup_group)
         id AS seed_id, organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, rollup_group,
         priority, queue_ts, record_count
  FROM metric_rollup_queue
  WHERE claimed_at IS NULL
    AND window_close_ts <= (SELECT now_ts FROM params LIMIT 1)
  ORDER BY organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, rollup_group,
           priority DESC, queue_ts ASC, id ASC
),

-- 3) Order groups globally by seed recency/priority
ordered_groups AS (
  SELECT s.*,
         ROW_NUMBER() OVER (ORDER BY s.priority DESC, s.queue_ts ASC, s.seed_id ASC) AS seed_rank
  FROM seeds_per_group s
),

-- 4) Attach per-group target_records with estimate tracking
group_flags AS (
  SELECT
    og.organization_id, og.dateint, og.frequency_ms, og.instance_num, og.slot_id, og.slot_count, og.rollup_group,
    og.priority, og.queue_ts, og.seed_rank,
    ((p.now_ts - og.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old,
    COALESCE(e_org.target_records, e_glob.target_records, p.default_target_records)::bigint AS target_records,
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
),

-- 5) All ready rows within each group
grp_scope AS (
  SELECT
    q.id, q.organization_id, q.dateint, q.frequency_ms, q.instance_num,
    q.slot_id, q.slot_count, q.rollup_group, q.priority, q.queue_ts, q.record_count,
    gf.seed_rank, gf.is_old, gf.target_records, gf.batch_count,
    gf.org_estimate, gf.global_estimate, gf.default_estimate, gf.estimate_source
  FROM metric_rollup_queue q
  JOIN group_flags gf
    ON q.claimed_at   IS NULL
   AND q.window_close_ts <= gf.now_ts
   AND q.organization_id = gf.organization_id
   AND q.dateint         = gf.dateint
   AND q.frequency_ms    = gf.frequency_ms
   AND q.instance_num    = gf.instance_num
   AND q.slot_id         = gf.slot_id
   AND q.slot_count      = gf.slot_count
   AND q.rollup_group    = gf.rollup_group
),

-- 6) Greedy pack per group
pack AS (
  SELECT
    g.*,
    SUM(g.record_count) OVER (
      PARTITION BY g.organization_id, g.dateint, g.frequency_ms, g.instance_num, g.slot_id, g.slot_count, g.rollup_group
      ORDER BY g.priority DESC, g.queue_ts ASC, g.id ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_records,
    ROW_NUMBER() OVER (
      PARTITION BY g.organization_id, g.dateint, g.frequency_ms, g.instance_num, g.slot_id, g.slot_count, g.rollup_group
      ORDER BY g.priority DESC, g.queue_ts ASC, g.id ASC
    ) AS rn
  FROM grp_scope g
),

-- 7) Rows that fit under caps with overshoot protection
pack_with_limits AS (
  SELECT p.*
  FROM pack p
  JOIN group_flags gf
    ON gf.organization_id = p.organization_id
   AND gf.dateint         = p.dateint
   AND gf.frequency_ms    = p.frequency_ms
   AND gf.instance_num    = p.instance_num
   AND gf.slot_id         = p.slot_id
   AND gf.slot_count      = p.slot_count
   AND gf.rollup_group    = p.rollup_group
  WHERE p.cum_records <= (gf.target_records * 1.2)
    AND p.rn          <= gf.batch_count
),

-- 8) Check for >20% overshoot and drop last item if needed (but keep at least 1 item)
prelim AS (
  SELECT pwl.*
  FROM pack_with_limits pwl
  JOIN group_flags gf
    ON gf.organization_id = pwl.organization_id
   AND gf.dateint         = pwl.dateint
   AND gf.frequency_ms    = pwl.frequency_ms
   AND gf.instance_num    = pwl.instance_num
   AND gf.slot_id         = pwl.slot_id
   AND gf.slot_count      = pwl.slot_count
   AND gf.rollup_group    = pwl.rollup_group
  WHERE 
    -- For old groups: keep all items (no overshoot protection)
    gf.is_old
    OR
    -- For full batches: drop the last item if it causes >20% overshoot and we have >=2 items
    (NOT gf.is_old AND (
      -- Keep if we're not over 20%
      pwl.cum_records <= (gf.target_records * 1.2)
      OR
      -- Keep if we only have 1 item in this group (don't drop everything)
      (SELECT COUNT(*) FROM pack_with_limits pwl2 
       WHERE pwl2.organization_id = pwl.organization_id
         AND pwl2.dateint = pwl.dateint
         AND pwl2.frequency_ms = pwl.frequency_ms
         AND pwl2.instance_num = pwl.instance_num
         AND pwl2.slot_id = pwl.slot_id
         AND pwl2.slot_count = pwl.slot_count
         AND pwl2.rollup_group = pwl.rollup_group) <= 1
      OR
      -- Keep if we're not the last item in this group
      pwl.rn < (SELECT MAX(rn) FROM pack_with_limits pwl3
                WHERE pwl3.organization_id = pwl.organization_id
                  AND pwl3.dateint = pwl.dateint
                  AND pwl3.frequency_ms = pwl.frequency_ms
                  AND pwl3.instance_num = pwl.instance_num
                  AND pwl3.slot_id = pwl.slot_id
                  AND pwl3.slot_count = pwl.slot_count
                  AND pwl3.rollup_group = pwl.rollup_group)
    ))
),

-- 9) Totals per group with full scope for availability calculation
group_availability AS (
  SELECT
    g.organization_id, g.dateint, g.frequency_ms, g.instance_num, g.slot_id, g.slot_count, g.rollup_group,
    COALESCE(SUM(g.record_count), 0) AS total_available_records,
    MIN(g.seed_rank) AS seed_rank
  FROM grp_scope g
  GROUP BY g.organization_id, g.dateint, g.frequency_ms, g.instance_num, g.slot_id, g.slot_count, g.rollup_group
),

-- 10) Totals per group from what fits under caps
prelim_stats AS (
  SELECT
    organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, rollup_group,
    COUNT(*) AS n_rows,
    COALESCE(SUM(record_count), 0) AS total_records,
    MIN(seed_rank) AS seed_rank
  FROM prelim
  GROUP BY organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, rollup_group
),

-- 11) Eligibility: full batch logic (old items OR efficient fresh batches)
eligible_groups AS (
  SELECT
    ga.organization_id, ga.dateint, ga.frequency_ms, ga.instance_num, 
    ga.slot_id, ga.slot_count, ga.rollup_group, ga.seed_rank,
    gf.target_records, gf.is_old, ga.total_available_records,
    CASE 
      WHEN gf.is_old THEN true
      WHEN ga.total_available_records >= gf.target_records THEN true
      ELSE false
    END AS is_eligible,
    CASE 
      WHEN gf.is_old THEN 'old'
      WHEN ga.total_available_records >= gf.target_records THEN 'full_batch'
      ELSE 'insufficient'
    END AS eligibility_reason
  FROM group_availability ga
  JOIN group_flags gf
    ON gf.organization_id = ga.organization_id
   AND gf.dateint         = ga.dateint
   AND gf.frequency_ms    = ga.frequency_ms
   AND gf.instance_num    = ga.instance_num
   AND gf.slot_id         = ga.slot_id
   AND gf.slot_count      = ga.slot_count
   AND gf.rollup_group    = ga.rollup_group
  JOIN prelim_stats ps
    ON ps.organization_id = ga.organization_id
   AND ps.dateint         = ga.dateint
   AND ps.frequency_ms    = ga.frequency_ms
   AND ps.instance_num    = ga.instance_num
   AND ps.slot_id         = ga.slot_id
   AND ps.slot_count      = ga.slot_count
   AND ps.rollup_group    = ga.rollup_group
  WHERE ps.total_records > 0
    AND (gf.is_old 
         OR ga.total_available_records >= gf.target_records)
),

-- 12) Pick earliest eligible group
winner_group AS (
  SELECT eg.*
  FROM eligible_groups eg
  WHERE eg.is_eligible = true
    AND eg.seed_rank = (SELECT MIN(seed_rank) FROM eligible_groups WHERE is_eligible = true)
),

-- 13) Rows to claim for the winner group
group_chosen AS (
  SELECT pr.id, w.eligibility_reason
  FROM prelim pr
  JOIN winner_group w
    ON w.organization_id = pr.organization_id
   AND w.dateint         = pr.dateint
   AND w.frequency_ms    = pr.frequency_ms
   AND w.instance_num    = pr.instance_num
   AND w.slot_id         = pr.slot_id
   AND w.slot_count      = pr.slot_count
   AND w.rollup_group    = pr.rollup_group
),

-- 14) Final chosen IDs with batch reason
chosen AS (
  SELECT 
    bs.id,
    CASE 
      WHEN (p.now_ts - q.queue_ts) > make_interval(secs => p.max_age_seconds) THEN 'old'
      ELSE 'big_single' 
    END AS batch_reason
  FROM big_single bs
  JOIN metric_rollup_queue q ON q.id = bs.id
  CROSS JOIN params p
  UNION ALL
  SELECT id, eligibility_reason AS batch_reason FROM group_chosen
  WHERE NOT EXISTS (SELECT 1 FROM big_single)
),

-- 15) Atomic optimistic claim
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
  COALESCE(pr.target_records, 0) AS used_target_records,
  COALESCE(pr.org_estimate, 0) AS org_estimate,
  COALESCE(pr.global_estimate, 0) AS global_estimate, 
  COALESCE(pr.default_estimate, 0) AS default_estimate,
  COALESCE(pr.estimate_source, 'unknown') AS estimate_source,
  COALESCE(c.batch_reason, 'unknown') AS batch_reason
FROM upd
LEFT JOIN prelim pr ON upd.id = pr.id
LEFT JOIN chosen c ON upd.id = c.id
ORDER BY upd.priority DESC, upd.queue_ts ASC, upd.id ASC;