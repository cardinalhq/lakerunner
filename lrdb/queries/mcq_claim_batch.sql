-- name: ClaimMetricCompactionWork :many
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
  FROM metric_compaction_queue q
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
    AND q.record_count >= trg.target_records
  ORDER BY q.priority DESC, q.queue_ts ASC, q.id ASC
  LIMIT 1
),

-- 2) One seed per group
seeds_per_group AS (
  SELECT DISTINCT ON (organization_id, dateint, frequency_ms, instance_num)
         id AS seed_id, organization_id, dateint, frequency_ms, instance_num,
         priority, queue_ts, record_count
  FROM metric_compaction_queue
  WHERE claimed_at IS NULL
  ORDER BY organization_id, dateint, frequency_ms, instance_num, priority DESC, queue_ts ASC, id ASC
),

-- 3) Order groups globally by seed recency/priority
ordered_groups AS (
  SELECT s.*,
         ROW_NUMBER() OVER (ORDER BY s.priority DESC, s.queue_ts ASC, s.seed_id ASC) AS seed_rank
  FROM seeds_per_group s
),

-- 4) Calculate group stats and eligibility criteria
group_analysis AS (
  SELECT
    og.organization_id, og.dateint, og.frequency_ms, og.instance_num,
    og.priority, og.queue_ts, og.seed_rank,
    -- Age and target calculation
    ((p.now_ts - og.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old,
    COALESCE(e_org.target_records, e_glob.target_records, p.default_target_records)::bigint AS target_records,
    -- Calculate total available records for this group
    COALESCE(SUM(q.record_count), 0) AS total_available_records,
    COUNT(*) AS total_items,
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
  -- Join with all unclaimed items in this group
  LEFT JOIN metric_compaction_queue q
         ON q.claimed_at IS NULL
        AND q.organization_id = og.organization_id
        AND q.dateint = og.dateint
        AND q.frequency_ms = og.frequency_ms
        AND q.instance_num = og.instance_num
  GROUP BY og.organization_id, og.dateint, og.frequency_ms, og.instance_num,
           og.priority, og.queue_ts, og.seed_rank, p.now_ts, p.max_age_seconds,
           e_org.target_records, e_glob.target_records, p.default_target_records,
           p.batch_count
),

-- 5) Determine group eligibility: old OR can make a full batch (target to target*2.0 for eligibility)
eligible_groups AS (
  SELECT 
    ga.*,
    CASE 
      WHEN ga.is_old THEN true
      WHEN ga.total_available_records >= ga.target_records THEN true
      ELSE false
    END AS is_eligible,
    CASE 
      WHEN ga.is_old THEN 'old'
      WHEN ga.total_available_records >= ga.target_records THEN 'full_batch'
      ELSE 'insufficient'
    END AS eligibility_reason
  FROM group_analysis ga
  WHERE ga.total_available_records > 0
),

-- 6) Pick the first eligible group (ordered by seed_rank)
winner_group AS (
  SELECT eg.*
  FROM eligible_groups eg
  WHERE eg.is_eligible = true
  ORDER BY eg.seed_rank ASC
  LIMIT 1
),

-- 7) For the winner group, get items in priority order and pack greedily
group_items AS (
  SELECT
    q.id, q.organization_id, q.dateint, q.frequency_ms, q.instance_num,
    q.priority, q.queue_ts, q.record_count,
    wg.seed_rank, wg.is_old, wg.target_records, wg.batch_count,
    wg.org_estimate, wg.global_estimate, wg.default_estimate, wg.estimate_source,
    wg.eligibility_reason
  FROM metric_compaction_queue q
  JOIN winner_group wg
    ON q.claimed_at   IS NULL
   AND q.organization_id = wg.organization_id
   AND q.dateint         = wg.dateint
   AND q.frequency_ms    = wg.frequency_ms
   AND q.instance_num    = wg.instance_num
  ORDER BY q.priority DESC, q.queue_ts ASC, q.id ASC
),

-- 8) Pack items greedily within limits
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
    -- For old batches: take up to batch_count items regardless of total records
    (wg.eligibility_reason = 'old' AND p.rn <= wg.batch_count)
    OR
    -- For full batches: stop once we exceed target_records to prevent massive overshoot
    (wg.eligibility_reason = 'full_batch' 
     AND p.cum_records <= wg.target_records
     AND p.rn <= wg.batch_count)
),

-- 10) Check for >20% overshoot and drop last item if needed (but keep at least 1 item)
final_selection AS (
  SELECT pwl.*
  FROM pack_with_limits pwl
  JOIN winner_group wg ON TRUE
  WHERE 
    -- For old batches: keep all items (no overshoot protection)
    wg.eligibility_reason = 'old'
    OR
    -- For full batches: drop the last item if it causes >20% overshoot and we have >=2 items
    (wg.eligibility_reason = 'full_batch' AND (
      -- Keep if we're not over 20%
      pwl.cum_records <= (wg.target_records * 1.2)
      OR
      -- Keep if we only have 1 item (don't drop everything)
      (SELECT COUNT(*) FROM pack_with_limits) <= 1
      OR
      -- Keep if we're not the last item
      pwl.rn < (SELECT MAX(rn) FROM pack_with_limits)
    ))
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
  UPDATE metric_compaction_queue q
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
