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

-- 2) One seed per group (org, dateint, freq, instance)
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

-- 4) Attach per-group target_records
group_flags AS (
  SELECT
    og.organization_id, og.dateint, og.frequency_ms, og.instance_num,
    og.priority, og.queue_ts, og.seed_rank,
    ((p.now_ts - og.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old,
    COALESCE(e_org.target_records, e_glob.target_records, p.default_target_records)::bigint AS target_records,
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
    q.priority, q.queue_ts, q.record_count,
    gf.seed_rank, gf.is_old, gf.target_records, gf.batch_count
  FROM metric_compaction_queue q
  JOIN group_flags gf
    ON q.claimed_at   IS NULL
   AND q.organization_id = gf.organization_id
   AND q.dateint         = gf.dateint
   AND q.frequency_ms    = gf.frequency_ms
   AND q.instance_num    = gf.instance_num
),

-- 6) Greedy pack per group
pack AS (
  SELECT
    g.*,
    SUM(g.record_count) OVER (
      PARTITION BY g.organization_id, g.dateint, g.frequency_ms, g.instance_num
      ORDER BY g.priority DESC, g.queue_ts ASC, g.id ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_records,
    ROW_NUMBER() OVER (
      PARTITION BY g.organization_id, g.dateint, g.frequency_ms, g.instance_num
      ORDER BY g.priority DESC, g.queue_ts ASC, g.id ASC
    ) AS rn
  FROM grp_scope g
),

-- 7) Rows that fit under caps
prelim AS (
  SELECT p.*
  FROM pack p
  JOIN group_flags gf
    ON gf.organization_id = p.organization_id
   AND gf.dateint         = p.dateint
   AND gf.frequency_ms    = p.frequency_ms
   AND gf.instance_num    = p.instance_num
  WHERE p.cum_records <= gf.target_records
    AND p.rn          <= gf.batch_count
),

-- 8) Totals per group
prelim_stats AS (
  SELECT
    organization_id, dateint, frequency_ms, instance_num,
    COUNT(*) AS n_rows,
    COALESCE(SUM(record_count), 0) AS total_records,
    MIN(seed_rank) AS seed_rank
  FROM prelim
  GROUP BY organization_id, dateint, frequency_ms, instance_num
),

-- 9) Eligibility: fresh = exact fill, old = any positive
eligible_groups AS (
  SELECT
    gf.organization_id, gf.dateint, gf.frequency_ms, gf.instance_num, gf.seed_rank, gf.target_records
  FROM group_flags gf
  JOIN prelim_stats ps
    ON ps.organization_id = gf.organization_id
   AND ps.dateint         = gf.dateint
   AND ps.frequency_ms    = gf.frequency_ms
   AND ps.instance_num    = gf.instance_num
  WHERE (NOT gf.is_old AND ps.total_records = gf.target_records)
     OR (gf.is_old      AND ps.total_records > 0)
),

-- 10) Pick earliest eligible group
winner_group AS (
  SELECT eg.*
  FROM eligible_groups eg
  WHERE eg.seed_rank = (SELECT MIN(seed_rank) FROM eligible_groups)
),

-- 11) Rows to claim for the winner group
group_chosen AS (
  SELECT pr.id
  FROM prelim pr
  JOIN winner_group w
    ON w.organization_id = pr.organization_id
   AND w.dateint         = pr.dateint
   AND w.frequency_ms    = pr.frequency_ms
   AND w.instance_num    = pr.instance_num
),

-- 12) Final chosen IDs
chosen AS (
  SELECT id FROM big_single
  UNION ALL
  SELECT id FROM group_chosen
  WHERE NOT EXISTS (SELECT 1 FROM big_single)
),

-- 13) Atomic optimistic claim
upd AS (
  UPDATE metric_compaction_queue q
  SET claimed_by = (SELECT worker_id FROM params),
      claimed_at = (SELECT now_ts FROM params)
  FROM chosen c
  WHERE q.id = c.id
    AND q.claimed_at IS NULL
  RETURNING q.*
)
SELECT * FROM upd
ORDER BY priority DESC, queue_ts ASC, id ASC;
