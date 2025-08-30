-- name: ClaimMetricCompactionWork :many
WITH
params AS (
  SELECT
    @worker_id::bigint         AS worker_id,
    COALESCE(sqlc.narg(now_ts)::timestamptz, now()) AS now_ts,
    @target_records::bigint    AS target_records,
    @max_age_seconds::integer  AS max_age_seconds,
    @batch_count::integer      AS batch_count
),

-- 1) Big single row safety-net (no locks in selection)
big_single AS (
  SELECT q.id
  FROM metric_compaction_queue q
  JOIN params p ON TRUE
  WHERE q.claimed_at IS NULL
    AND q.record_count >= p.target_records
  ORDER BY q.priority DESC, q.queue_ts ASC
  LIMIT 1
),

-- 2) Seeds: oldest/highest-priority row per (org,instance,dateint)
seeds_per_group AS (
  SELECT DISTINCT ON (organization_id, instance_num, dateint)
         id AS seed_id, organization_id, instance_num, dateint,
         priority, queue_ts, record_count
  FROM metric_compaction_queue
  WHERE claimed_at IS NULL
  ORDER BY organization_id, instance_num, dateint, priority DESC, queue_ts ASC
),

-- 3) Order groups globally by seed (priority DESC, queue_ts ASC)
ordered_groups AS (
  SELECT s.*,
         ROW_NUMBER() OVER (ORDER BY s.priority DESC, s.queue_ts ASC) AS seed_rank
  FROM seeds_per_group s
),

-- 4) Flags and parameters per group (based on the seed row)
group_flags AS (
  SELECT
    og.organization_id, og.instance_num, og.dateint,
    og.priority, og.queue_ts, og.seed_rank,
    ((p.now_ts - og.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old,
    p.target_records, p.batch_count, p.now_ts
  FROM ordered_groups og
  JOIN params p ON TRUE
),

-- 5) All ready rows in each group (ordered). No locks; just compute packs.
grp_scope AS (
  SELECT
    q.id, q.organization_id, q.instance_num, q.dateint,
    q.priority, q.queue_ts, q.record_count,
    gf.seed_rank, gf.is_old, gf.target_records, gf.batch_count
  FROM metric_compaction_queue q
  JOIN group_flags gf
    ON q.claimed_at IS NULL
   AND q.organization_id = gf.organization_id
   AND q.instance_num    = gf.instance_num
   AND q.dateint         = gf.dateint
),

-- 6) Greedy pack within each group up to target_records and batch_count
pack AS (
  SELECT
    g.*,
    SUM(g.record_count) OVER (
      PARTITION BY g.organization_id, g.instance_num, g.dateint
      ORDER BY g.priority DESC, g.queue_ts ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_records,
    ROW_NUMBER() OVER (
      PARTITION BY g.organization_id, g.instance_num, g.dateint
      ORDER BY g.priority DESC, g.queue_ts ASC
    ) AS rn
  FROM grp_scope g
),

prelim AS (
  SELECT p.*
  FROM pack p
  WHERE p.cum_records <= p.target_records
    AND p.rn          <= p.batch_count
),

-- 7) Totals per group and eligibility:
--    - fresh: exact fill (total = target)
--    - old:   any positive total (already capped by target)
prelim_stats AS (
  SELECT
    organization_id, instance_num, dateint,
    MIN(seed_rank) AS seed_rank,
    COUNT(*) AS n_rows,
    COALESCE(SUM(record_count), 0) AS total_records
  FROM prelim
  GROUP BY organization_id, instance_num, dateint
),

eligible_groups AS (
  SELECT
    gf.organization_id, gf.instance_num, gf.dateint, gf.seed_rank
  FROM group_flags gf
  JOIN prelim_stats ps
    ON ps.organization_id = gf.organization_id
   AND ps.instance_num    = gf.instance_num
   AND ps.dateint         = gf.dateint
  WHERE (NOT gf.is_old AND ps.total_records = gf.target_records)
     OR (gf.is_old      AND ps.total_records > 0)
),

-- 8) Pick earliest eligible group
winner_group AS (
  SELECT eg.*
  FROM eligible_groups eg
  WHERE eg.seed_rank = (SELECT MIN(seed_rank) FROM eligible_groups)
),

-- 9) Rows to claim if using the group path
group_chosen AS (
  SELECT pr.id
  FROM prelim pr
  JOIN winner_group w
    ON w.organization_id = pr.organization_id
   AND w.instance_num    = pr.instance_num
   AND w.dateint         = pr.dateint
),

-- 10) Final chosen IDs: prefer big_single if exists
chosen AS (
  SELECT id FROM big_single
  UNION ALL
  SELECT id FROM group_chosen
  WHERE NOT EXISTS (SELECT 1 FROM big_single)
),

-- 11) Optimistic claim (atomic per-row; guarded by claimed_at IS NULL)
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
ORDER BY priority DESC, queue_ts ASC;
