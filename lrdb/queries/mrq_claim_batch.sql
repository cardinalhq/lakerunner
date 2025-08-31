-- name: ClaimMetricRollupWork :many
WITH
params AS (
  SELECT
    @worker_id::bigint                               AS worker_id,
    COALESCE(sqlc.narg(now_ts)::timestamptz, now())  AS now_ts,
    @max_age_seconds::integer                        AS max_age_seconds,
    @batch_count::integer                            AS batch_count
),

-- 1) One seed per group (org, dateint, freq, instance, slot_id, slot_count)
seeds_per_group AS (
  SELECT DISTINCT ON (organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count)
         id AS seed_id, organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count,
         priority, queue_ts
  FROM metric_rollup_queue
  WHERE claimed_at IS NULL
  ORDER BY organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count, 
           priority DESC, queue_ts ASC, id ASC
),

-- 2) Order groups globally by seed recency/priority
ordered_groups AS (
  SELECT s.*,
         ROW_NUMBER() OVER (ORDER BY s.priority DESC, s.queue_ts ASC, s.seed_id ASC) AS seed_rank
  FROM seeds_per_group s
),

-- 3) Attach per-group flags
group_flags AS (
  SELECT
    og.organization_id, og.dateint, og.frequency_ms, og.instance_num, og.slot_id, og.slot_count,
    og.priority, og.queue_ts, og.seed_rank,
    ((p.now_ts - og.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old,
    p.batch_count,
    p.now_ts
  FROM ordered_groups og
  CROSS JOIN params p
),

-- 4) All ready rows within each group
grp_scope AS (
  SELECT
    q.id, q.organization_id, q.dateint, q.frequency_ms, q.instance_num, 
    q.slot_id, q.slot_count, q.priority, q.queue_ts,
    gf.seed_rank, gf.is_old, gf.batch_count
  FROM metric_rollup_queue q
  JOIN group_flags gf
    ON q.claimed_at   IS NULL
   AND q.organization_id = gf.organization_id
   AND q.dateint         = gf.dateint
   AND q.frequency_ms    = gf.frequency_ms
   AND q.instance_num    = gf.instance_num
   AND q.slot_id         = gf.slot_id
   AND q.slot_count      = gf.slot_count
),

-- 5) Limit per group
pack AS (
  SELECT
    g.*,
    ROW_NUMBER() OVER (
      PARTITION BY g.organization_id, g.dateint, g.frequency_ms, g.instance_num, g.slot_id, g.slot_count
      ORDER BY g.priority DESC, g.queue_ts ASC, g.id ASC
    ) AS rn
  FROM grp_scope g
),

-- 6) Rows that fit under caps
prelim AS (
  SELECT p.*
  FROM pack p
  JOIN group_flags gf
    ON gf.organization_id = p.organization_id
   AND gf.dateint         = p.dateint
   AND gf.frequency_ms    = p.frequency_ms
   AND gf.instance_num    = p.instance_num
   AND gf.slot_id         = p.slot_id
   AND gf.slot_count      = p.slot_count
  WHERE p.rn <= gf.batch_count
),

-- 7) Totals per group
prelim_stats AS (
  SELECT
    organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count,
    COUNT(*) AS n_rows,
    MIN(seed_rank) AS seed_rank
  FROM prelim
  GROUP BY organization_id, dateint, frequency_ms, instance_num, slot_id, slot_count
),

-- 8) Eligibility: fresh = at least 1, old = any positive
eligible_groups AS (
  SELECT
    gf.organization_id, gf.dateint, gf.frequency_ms, gf.instance_num, 
    gf.slot_id, gf.slot_count, gf.seed_rank
  FROM group_flags gf
  JOIN prelim_stats ps
    ON ps.organization_id = gf.organization_id
   AND ps.dateint         = gf.dateint
   AND ps.frequency_ms    = gf.frequency_ms
   AND ps.instance_num    = gf.instance_num
   AND ps.slot_id         = gf.slot_id
   AND ps.slot_count      = gf.slot_count
  WHERE (NOT gf.is_old AND ps.n_rows >= 1)
     OR (gf.is_old      AND ps.n_rows > 0)
),

-- 9) Pick earliest eligible group
winner_group AS (
  SELECT eg.*
  FROM eligible_groups eg
  WHERE eg.seed_rank = (SELECT MIN(seed_rank) FROM eligible_groups)
),

-- 10) Rows to claim for the winner group
group_chosen AS (
  SELECT pr.id, pr.organization_id, pr.dateint, pr.frequency_ms, pr.instance_num,
         pr.slot_id, pr.slot_count, pr.priority
  FROM prelim pr
  JOIN winner_group w
    ON w.organization_id = pr.organization_id
   AND w.dateint         = pr.dateint
   AND w.frequency_ms    = pr.frequency_ms
   AND w.instance_num    = pr.instance_num
   AND w.slot_id         = pr.slot_id
   AND w.slot_count      = pr.slot_count
),

-- 11) Atomic optimistic claim
upd AS (
  UPDATE metric_rollup_queue q
  SET claimed_by = (SELECT worker_id FROM params),
      claimed_at = (SELECT now_ts FROM params),
      heartbeated_at = (SELECT now_ts FROM params)
  FROM group_chosen c
  WHERE q.id = c.id
    AND q.claimed_at IS NULL
  RETURNING q.*
)
SELECT * FROM upd
ORDER BY priority DESC, queue_ts ASC, id ASC;