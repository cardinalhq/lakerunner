-- name: ClaimInqueueWorkBatch :many
WITH
params AS (
  SELECT
    @signal::text              AS signal,
    @worker_id::bigint         AS worker_id,
    COALESCE(sqlc.narg(now_ts)::timestamptz, now()) AS now_ts,
    @max_total_size::bigint    AS max_total_size,    -- hard cap on summed file_size
    @min_total_size::bigint    AS min_total_size,    -- fresh-path minimum by size
    @max_age_seconds::integer  AS max_age_seconds,   -- age threshold for relaxing min
    @batch_count::integer      AS batch_count        -- row cap
),

-- 1) Safety net: if any single file already meets/exceeds the cap, take that file alone
big_single AS (
  SELECT q.id
  FROM inqueue q
  JOIN params p ON TRUE
  WHERE q.claimed_at IS NULL
    AND q.signal = p.signal
    AND q.file_size >= p.max_total_size
  ORDER BY q.priority DESC, q.queue_ts ASC, q.id ASC
  LIMIT 1
),

-- 2) One seed (oldest/highest-priority) per group (org, instance) within signal
seeds_per_group AS (
  SELECT DISTINCT ON (q.organization_id, q.instance_num)
         q.id AS seed_id, q.organization_id, q.instance_num, q.priority, q.queue_ts
  FROM inqueue q
  JOIN params p ON TRUE
  WHERE q.claimed_at IS NULL
    AND q.signal = p.signal
  ORDER BY q.organization_id, q.instance_num, q.priority DESC, q.queue_ts ASC, q.id ASC
),

-- 3) Order groups globally by their seed (priority DESC, queue_ts ASC)
ordered_groups AS (
  SELECT s.*,
         ROW_NUMBER() OVER (ORDER BY s.priority DESC, s.queue_ts ASC, s.seed_id ASC) AS seed_rank
  FROM seeds_per_group s
),

-- 4) Attach age flags + caps per group (using params only; no per-row estimator here)
group_flags AS (
  SELECT
    og.organization_id,
    og.instance_num,
    og.priority,
    og.queue_ts,
    og.seed_rank,
    ((p.now_ts - og.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old,
    p.max_total_size,
    p.min_total_size,
    p.batch_count,
    p.signal,
    p.now_ts
  FROM ordered_groups og
  CROSS JOIN params p
),

-- 5) All ready rows in each group for that signal (selection only; claim happens later)
grp_scope AS (
  SELECT
    q.id, q.organization_id, q.instance_num, q.priority, q.queue_ts, q.file_size,
    gf.seed_rank, gf.is_old, gf.max_total_size, gf.min_total_size, gf.batch_count
  FROM inqueue q
  JOIN group_flags gf
    ON q.claimed_at IS NULL
   AND q.signal = gf.signal
   AND q.organization_id = gf.organization_id
   AND q.instance_num = gf.instance_num
),

-- 6) Greedy pack within each group, ordered by priority/queue_ts
pack AS (
  SELECT
    g.*,
    SUM(g.file_size) OVER (
      PARTITION BY g.organization_id, g.instance_num
      ORDER BY g.priority DESC, g.queue_ts ASC, g.id ASC
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_size,
    ROW_NUMBER() OVER (
      PARTITION BY g.organization_id, g.instance_num
      ORDER BY g.priority DESC, g.queue_ts ASC, g.id ASC
    ) AS rn
  FROM grp_scope g
),

-- 7) Keep only rows that fit under caps
prelim AS (
  SELECT p.*
  FROM pack p
  JOIN group_flags gf
    ON gf.organization_id = p.organization_id
   AND gf.instance_num    = p.instance_num
  WHERE p.cum_size <= gf.max_total_size
    AND p.rn       <= gf.batch_count
),

-- 8) Totals per group (what we’d actually claim)
prelim_stats AS (
  SELECT
    organization_id, instance_num,
    COUNT(*) AS n_rows,
    COALESCE(SUM(file_size), 0) AS total_size,
    MIN(seed_rank) AS seed_rank
  FROM prelim
  GROUP BY organization_id, instance_num
),

-- 9) Eligibility: any group with positive size (greedy batching)
eligible_groups AS (
  SELECT
    gf.organization_id, gf.instance_num, gf.seed_rank
  FROM group_flags gf
  JOIN prelim_stats ps
    ON ps.organization_id = gf.organization_id
   AND ps.instance_num    = gf.instance_num
  WHERE ps.total_size > 0
),

-- 10) Pick earliest eligible group globally
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
   AND w.instance_num    = pr.instance_num
),

-- 12) Final chosen IDs: prefer big_single if present; else packed group rows
chosen AS (
  SELECT id FROM big_single
  UNION ALL
  SELECT id FROM group_chosen
  WHERE NOT EXISTS (SELECT 1 FROM big_single)
),

-- 13) Atomic optimistic claim (no window funcs here)
upd AS (
  UPDATE inqueue q
  SET claimed_by = (SELECT worker_id FROM params),
      claimed_at = (SELECT now_ts FROM params),
      heartbeated_at = (SELECT now_ts FROM params)
  FROM chosen c
  WHERE q.id = c.id
    AND q.claimed_at IS NULL
  RETURNING q.*
)
SELECT * FROM upd
ORDER BY upd.priority DESC, upd.queue_ts ASC, upd.id ASC;
