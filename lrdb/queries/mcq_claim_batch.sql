-- name: ClaimMetricCompactionWorkBatch :many
WITH
params AS (
  SELECT
    @organization_id::uuid     AS organization_id,
    @instance_num::smallint    AS instance_num,
    @worker_id::bigint         AS worker_id,
    COALESCE(sqlc.narg(now_ts)::timestamptz, now()) AS now_ts,
    @max_records::bigint       AS max_records,       -- hard cap on number of records
    @min_records::bigint       AS min_records,       -- minimum records for fresh items
    @max_age_seconds::integer  AS max_age_seconds,   -- age threshold for taking whatever is available
    @batch_count::integer      AS batch_count        -- row cap (no more than this many items)
),
scope AS (
  SELECT q.*
  FROM metric_compaction_queue q
  JOIN params p ON TRUE
  WHERE q.claimed_at IS NULL
    AND q.organization_id = p.organization_id
    AND q.instance_num    = p.instance_num
  ORDER BY q.priority DESC, q.queue_ts ASC
  FOR UPDATE SKIP LOCKED
),
oldest AS (
  SELECT * FROM scope LIMIT 1
),
flags AS (
  SELECT
    (o.record_count >  p.max_records)                               AS is_oversized,
    ((p.now_ts - o.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old,
    p.*
  FROM params p
  JOIN oldest o ON TRUE
),
-- Greedy pack up to record cap and row cap
pack AS (
  SELECT
    s.*,
    SUM(s.record_count) OVER (ORDER BY s.priority DESC, s.queue_ts ASC
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_records,
    ROW_NUMBER() OVER (ORDER BY s.priority DESC, s.queue_ts ASC)              AS rn
  FROM scope s
),
prelim AS (
  SELECT p.*
  FROM pack p
  JOIN flags f ON TRUE
  WHERE p.cum_records <= f.max_records
    AND p.rn         <= f.batch_count
),
prelim_stats AS (
  SELECT
    COUNT(*) AS n_rows,
    COALESCE(SUM(record_count), 0) AS total_records
  FROM prelim
),
chosen AS (
  -- 1) Oversized oldest -> claim just that one  
  (
    SELECT o.id, o.queue_ts, o.priority, o.organization_id, o.dateint, 
           o.frequency_ms, o.segment_id, o.instance_num, o.ts_range, o.record_count, 
           o.tries, o.claimed_by, o.claimed_at
    FROM oldest o
    JOIN flags f ON TRUE
    WHERE f.is_oversized
  )
  UNION ALL
  -- 2) Too old -> take whatever fits under caps (ignore min records)
  (
    SELECT p.id, p.queue_ts, p.priority, p.organization_id, p.dateint, 
           p.frequency_ms, p.segment_id, p.instance_num, p.ts_range, p.record_count, 
           p.tries, p.claimed_by, p.claimed_at
    FROM prelim p
    JOIN flags f ON TRUE
    WHERE NOT f.is_oversized AND f.is_old
  )
  UNION ALL
  -- 3) Fresh -> only if record minimum met (and caps already enforced by prelim)
  (
    SELECT p.id, p.queue_ts, p.priority, p.organization_id, p.dateint, 
           p.frequency_ms, p.segment_id, p.instance_num, p.ts_range, p.record_count, 
           p.tries, p.claimed_by, p.claimed_at
    FROM prelim p
    JOIN flags f ON TRUE
    JOIN prelim_stats ps ON TRUE
    WHERE NOT f.is_oversized
      AND NOT f.is_old
      AND ps.total_records >= f.min_records
  )
),
upd AS (
  UPDATE metric_compaction_queue q
  SET claimed_by = (SELECT worker_id FROM params),
      claimed_at = (SELECT now_ts FROM params)
  FROM chosen c
  WHERE q.id = c.id
  RETURNING q.*
)
SELECT * FROM upd
ORDER BY priority DESC, queue_ts ASC;