-- name: ClaimInqueueWorkBatch :many
WITH
params AS (
  SELECT
    @telemetry_type::text      AS telemetry_type,
    @worker_id::bigint         AS worker_id,
    COALESCE(sqlc.narg(now_ts)::timestamptz, now()) AS now_ts,
    @max_total_size::bigint    AS max_total_size,    -- hard cap
    @min_total_size::bigint    AS min_total_size,    -- fresh-path minimum-by-size
    @max_age_seconds::integer  AS max_age_seconds,
    @batch_count::integer      AS batch_count        -- row cap (no more than)
),
first AS (
  SELECT q.*
  FROM inqueue q
  JOIN params p ON q.telemetry_type = p.telemetry_type
  WHERE q.claimed_at IS NULL
  ORDER BY q.priority DESC, q.queue_ts ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
),
scope AS (
  SELECT q.*
  FROM inqueue q
  JOIN first f ON q.organization_id = f.organization_id
               AND q.instance_num    = f.instance_num
               AND q.telemetry_type  = f.telemetry_type
  WHERE q.claimed_at IS NULL
  ORDER BY q.priority DESC, q.queue_ts ASC
  FOR UPDATE SKIP LOCKED
),
oldest AS (
  SELECT * FROM scope LIMIT 1
),
flags AS (
  SELECT
    (o.file_size >  p.max_total_size)                               AS is_oversized,
    ((p.now_ts - o.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old,
    p.*
  FROM params p
  JOIN oldest o ON TRUE
),
-- Greedy pack up to size cap and row cap
pack AS (
  SELECT
    s.*,
    SUM(s.file_size) OVER (ORDER BY s.priority DESC, s.queue_ts ASC
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_size,
    ROW_NUMBER() OVER (ORDER BY s.priority DESC, s.queue_ts ASC)              AS rn
  FROM scope s
),
prelim AS (
  SELECT p.*
  FROM pack p
  JOIN flags f ON TRUE
  WHERE p.cum_size <= f.max_total_size
    AND p.rn      <= f.batch_count
),
prelim_stats AS (
  SELECT
    COUNT(*) AS n_rows,
    COALESCE(SUM(file_size), 0) AS total_size
  FROM prelim
),
chosen AS (
  -- 1) Oversized oldest -> claim just that one  
  (
    SELECT o.id, o.queue_ts, o.priority, o.organization_id, o.collector_name, 
           o.instance_num, o.bucket, o.object_id, o.telemetry_type, o.tries, 
           o.claimed_by, o.claimed_at, o.file_size
    FROM oldest o
    JOIN flags f ON TRUE
    WHERE f.is_oversized
  )
  UNION ALL
  -- 2) Too old -> take whatever fits under caps (ignore min size)
  (
    SELECT p.id, p.queue_ts, p.priority, p.organization_id, p.collector_name, 
           p.instance_num, p.bucket, p.object_id, p.telemetry_type, p.tries, 
           p.claimed_by, p.claimed_at, p.file_size
    FROM prelim p
    JOIN flags f ON TRUE
    WHERE NOT f.is_oversized AND f.is_old
  )
  UNION ALL
  -- 3) Fresh -> only if size minimum met (and caps already enforced by prelim)
  (
    SELECT p.id, p.queue_ts, p.priority, p.organization_id, p.collector_name, 
           p.instance_num, p.bucket, p.object_id, p.telemetry_type, p.tries, 
           p.claimed_by, p.claimed_at, p.file_size
    FROM prelim p
    JOIN flags f ON TRUE
    JOIN prelim_stats ps ON TRUE
    WHERE NOT f.is_oversized
      AND NOT f.is_old
      AND ps.total_size >= f.min_total_size
  )
),
upd AS (
  UPDATE inqueue q
  SET claimed_by = (SELECT worker_id FROM params),
      claimed_at = (SELECT now_ts FROM params)
  FROM chosen c
  WHERE q.id = c.id
  RETURNING q.*
)
SELECT * FROM upd
ORDER BY priority DESC, queue_ts ASC;
