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

-- Safety net: claim a single big row immediately if any row >= target_records
big_single AS (
  SELECT q.*
  FROM metric_compaction_queue q
  JOIN params p ON TRUE
  WHERE q.claimed_at IS NULL
    AND q.record_count >= p.target_records
  ORDER BY q.priority DESC, q.queue_ts ASC
  LIMIT 1
  FOR UPDATE SKIP LOCKED
),

-- Ready rows excluding nothing (the big_single branch is short-circuited later)
ready AS (
  SELECT *
  FROM metric_compaction_queue
  WHERE claimed_at IS NULL
),

-- One seed per (org, instance, dateint): oldest/highest-priority in the group
seeds_per_group AS (
  SELECT DISTINCT ON (organization_id, instance_num, dateint)
         id AS seed_id, organization_id, instance_num, dateint,
         priority, queue_ts, record_count
  FROM ready
  ORDER BY organization_id, instance_num, dateint, priority DESC, queue_ts ASC
),

-- Order groups globally by seed's (priority DESC, queue_ts ASC)
ordered_groups AS (
  SELECT s.*,
         ROW_NUMBER() OVER (ORDER BY s.priority DESC, s.queue_ts ASC) AS seed_rank
  FROM seeds_per_group s
),

-- Evaluate groups in global order; compute per-group “pack” and eligibility
-- We use LATERAL so Postgres can walk groups one-by-one and stop at the first match.
first_eligible_group AS (
  SELECT og.organization_id, og.instance_num, og.dateint, og.priority, og.queue_ts
  FROM ordered_groups og
  JOIN params p ON TRUE
  CROSS JOIN LATERAL (
    -- Determine if the seed is "old"
    SELECT ((p.now_ts - og.queue_ts) > make_interval(secs => p.max_age_seconds)) AS is_old
  ) age
  CROSS JOIN LATERAL (
    -- Greedy pack within THIS group (ordered), capped by target and batch_count
    SELECT x.*
    FROM (
      SELECT r.*,
             SUM(r.record_count) OVER (
               ORDER BY r.priority DESC, r.queue_ts ASC
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             ) AS cum_records,
             ROW_NUMBER() OVER (
               ORDER BY r.priority DESC, r.queue_ts ASC
             ) AS rn
      FROM ready r
      WHERE r.organization_id = og.organization_id
        AND r.instance_num    = og.instance_num
        AND r.dateint         = og.dateint
      ORDER BY r.priority DESC, r.queue_ts ASC
      FOR UPDATE SKIP LOCKED
    ) x
    WHERE x.cum_records <= p.target_records
      AND x.rn          <= p.batch_count
  ) packed
  GROUP BY og.organization_id, og.instance_num, og.dateint, og.priority, og.queue_ts, age.is_old, p.target_records
  HAVING
       (age.is_old AND SUM(packed.record_count) > 0)            -- old: any positive amount up to target
    OR (!age.is_old AND SUM(packed.record_count) = p.target_records) -- fresh: exact fill
  ORDER BY og.priority DESC, og.queue_ts ASC
  LIMIT 1
),

-- The rows to claim when using the group path (exact packed rows for the winner)
group_chosen AS (
  SELECT pr.*
  FROM params p
  JOIN first_eligible_group w ON TRUE
  JOIN LATERAL (
    SELECT r.*,
           SUM(r.record_count) OVER (
             ORDER BY r.priority DESC, r.queue_ts ASC
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           ) AS cum_records,
           ROW_NUMBER() OVER (
             ORDER BY r.priority DESC, r.queue_ts ASC
           ) AS rn
    FROM ready r
    WHERE r.organization_id = w.organization_id
      AND r.instance_num    = w.instance_num
      AND r.dateint         = w.dateint
    ORDER BY r.priority DESC, r.queue_ts ASC
    FOR UPDATE SKIP LOCKED
  ) pr
    ON TRUE
  WHERE pr.cum_records <= p.target_records
    AND pr.rn          <= p.batch_count
),

-- Final choice: prefer big_single if any; otherwise the packed group
chosen AS (
  SELECT * FROM big_single
  UNION ALL
  SELECT gc.* FROM group_chosen gc
  WHERE NOT EXISTS (SELECT 1 FROM big_single)
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
