-- name: WorkQueueAdd :one
INSERT INTO work_queue (
  task_name,
  organization_id,
  instance_num,
  spec,
  priority
) VALUES (
  @task_name,
  @organization_id,
  @instance_num,
  @spec,
  @priority
)
RETURNING *;

-- name: WorkQueueClaim :one
WITH next AS (
  SELECT id
  FROM work_queue wq
  WHERE wq.task_name = @task_name
    AND wq.claimed_by = -1
    AND wq.failed = false
  ORDER BY wq.priority, wq.id
  FOR UPDATE SKIP LOCKED
  LIMIT 1
)
UPDATE work_queue w
SET claimed_by     = @worker_id,
    claimed_at     = now(),
    heartbeated_at = now()
FROM next
WHERE w.id = next.id
RETURNING w.*;

-- name: WorkQueueHeartbeat :exec
UPDATE work_queue
   SET heartbeated_at = now()
 WHERE id = ANY(@ids::BIGINT[])
   AND claimed_by = @worker_id;

-- name: WorkQueueComplete :exec
DELETE FROM work_queue
 WHERE id = @id
   AND claimed_by = @worker_id;

-- name: WorkQueueFail :one
UPDATE work_queue
   SET claimed_by     = -1,
       claimed_at     = NULL,
       heartbeated_at = NULL,
       tries          = tries + 1,
       failed         = (tries + 1 >= @max_retries),
       failed_reason  = COALESCE(@failed_reason, failed_reason)
 WHERE id = @id
   AND claimed_by = @worker_id
RETURNING tries;

-- name: WorkQueueCleanup :exec
UPDATE work_queue
   SET claimed_by     = -1,
       claimed_at     = NULL,
       heartbeated_at = NULL
 WHERE claimed_by <> -1
   AND heartbeated_at < now() - @heartbeat_timeout::INTERVAL;

-- name: WorkQueueDepth :one
SELECT COUNT(*) as depth
  FROM work_queue
 WHERE task_name = @task_name
   AND claimed_by = -1
   AND failed = false;

-- name: WorkQueueDepthAll :many
SELECT task_name, priority, COUNT(*) as depth
  FROM work_queue
 WHERE claimed_by = -1
   AND failed = false
 GROUP BY task_name, priority;

-- name: WorkQueueStatus :many
SELECT
  task_name,
  priority,
  COUNT(*) FILTER (WHERE claimed_by = -1 AND failed = false) as pending,
  COUNT(*) FILTER (WHERE claimed_by <> -1) as in_progress,
  COUNT(*) FILTER (WHERE failed = true) as failed,
  COUNT(DISTINCT claimed_by) FILTER (WHERE claimed_by <> -1) as workers
FROM work_queue
GROUP BY task_name, priority
ORDER BY task_name, priority;
