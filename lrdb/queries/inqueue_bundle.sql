-- name: InqueuePickHead :one
-- Pick the oldest eligible item for a given signal
SELECT id, organization_id, instance_num, signal, file_size, queue_ts, priority
FROM public.inqueue
WHERE claimed_at IS NULL
  AND signal = $1::text
  AND eligible_at <= now()
ORDER BY priority ASC, queue_ts ASC, id ASC
LIMIT 1
FOR UPDATE;

-- name: InqueueFetchCandidates :many
-- Fetch all eligible candidates matching the grouping key
SELECT id, organization_id, instance_num, signal, file_size, queue_ts, priority, 
       bucket, object_id, collector_name, tries
FROM public.inqueue  
WHERE claimed_at IS NULL
  AND signal = $1::text
  AND organization_id = $2::uuid
  AND instance_num = $3::smallint
  AND eligible_at <= now()
ORDER BY priority ASC, queue_ts ASC, id ASC
LIMIT $4::integer
FOR UPDATE;

-- name: InqueueClaimBundle :exec
-- Claim a bundle of items
UPDATE public.inqueue
SET claimed_by = $1::bigint,
    claimed_at = now(),
    heartbeated_at = now()
WHERE id = ANY($2::uuid[])
  AND claimed_at IS NULL;

-- name: InqueueDeferItems :exec
-- Defer items by pushing their eligible_at forward
UPDATE public.inqueue
SET eligible_at = now() + $1::interval
WHERE claimed_at IS NULL
  AND id = ANY($2::uuid[]);

-- name: InqueueGetBundleItems :many
-- Get full details for claimed bundle items
SELECT id, queue_ts, priority, organization_id, collector_name, instance_num,
       bucket, object_id, signal, tries, claimed_by, claimed_at, heartbeated_at,
       file_size, eligible_at
FROM public.inqueue
WHERE id = ANY($1::uuid[])
ORDER BY priority ASC, queue_ts ASC, id ASC;