-- name: InsertSegLog :exec
-- Insert a debugging log entry for segment operations
INSERT INTO seg_log (
    signal,
    action,
    organization_id,
    instance_num,
    dateint,
    frequency_ms,
    source_count,
    source_object_keys,
    source_total_records,
    source_total_size,
    dest_count,
    dest_object_keys,
    dest_total_records,
    dest_total_size,
    metadata
) VALUES (
    @signal,
    @action,
    @organization_id,
    @instance_num,
    @dateint,
    @frequency_ms,
    @source_count,
    @source_object_keys,
    @source_total_records,
    @source_total_size,
    @dest_count,
    @dest_object_keys,
    @dest_total_records,
    @dest_total_size,
    @metadata
);

-- name: GetSegLogByOrg :many
-- Get seg_log entries for debugging, filtered by organization
SELECT id, signal, action, created_at, organization_id, instance_num, dateint, frequency_ms,
       source_count, source_object_keys, source_total_records, source_total_size,
       dest_count, dest_object_keys, dest_total_records, dest_total_size,
       metadata
FROM seg_log
WHERE organization_id = @organization_id
  AND (@signal::smallint IS NULL OR signal = @signal)
  AND (@action::smallint IS NULL OR action = @action)
ORDER BY created_at DESC
LIMIT @limit_val;

-- name: GetSegLogByID :one
-- Get a specific seg_log entry by ID
SELECT id, signal, action, created_at, organization_id, instance_num, dateint, frequency_ms,
       source_count, source_object_keys, source_total_records, source_total_size,
       dest_count, dest_object_keys, dest_total_records, dest_total_size,
       metadata
FROM seg_log
WHERE id = @id;

-- name: GetLatestSegLog :one
-- Get the most recent seg_log entry
SELECT id, signal, action, created_at, organization_id, instance_num, dateint, frequency_ms,
       source_count, source_object_keys, source_total_records, source_total_size,
       dest_count, dest_object_keys, dest_total_records, dest_total_size,
       metadata
FROM seg_log
ORDER BY created_at DESC
LIMIT 1;

-- name: DeleteOldSegLogs :exec
-- Clean up old seg_log entries for maintenance
DELETE FROM seg_log
WHERE created_at < @cutoff_time;