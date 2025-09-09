-- name: InsertSegmentJournal :exec
-- Insert a debugging journal entry for segment operations
INSERT INTO segment_journal (
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
    record_estimate,
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
    @record_estimate,
    @metadata
);

-- name: GetSegmentJournalByOrg :many
-- Get segment_journal entries for debugging, filtered by organization
SELECT id, signal, action, created_at, organization_id, instance_num, dateint, frequency_ms,
       source_count, source_object_keys, source_total_records, source_total_size,
       dest_count, dest_object_keys, dest_total_records, dest_total_size,
       record_estimate, metadata
FROM segment_journal
WHERE organization_id = @organization_id
  AND (@signal::smallint IS NULL OR signal = @signal)
  AND (@action::smallint IS NULL OR action = @action)
ORDER BY created_at DESC
LIMIT @limit_val;

-- name: GetSegmentJournalByID :one
-- Get a specific segment_journal entry by ID
SELECT *
FROM segment_journal
WHERE id = @id;

-- name: GetLatestSegmentJournal :one
-- Get the most recent segment_journal entry
SELECT *
FROM segment_journal
ORDER BY created_at DESC
LIMIT 1;

-- name: DeleteOldSegmentJournals :exec
-- Clean up old segment_journal entries for maintenance
DELETE FROM segment_journal
WHERE created_at < @cutoff_time;