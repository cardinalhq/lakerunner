-- name: BatchUpsertExemplarLogs :batchone
-- This will upsert a new log exemplar. Attributes, exemplar, and updated_at are always updated
-- to the provided values. If old_fingerprint is not 0, it is added to the list of related
-- fingerprints. This means the "old" fingerprint should be fingerprint, so it always updates
-- an existing record, not changing it to the new one.
-- The return value is a boolean indicating if the record is new.
INSERT INTO exemplar_logs
            ( organization_id,  collector_id,  processor_id,  service_identifier_id,  fingerprint,  attributes,  exemplar)
VALUES      (@organization_id, @collector_id, @processor_id, @service_identifier_id, @fingerprint, @attributes, @exemplar)
ON CONFLICT ( organization_id,  collector_id,  processor_id,  service_identifier_id,  fingerprint)
DO UPDATE SET
  attributes = EXCLUDED.attributes,
  exemplar   = EXCLUDED.exemplar,
  updated_at = now(),
  related_fingerprints = CASE
    WHEN @old_fingerprint::BIGINT != 0
      AND @fingerprint != @old_fingerprint
      THEN add_to_bigint_list(exemplar_logs.related_fingerprints, @old_fingerprint, 100)
    ELSE exemplar_logs.related_fingerprints
  END
RETURNING (created_at = updated_at) as is_new;

-- name: GetExemplarLogsCreatedAfter :many
SELECT * FROM exemplar_logs WHERE created_at > @ts;

-- name: GetExemplarLogsByService :many
SELECT * FROM exemplar_logs 
WHERE organization_id = @organization_id 
  AND service_identifier_id = @service_identifier_id
ORDER BY created_at DESC;

-- name: GetExemplarLogsByFingerprint :one
SELECT * FROM exemplar_logs 
WHERE organization_id = @organization_id 
  AND fingerprint = @fingerprint
LIMIT 1; 