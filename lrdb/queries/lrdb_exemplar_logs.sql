-- name: BatchUpsertExemplarLogs :batchone
-- This will upsert a new log exemplar. Exemplar and updated_at are always updated
-- to the provided values. If old_fingerprint is not 0, it is added to the list of related
-- fingerprints. This means the "old" fingerprint should be fingerprint, so it always updates
-- an existing record, not changing it to the new one.
-- The return value is a boolean indicating if the record is new.
INSERT INTO lrdb_exemplar_logs
            ( organization_id,  service_identifier_id,  fingerprint,  exemplar,  source)
VALUES      (@organization_id, @service_identifier_id, @fingerprint, @exemplar, @source)
ON CONFLICT ( organization_id,  service_identifier_id,  fingerprint)
DO UPDATE SET
  exemplar   = EXCLUDED.exemplar,
  source     = EXCLUDED.source,
  updated_at = now(),
  related_fingerprints = CASE
    WHEN @old_fingerprint::BIGINT != 0
      AND @fingerprint != @old_fingerprint
      THEN add_to_bigint_list(lrdb_exemplar_logs.related_fingerprints, @old_fingerprint, 100)
    ELSE lrdb_exemplar_logs.related_fingerprints
  END
RETURNING (created_at = updated_at) as is_new;

 