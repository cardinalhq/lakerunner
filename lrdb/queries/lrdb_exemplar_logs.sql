-- name: BatchUpsertExemplarLogs :batchone
-- This will upsert a new log exemplar. Exemplar and updated_at are always updated
-- to the provided values. If old_fingerprint is not 0, it is added to the list of related
-- fingerprints. This means the "old" fingerprint should be fingerprint, so it always updates
-- an existing record, not changing it to the new one.
-- The return value is a boolean indicating if the record is new.
INSERT INTO lrdb_exemplar_logs
            ( organization_id,  service_identifier_id,  fingerprint,  exemplar,  source)
VALUES      (@organization_id, @service_identifier_id, @fingerprint, @exemplar, @source)
ON CONFLICT ( organization_id,  service_identifier_id,  fingerprint,  source)
DO UPDATE SET
  exemplar   = EXCLUDED.exemplar,
  updated_at = now(),
  related_fingerprints = CASE
    WHEN @old_fingerprint::BIGINT != 0
      AND @fingerprint != @old_fingerprint
      THEN add_to_bigint_list(lrdb_exemplar_logs.related_fingerprints, @old_fingerprint, 100)
    ELSE lrdb_exemplar_logs.related_fingerprints
  END
RETURNING (created_at = updated_at) as is_new;

-- name: GetExemplarLogsByFingerprints :many
-- Fetches log exemplars for a set of fingerprints within an organization.
-- Returns the exemplar data along with service identifier information.
SELECT
  el.fingerprint,
  el.exemplar
FROM lrdb_exemplar_logs el
JOIN lrdb_service_identifiers si ON el.service_identifier_id = si.id
WHERE el.organization_id = @organization_id::uuid
  AND el.fingerprint = ANY(@fingerprints::bigint[]);