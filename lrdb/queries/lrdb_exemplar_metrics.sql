-- name: BatchUpsertExemplarMetrics :batchone
INSERT INTO lrdb_exemplar_metrics
            ( organization_id,  service_identifier_id,  metric_name,  metric_type,  exemplar,  source)
VALUES      (@organization_id, @service_identifier_id, @metric_name, @metric_type, @exemplar, @source)
ON CONFLICT ( organization_id,  service_identifier_id,  metric_name,  metric_type,  source)
DO UPDATE SET
  exemplar = EXCLUDED.exemplar,
  updated_at = now()
RETURNING (created_at = updated_at) as is_new;

