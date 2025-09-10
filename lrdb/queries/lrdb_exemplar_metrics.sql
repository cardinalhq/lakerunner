-- name: BatchUpsertExemplarMetrics :batchone
INSERT INTO lrdb_exemplar_metrics
            ( organization_id,  service_identifier_id,  metric_name,  metric_type,  attributes,  exemplar)
VALUES      (@organization_id, @service_identifier_id, @metric_name, @metric_type, @attributes, @exemplar)
ON CONFLICT ( organization_id,  service_identifier_id,  metric_name,  metric_type)
DO UPDATE SET
  attributes = EXCLUDED.attributes,
  exemplar = EXCLUDED.exemplar,
  updated_at = now()
RETURNING (created_at = updated_at) as is_new;

