-- name: BatchUpsertExemplarMetrics :batchone
INSERT INTO exemplar_metrics
            ( organization_id,  service_identifier_id,  metric_name,  metric_type,  attributes,  exemplar)
VALUES      (@organization_id, @service_identifier_id, @metric_name, @metric_type, @attributes, @exemplar)
ON CONFLICT ( organization_id,  service_identifier_id,  metric_name,  metric_type)
DO UPDATE SET
  attributes = EXCLUDED.attributes,
  exemplar = EXCLUDED.exemplar,
  updated_at = now()
RETURNING (created_at = updated_at) as is_new;

-- name: GetExemplarMetricsCreatedAfter :many
SELECT * FROM exemplar_metrics WHERE created_at > @ts;

-- name: GetExemplarMetricsByService :many
SELECT * FROM exemplar_metrics 
WHERE organization_id = @organization_id 
  AND service_identifier_id = @service_identifier_id
ORDER BY created_at DESC;