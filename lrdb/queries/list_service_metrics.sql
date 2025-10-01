-- name: ListServiceMetrics :many
SELECT DISTINCT em.metric_name
FROM lrdb_exemplar_metrics em
WHERE em.organization_id = @organization_id
  AND em.attributes->>'service_name' = @service_name::text
  AND (
    (
      em.updated_at IS NOT NULL
      AND em.updated_at >= COALESCE(to_timestamp(@start_time / 1000.0), NOW() - interval '1 day')
      AND em.updated_at <= COALESCE(to_timestamp(@end_time / 1000.0), NOW())
    )
    OR (
      em.updated_at IS NULL
      AND em.created_at >= COALESCE(to_timestamp(@start_time / 1000.0), NOW() - interval '1 day')
      AND em.created_at <= COALESCE(to_timestamp(@end_time / 1000.0), NOW())
    )
  )
ORDER BY em.metric_name;
