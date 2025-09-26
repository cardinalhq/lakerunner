-- name: UpsertServiceIdentifier :one
INSERT INTO lrdb_service_identifiers
  (organization_id, service_name, cluster_name, namespace)
  VALUES (@organization_id, @service_name, @cluster_name, @namespace)
  ON CONFLICT (organization_id, service_name, cluster_name, namespace)
  DO UPDATE SET updated_at = now()
  RETURNING id, created_at;

-- name: ListServiceNames :many
SELECT DISTINCT service_name::text
FROM lrdb_service_identifiers
WHERE organization_id = @organization_id::uuid
ORDER BY service_name; 