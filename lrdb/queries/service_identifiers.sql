-- name: UpsertServiceIdentifier :one
INSERT INTO service_identifiers
  (organization_id, service_name, cluster_name, namespace)
  VALUES (@organization_id, @service_name, @cluster_name, @namespace)
  ON CONFLICT (organization_id, service_name, cluster_name, namespace)
  DO UPDATE SET updated_at = now()
  RETURNING id, created_at; 