-- name: UpsertOrganization :one
INSERT INTO lrconfig_organizations (
  id, name, enabled, synced_at
) VALUES (
  @id, @name, @enabled, NOW()
) ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  enabled = EXCLUDED.enabled,
  synced_at = NOW()
RETURNING *;

-- name: GetOrganization :one
SELECT * FROM lrconfig_organizations 
WHERE id = @id;

-- name: GetOrganizationByName :one
SELECT * FROM lrconfig_organizations 
WHERE name = @name;

-- name: ListOrganizations :many
SELECT * FROM lrconfig_organizations
ORDER BY name;

-- name: ListEnabledOrganizations :many
SELECT * FROM lrconfig_organizations
WHERE enabled = true
ORDER BY name;

-- name: DeleteOrganization :exec
DELETE FROM lrconfig_organizations 
WHERE id = @id;

-- name: ClearOrganizations :exec
DELETE FROM lrconfig_organizations;

