-- name: UpsertOrganization :one
INSERT INTO organizations (
  id, name, enabled, synced_at
) VALUES (
  @id, @name, @enabled, NOW()
) ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  enabled = EXCLUDED.enabled,
  synced_at = NOW()
RETURNING *;

-- name: GetOrganization :one
SELECT * FROM organizations 
WHERE id = @id;

-- name: GetOrganizationByName :one
SELECT * FROM organizations 
WHERE name = @name;

-- name: ListOrganizations :many
SELECT * FROM organizations
ORDER BY name;

-- name: ListEnabledOrganizations :many
SELECT * FROM organizations
WHERE enabled = true
ORDER BY name;

-- name: DeleteOrganization :exec
DELETE FROM organizations 
WHERE id = @id;

-- name: ClearOrganizations :exec
DELETE FROM organizations;

