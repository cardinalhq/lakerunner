-- name: GetOrgConfig :one
-- Get config for a specific org and key. Returns NULL if not found.
SELECT value
FROM organization_config
WHERE organization_id = $1 AND key = $2;

-- name: ListOrgConfigs :many
-- List all config keys/values for an org.
SELECT key, value, created_at, updated_at
FROM organization_config
WHERE organization_id = $1
ORDER BY key;

-- name: UpsertOrgConfig :exec
-- Insert or update a config value for an org.
INSERT INTO organization_config (organization_id, key, value, updated_at)
VALUES ($1, $2, $3, NOW())
ON CONFLICT (organization_id, key)
DO UPDATE SET
    value = EXCLUDED.value,
    updated_at = NOW();

-- name: DeleteOrgConfig :exec
-- Delete a specific config for an org.
DELETE FROM organization_config
WHERE organization_id = $1 AND key = $2;
