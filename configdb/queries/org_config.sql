-- name: GetOrgConfig :one
-- Get config for a specific org and key. Returns NULL if not found.
SELECT value
FROM organization_config
WHERE organization_id = $1 AND key = $2;

-- name: GetOrgConfigWithDefault :one
-- Get config for org, falling back to system default (nil UUID) if not found.
SELECT COALESCE(
    (SELECT oc1.value FROM organization_config oc1 WHERE oc1.organization_id = $1 AND oc1.key = $2),
    (SELECT oc2.value FROM organization_config oc2 WHERE oc2.organization_id = '00000000-0000-0000-0000-000000000000' AND oc2.key = $2)
) AS config_value;

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
