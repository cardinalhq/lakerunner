-- name: GetOrganizationAPIKeyByHash :one
SELECT ak.*, ako.organization_id
FROM lrconfig_organization_api_keys ak
JOIN lrconfig_organization_api_key_mappings ako ON ak.id = ako.api_key_id
WHERE ak.key_hash = @key_hash;

-- name: GetOrganizationAPIKeyByID :one
SELECT ak.*, ako.organization_id
FROM lrconfig_organization_api_keys ak
JOIN lrconfig_organization_api_key_mappings ako ON ak.id = ako.api_key_id
WHERE ak.id = @api_key_id;

-- name: CreateOrganizationAPIKey :one
INSERT INTO lrconfig_organization_api_keys (
  key_hash, name, description
) VALUES (
  @key_hash, @name, @description
) RETURNING *;

-- name: CreateOrganizationAPIKeyMapping :one
INSERT INTO lrconfig_organization_api_key_mappings (
  api_key_id, organization_id
) VALUES (
  @api_key_id, @organization_id
) RETURNING *;

-- name: UpsertOrganizationAPIKey :one
INSERT INTO lrconfig_organization_api_keys (
  key_hash, name, description
) VALUES (
  @key_hash, @name, @description
) ON CONFLICT (key_hash) DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description
RETURNING *;

-- name: UpsertOrganizationAPIKeyMapping :exec
INSERT INTO lrconfig_organization_api_key_mappings (
  api_key_id, organization_id
) VALUES (
  @api_key_id, @organization_id
) ON CONFLICT (api_key_id) DO UPDATE SET
  organization_id = EXCLUDED.organization_id;

-- name: ClearOrganizationAPIKeyMappings :exec
DELETE FROM lrconfig_organization_api_key_mappings;

-- name: ClearOrganizationAPIKeys :exec
DELETE FROM lrconfig_organization_api_keys;

-- name: GetAllOrganizationAPIKeys :many
SELECT ak.*, ako.organization_id
FROM lrconfig_organization_api_keys ak
JOIN lrconfig_organization_api_key_mappings ako ON ak.id = ako.api_key_id;