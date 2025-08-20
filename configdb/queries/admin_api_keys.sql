-- name: GetAdminAPIKeyByHash :one
SELECT * FROM admin_api_keys WHERE key_hash = @key_hash;

-- name: GetAdminAPIKeyByID :one  
SELECT * FROM admin_api_keys WHERE id = @api_key_id;

-- name: CreateAdminAPIKey :one
INSERT INTO admin_api_keys (
  key_hash, name, description
) VALUES (
  @key_hash, @name, @description
) RETURNING *;

-- name: UpsertAdminAPIKey :one
INSERT INTO admin_api_keys (
  key_hash, name, description
) VALUES (
  @key_hash, @name, @description
) ON CONFLICT (key_hash) DO UPDATE SET
  name = EXCLUDED.name,
  description = EXCLUDED.description
RETURNING *;

-- name: DeleteAdminAPIKey :exec
DELETE FROM admin_api_keys WHERE id = @api_key_id;

-- name: GetAllAdminAPIKeys :many
SELECT * FROM admin_api_keys ORDER BY created_at DESC;

-- name: ClearAdminAPIKeys :exec
DELETE FROM admin_api_keys;