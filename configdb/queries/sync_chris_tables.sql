-- This file contains queries for syncing data from legacy c_ tables (managed externally)
-- to our own tables that we control.

-- Fetch all organizations from c_ tables
-- name: GetAllCOrganizations :many
SELECT id, name, enabled, created_at
FROM c_organizations;

-- Fetch all our organizations
-- name: GetAllOrganizations :many
SELECT id, name, enabled
FROM organizations;

-- Upsert organization
-- name: UpsertOrganizationSync :exec
INSERT INTO organizations (id, name, enabled, synced_at)
VALUES (@id, @name, @enabled, NOW())
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  enabled = EXCLUDED.enabled,
  synced_at = NOW();

-- Delete organizations not in c_ tables
-- name: DeleteOrganizationsNotInList :exec
DELETE FROM organizations
WHERE id = ANY(@ids_to_delete::uuid[]);

-- Get all bucket configurations from c_ tables with org mappings
-- name: GetAllCBucketData :many
SELECT DISTINCT
  sp.bucket AS bucket_name,
  sp.cloud_provider,
  sp.region,
  sp.role,
  c.organization_id,
  c.instance_num,
  c.external_id AS collector_name
FROM c_storage_profiles sp
LEFT JOIN c_collectors c ON c.storage_profile_id = sp.id
WHERE c.deleted_at IS NULL
  AND c.organization_id IS NOT NULL;

-- Get all our bucket configurations
-- name: GetAllBucketConfigurations :many
SELECT id, bucket_name, cloud_provider, region, role
FROM bucket_configurations;

-- Get all our organization bucket mappings
-- name: GetAllOrganizationBucketMappings :many
SELECT
  ob.organization_id,
  ob.instance_num,
  ob.collector_name,
  bc.bucket_name
FROM organization_buckets ob
JOIN bucket_configurations bc ON ob.bucket_id = bc.id;

-- Delete organization bucket mappings not in c_ tables
-- name: DeleteOrganizationBucketMappings :exec
DELETE FROM organization_buckets
WHERE (organization_id, instance_num, collector_name) IN (
  SELECT unnest(@org_ids::uuid[]),
         unnest(@instance_nums::smallint[]),
         unnest(@collector_names::text[])
);

-- Get all API keys from c_ tables
-- name: GetAllCOrganizationAPIKeys :many
SELECT id, organization_id, name, api_key, enabled
FROM c_organization_api_keys
WHERE organization_id IS NOT NULL AND api_key IS NOT NULL;

-- Get all our API key mappings
-- name: GetAllOrganizationAPIKeyMappings :many
SELECT
  oakm.organization_id,
  oak.key_hash,
  oak.name
FROM organization_api_key_mappings oakm
JOIN organization_api_keys oak ON oakm.api_key_id = oak.id;

-- Delete API key mapping by hash
-- name: DeleteOrganizationAPIKeyMappingByHash :exec
DELETE FROM organization_api_key_mappings
WHERE organization_id = @organization_id
AND api_key_id = (SELECT id FROM organization_api_keys WHERE key_hash = @key_hash);