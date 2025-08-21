-- This file contains queries for syncing data from legacy c_ tables (managed externally)
-- to our own tables that we control.

-- name: SyncOrganizations :exec
INSERT INTO organizations (id, name, enabled, created_at, synced_at)
SELECT id, name, COALESCE(enabled, true), COALESCE(created_at, NOW()), NOW()
FROM c_organizations
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  enabled = EXCLUDED.enabled,
  synced_at = NOW();

-- name: SyncOrganizationBuckets :exec
INSERT INTO organization_buckets (organization_id, bucket_id, instance_num, collector_name)
SELECT 
  c.organization_id,
  bc.id as bucket_id,
  c.instance_num,
  c.external_id as collector_name
FROM c_collectors c
JOIN c_storage_profiles sp ON c.storage_profile_id = sp.id
JOIN bucket_configurations bc ON sp.bucket = bc.bucket_name
WHERE c.deleted_at IS NULL
ON CONFLICT (organization_id, bucket_id, instance_num, collector_name) DO NOTHING;