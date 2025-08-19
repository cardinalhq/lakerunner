-- name: GetBucketConfiguration :one
SELECT * FROM lrconfig_bucket_configurations WHERE bucket_name = @bucket_name;

-- name: GetOrganizationsByBucket :many
SELECT ob.organization_id 
FROM lrconfig_organization_buckets ob
JOIN lrconfig_bucket_configurations bc ON ob.bucket_id = bc.id
WHERE bc.bucket_name = @bucket_name;

-- name: CheckOrgBucketAccess :one
SELECT COUNT(*) > 0 as has_access
FROM lrconfig_organization_buckets ob
JOIN lrconfig_bucket_configurations bc ON ob.bucket_id = bc.id
WHERE ob.organization_id = @org_id AND bc.bucket_name = @bucket_name;

-- name: GetLongestPrefixMatch :one
SELECT bpm.organization_id
FROM lrconfig_bucket_prefix_mappings bpm
JOIN lrconfig_bucket_configurations bc ON bpm.bucket_id = bc.id
WHERE bc.bucket_name = @bucket_name 
  AND bpm.signal = @signal
  AND @object_path LIKE bpm.path_prefix || '%'
ORDER BY LENGTH(bpm.path_prefix) DESC
LIMIT 1;

-- name: CreateBucketConfiguration :one
INSERT INTO lrconfig_bucket_configurations (
  bucket_name, cloud_provider, region, endpoint, role
) VALUES (
  @bucket_name, @cloud_provider, @region, @endpoint, @role
) RETURNING *;

-- name: CreateOrganizationBucket :one
INSERT INTO lrconfig_organization_buckets (
  organization_id, bucket_id
) VALUES (
  @organization_id, @bucket_id
) RETURNING *;

-- name: CreateBucketPrefixMapping :one
INSERT INTO lrconfig_bucket_prefix_mappings (
  bucket_id, organization_id, path_prefix, signal
) VALUES (
  @bucket_id, @organization_id, @path_prefix, @signal
) RETURNING *;

-- Legacy table sync operations

-- name: GetAllCStorageProfilesForSync :many
SELECT DISTINCT
  sp.bucket AS bucket_name,
  sp.cloud_provider,
  sp.region,
  sp.role,
  c.organization_id
FROM c_storage_profiles sp
LEFT OUTER JOIN c_collectors c ON c.storage_profile_id = sp.id
WHERE c.deleted_at IS NULL;

-- name: ClearBucketPrefixMappings :exec
DELETE FROM lrconfig_bucket_prefix_mappings;

-- name: ClearOrganizationBuckets :exec
DELETE FROM lrconfig_organization_buckets;

-- name: ClearBucketConfigurations :exec
DELETE FROM lrconfig_bucket_configurations;

-- name: UpsertBucketConfiguration :one
INSERT INTO lrconfig_bucket_configurations (
  bucket_name, cloud_provider, region, endpoint, role
) VALUES (
  @bucket_name, @cloud_provider, @region, @endpoint, @role
) ON CONFLICT (bucket_name) DO UPDATE SET
  cloud_provider = EXCLUDED.cloud_provider,
  region = EXCLUDED.region,
  endpoint = EXCLUDED.endpoint,
  role = EXCLUDED.role
RETURNING *;

-- name: UpsertOrganizationBucket :exec
INSERT INTO lrconfig_organization_buckets (
  organization_id, bucket_id
) VALUES (
  @organization_id, @bucket_id
) ON CONFLICT (organization_id) DO UPDATE SET
  bucket_id = EXCLUDED.bucket_id;

-- name: GetBucketByOrganization :one
SELECT bc.bucket_name
FROM lrconfig_organization_buckets ob
JOIN lrconfig_bucket_configurations bc ON ob.bucket_id = bc.id
WHERE ob.organization_id = @organization_id;