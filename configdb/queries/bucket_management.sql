-- name: GetBucketConfiguration :one
SELECT * FROM bucket_configurations WHERE bucket_name = @bucket_name;

-- name: GetOrganizationsByBucket :many
SELECT ob.organization_id 
FROM organization_buckets ob
JOIN bucket_configurations bc ON ob.bucket_id = bc.id
WHERE bc.bucket_name = @bucket_name;

-- name: CheckOrgBucketAccess :one
SELECT COUNT(*) > 0 as has_access
FROM organization_buckets ob
JOIN bucket_configurations bc ON ob.bucket_id = bc.id
WHERE ob.organization_id = @org_id AND bc.bucket_name = @bucket_name;

-- name: GetLongestPrefixMatch :one
SELECT bpm.organization_id, bpm.signal
FROM bucket_prefix_mappings bpm
JOIN bucket_configurations bc ON bpm.bucket_id = bc.id
WHERE bc.bucket_name = @bucket_name
  AND @object_path LIKE bpm.path_prefix || '%'
ORDER BY LENGTH(bpm.path_prefix) DESC
LIMIT 1;

-- name: CreateBucketConfiguration :one
INSERT INTO bucket_configurations (
  bucket_name, cloud_provider, region, endpoint, role, use_path_style, insecure_tls
) VALUES (
  @bucket_name, @cloud_provider, @region, @endpoint, @role, @use_path_style, @insecure_tls
) RETURNING *;

-- name: CreateOrganizationBucket :one
INSERT INTO organization_buckets (
  organization_id, bucket_id, instance_num, collector_name
) VALUES (
  @organization_id, @bucket_id, @instance_num, @collector_name
) RETURNING *;

-- name: CreateBucketPrefixMapping :one
INSERT INTO bucket_prefix_mappings (
  bucket_id, organization_id, path_prefix, signal
) VALUES (
  @bucket_id, @organization_id, @path_prefix, @signal
) RETURNING *;

-- name: ListBucketConfigurations :many
SELECT id, bucket_name, cloud_provider, region, endpoint, role, use_path_style, insecure_tls
FROM bucket_configurations
ORDER BY bucket_name;

-- name: DeleteBucketConfiguration :exec
DELETE FROM bucket_configurations WHERE bucket_name = @bucket_name;

-- name: ListBucketPrefixMappings :many
SELECT bpm.id, bc.bucket_name, bpm.organization_id, bpm.path_prefix, bpm.signal
FROM bucket_prefix_mappings bpm
JOIN bucket_configurations bc ON bpm.bucket_id = bc.id
ORDER BY bc.bucket_name, bpm.path_prefix;

-- name: DeleteBucketPrefixMapping :exec
DELETE FROM bucket_prefix_mappings WHERE id = @id;

-- Legacy table sync operations

-- name: GetAllCStorageProfilesForSync :many
SELECT DISTINCT
  sp.bucket AS bucket_name,
  sp.cloud_provider,
  sp.region,
  sp.role,
  sp.organization_id
FROM c_storage_profiles sp
WHERE sp.organization_id IS NOT NULL;

-- name: ClearBucketPrefixMappings :exec
DELETE FROM bucket_prefix_mappings;

-- name: ClearOrganizationBuckets :exec
DELETE FROM organization_buckets;

-- name: ClearBucketConfigurations :exec
DELETE FROM bucket_configurations;

-- name: UpsertBucketConfiguration :one
INSERT INTO bucket_configurations (
  bucket_name, cloud_provider, region, endpoint, role, use_path_style, insecure_tls
) VALUES (
  @bucket_name, @cloud_provider, @region, @endpoint, @role, @use_path_style, @insecure_tls
) ON CONFLICT (bucket_name) DO UPDATE SET
  cloud_provider = EXCLUDED.cloud_provider,
  region = EXCLUDED.region,
  endpoint = EXCLUDED.endpoint,
  role = EXCLUDED.role,
  use_path_style = EXCLUDED.use_path_style,
  insecure_tls = EXCLUDED.insecure_tls
RETURNING *;

-- name: UpsertOrganizationBucket :exec
INSERT INTO organization_buckets (
  organization_id, bucket_id, instance_num, collector_name
) VALUES (
  @organization_id, @bucket_id, @instance_num, @collector_name
) ON CONFLICT (organization_id, bucket_id, instance_num, collector_name) DO NOTHING;

-- name: GetBucketByOrganization :one
SELECT bc.bucket_name
FROM organization_buckets ob
JOIN bucket_configurations bc ON ob.bucket_id = bc.id
WHERE ob.organization_id = @organization_id;

-- name: HasExistingStorageProfiles :one
SELECT COUNT(*) > 0 as has_profiles
FROM bucket_configurations;

-- name: GetOrganizationBucketByInstance :one
SELECT ob.organization_id, ob.instance_num, ob.collector_name, bc.bucket_name, bc.cloud_provider, bc.region, bc.role, bc.endpoint, bc.use_path_style, bc.insecure_tls
FROM organization_buckets ob
JOIN bucket_configurations bc ON ob.bucket_id = bc.id  
WHERE ob.organization_id = $1 AND ob.instance_num = $2;

-- name: GetOrganizationBucketByCollector :one
SELECT ob.organization_id, ob.instance_num, ob.collector_name, bc.bucket_name, bc.cloud_provider, bc.region, bc.role, bc.endpoint, bc.use_path_style, bc.insecure_tls
FROM organization_buckets ob
JOIN bucket_configurations bc ON ob.bucket_id = bc.id  
WHERE ob.organization_id = $1 AND ob.collector_name = $2;

-- name: GetDefaultOrganizationBucket :one
SELECT ob.organization_id, ob.instance_num, ob.collector_name, bc.bucket_name, bc.cloud_provider, bc.region, bc.role, bc.endpoint, bc.use_path_style, bc.insecure_tls
FROM organization_buckets ob
JOIN bucket_configurations bc ON ob.bucket_id = bc.id  
WHERE ob.organization_id = $1 
ORDER BY ob.instance_num, ob.collector_name 
LIMIT 1;

-- name: GetLowestInstanceOrganizationBucket :one
SELECT ob.organization_id, ob.instance_num, ob.collector_name, bc.bucket_name, bc.cloud_provider, bc.region, bc.role, bc.endpoint, bc.use_path_style, bc.insecure_tls
FROM organization_buckets ob
JOIN bucket_configurations bc ON ob.bucket_id = bc.id
WHERE ob.organization_id = $1 AND bc.bucket_name = $2
ORDER BY ob.instance_num, ob.collector_name
LIMIT 1;

-- name: ListOrganizationBucketsByOrg :many
SELECT ob.organization_id, bc.bucket_name, ob.instance_num, ob.collector_name
FROM organization_buckets ob
JOIN bucket_configurations bc ON ob.bucket_id = bc.id
WHERE ob.organization_id = @organization_id
ORDER BY bc.bucket_name, ob.instance_num;

-- name: DeleteOrganizationBucket :exec
DELETE FROM organization_buckets
WHERE organization_id = @organization_id
AND bucket_id = (SELECT id FROM bucket_configurations WHERE bucket_name = @bucket_name)
AND instance_num = @instance_num
AND collector_name = @collector_name;

-- name: GetBucketConfigurationByName :one
SELECT * FROM bucket_configurations WHERE bucket_name = @bucket_name;