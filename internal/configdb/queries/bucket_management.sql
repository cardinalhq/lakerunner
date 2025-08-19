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
SELECT bpm.organization_id
FROM bucket_prefix_mappings bpm
JOIN bucket_configurations bc ON bpm.bucket_id = bc.id
WHERE bc.bucket_name = @bucket_name 
  AND bpm.signal = @signal
  AND @object_path LIKE bpm.path_prefix || '%'
ORDER BY LENGTH(bpm.path_prefix) DESC
LIMIT 1;

-- name: CreateBucketConfiguration :one
INSERT INTO bucket_configurations (
  bucket_name, cloud_provider, region, endpoint, role
) VALUES (
  @bucket_name, @cloud_provider, @region, @endpoint, @role
) RETURNING *;

-- name: CreateOrganizationBucket :one
INSERT INTO organization_buckets (
  organization_id, bucket_id
) VALUES (
  @organization_id, @bucket_id
) RETURNING *;

-- name: CreateBucketPrefixMapping :one
INSERT INTO bucket_prefix_mappings (
  bucket_id, organization_id, path_prefix, signal
) VALUES (
  @bucket_id, @organization_id, @path_prefix, @signal
) RETURNING *;