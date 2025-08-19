-- name: GetStorageProfileUncached :one
SELECT
  sp.cloud_provider AS cloud_provider,
  sp.region AS region,
  sp.role AS role,
  sp.bucket AS bucket,
  c.instance_num::SMALLINT AS instance_num,
  c.organization_id::UUID AS organization_id,
  c.external_id::TEXT AS external_id
FROM
  c_storage_profiles sp
  LEFT OUTER JOIN c_collectors c ON c.storage_profile_id = sp.id
WHERE
  c.deleted_at IS NULL
  AND c.organization_id = @organization_id
  AND c.instance_num = @instance_num;

-- name: GetStorageProfileByCollectorNameUncached :one
SELECT
  sp.cloud_provider AS cloud_provider,
  sp.region AS region,
  sp.role AS role,
  sp.bucket AS bucket,
  c.instance_num::SMALLINT AS instance_num,
  c.organization_id::UUID AS organization_id,
  c.external_id::TEXT AS external_id
FROM
  c_storage_profiles sp
  LEFT OUTER JOIN c_collectors c ON c.storage_profile_id = sp.id
WHERE
  c.deleted_at IS NULL
  AND c.organization_id = @organization_id
  AND c.external_id = @collector_name;

-- name: GetStorageProfilesByBucketNameUncached :many
SELECT
  sp.cloud_provider AS cloud_provider,
  sp.region AS region,
  sp.role AS role,
  sp.bucket AS bucket,
  c.instance_num::SMALLINT AS instance_num,
  c.organization_id::UUID AS organization_id,
  c.external_id::TEXT AS external_id
FROM
  c_storage_profiles sp
  LEFT OUTER JOIN c_collectors c ON c.storage_profile_id = sp.id
WHERE
  c.deleted_at IS NULL
  AND sp.bucket = @bucket_name;
