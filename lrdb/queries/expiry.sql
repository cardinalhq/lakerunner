-- name: CallFindOrgPartition :one
SELECT find_org_partition(@table_name::regclass, @organization_id::uuid)::text AS partition_name;

-- name: CallExpirePublishedByIngestCutoff :one
SELECT expire_published_by_ingest_cutoff(@partition_name::regclass, @organization_id::uuid, @cutoff_dateint::integer, @batch_size::integer) AS rows_expired;