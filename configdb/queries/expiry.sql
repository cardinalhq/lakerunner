-- Copyright (C) 2025 CardinalHQ, Inc
--
-- This program is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as
-- published by the Free Software Foundation, version 3.
--
-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU Affero General Public License for more details.
--
-- You should have received a copy of the GNU Affero General Public License
-- along with this program. If not, see <http://www.gnu.org/licenses/>.

-- name: GetActiveOrganizations :many
SELECT id, name, enabled
FROM organizations
WHERE enabled = true;

-- name: GetOrganizationExpiry :one
SELECT
    organization_id,
    signal_type,
    max_age_days,
    created_at,
    updated_at
FROM organization_signal_expiry
WHERE organization_id = @organization_id
  AND signal_type = @signal_type;

-- name: UpsertOrganizationExpiry :exec
INSERT INTO organization_signal_expiry (
    organization_id,
    signal_type,
    max_age_days
) VALUES (
    @organization_id, @signal_type, @max_age_days
)
ON CONFLICT (organization_id, signal_type)
DO UPDATE SET
    max_age_days = EXCLUDED.max_age_days,
    updated_at = NOW();

-- name: GetExpiryLastRun :one
SELECT
    organization_id,
    signal_type,
    last_run_at,
    created_at,
    updated_at
FROM expiry_run_tracking
WHERE organization_id = @organization_id
  AND signal_type = @signal_type;

-- name: UpsertExpiryRunTracking :exec
INSERT INTO expiry_run_tracking (
    organization_id,
    signal_type,
    last_run_at
) VALUES (
    @organization_id, @signal_type, NOW()
)
ON CONFLICT (organization_id, signal_type)
DO UPDATE SET
    last_run_at = NOW(),
    updated_at = NOW();

-- name: CallFindOrgPartition :one
SELECT find_org_partition(@table_name::regclass, @organization_id::uuid)::text AS partition_name;

-- name: CallExpirePublishedByIngestCutoff :one
SELECT expire_published_by_ingest_cutoff(@partition_name::regclass, @organization_id::uuid, @cutoff_dateint::integer, @batch_size::integer) AS rows_expired;