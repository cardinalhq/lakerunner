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
    last_checked_at,
    created_at,
    updated_at
FROM organization_signal_expiry
WHERE organization_id = @organization_id
  AND signal_type = @signal_type;

-- name: GetExpiryToProcess :many
-- Get organizations that need expiry processing (not checked today)
SELECT
    ose.organization_id,
    ose.signal_type,
    ose.max_age_days,
    ose.last_checked_at,
    o.name as organization_name
FROM organization_signal_expiry ose
JOIN organizations o ON o.id = ose.organization_id
WHERE o.enabled = true
  AND ose.last_checked_at < CURRENT_DATE
ORDER BY ose.last_checked_at ASC;

-- name: UpsertOrganizationExpiry :exec
INSERT INTO organization_signal_expiry (
    organization_id,
    signal_type,
    max_age_days,
    last_checked_at
) VALUES (
    @organization_id, @signal_type, @max_age_days, NOW()
)
ON CONFLICT (organization_id, signal_type)
DO UPDATE SET
    max_age_days = EXCLUDED.max_age_days,
    last_checked_at = EXCLUDED.last_checked_at,
    updated_at = NOW();

-- name: UpdateExpiryCheckedAt :exec
UPDATE organization_signal_expiry
SET last_checked_at = NOW(),
    updated_at = NOW()
WHERE organization_id = @organization_id
  AND signal_type = @signal_type;

-- name: CallFindOrgPartition :one
SELECT find_org_partition(@table_name::regclass, @organization_id::uuid)::text AS partition_name;

-- name: CallExpirePublishedByIngestCutoff :one
SELECT expire_published_by_ingest_cutoff(@partition_name::regclass, @organization_id::uuid, @cutoff_dateint::integer, @batch_size::integer) AS rows_expired;