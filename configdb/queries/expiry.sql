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