-- name: GetAllMetricPackEstimates :many
-- Retrieves all existing metric pack estimates for EWMA calculations
SELECT
  organization_id,
  frequency_ms,
  target_records,
  updated_at
FROM metric_pack_estimate
ORDER BY organization_id, frequency_ms;

-- name: UpsertMetricPackEstimate :exec
-- Updates or inserts a single metric pack estimate
INSERT INTO metric_pack_estimate (organization_id, frequency_ms, target_records, updated_at)
VALUES (@organization_id, @frequency_ms, @target_records, now())
ON CONFLICT (organization_id, frequency_ms)
DO UPDATE SET
  target_records = EXCLUDED.target_records,
  updated_at = now();

-- name: GetMetricPackEstimateForOrg :many
-- Gets metric pack estimate for specific org with fallback to default (all zeros)
-- Returns up to 2 rows: one for the specific org and one for the default
SELECT
  organization_id,
  frequency_ms,
  target_records,
  updated_at
FROM metric_pack_estimate
WHERE (organization_id = @organization_id OR organization_id = '00000000-0000-0000-0000-000000000000')
  AND frequency_ms = @frequency_ms
ORDER BY organization_id DESC; -- Puts specific org first if it exists
