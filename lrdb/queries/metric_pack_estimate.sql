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

