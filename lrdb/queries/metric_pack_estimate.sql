-- name: GetAllPackEstimates :many
-- Retrieves all existing pack estimates for EWMA calculations across all signals
SELECT
  organization_id,
  frequency_ms,
  signal,
  target_records,
  updated_at
FROM pack_estimate
ORDER BY organization_id, frequency_ms, signal;

-- name: GetMetricPackEstimates :many
-- Retrieves metric pack estimates for EWMA calculations (backward compatibility)
SELECT
  organization_id,
  frequency_ms,
  target_records,
  updated_at
FROM pack_estimate
WHERE signal = 'metrics'
ORDER BY organization_id, frequency_ms;

-- name: UpsertPackEstimate :exec
-- Updates or inserts a single pack estimate for any signal type
INSERT INTO pack_estimate (organization_id, frequency_ms, signal, target_records, updated_at)
VALUES (@organization_id, @frequency_ms, @signal, @target_records, now())
ON CONFLICT (organization_id, frequency_ms, signal)
DO UPDATE SET
  target_records = EXCLUDED.target_records,
  updated_at = now();

-- name: UpsertMetricPackEstimate :exec  
-- Updates or inserts a single metric pack estimate (backward compatibility)
INSERT INTO pack_estimate (organization_id, frequency_ms, signal, target_records, updated_at)
VALUES (@organization_id, @frequency_ms, 'metrics', @target_records, now())
ON CONFLICT (organization_id, frequency_ms, signal)
DO UPDATE SET
  target_records = EXCLUDED.target_records,
  updated_at = now();

-- name: GetAllBySignal :many
-- Retrieves all pack estimates for a specific signal type
SELECT
  organization_id,
  frequency_ms,
  target_records,
  updated_at
FROM pack_estimate
WHERE signal = @signal
ORDER BY organization_id, frequency_ms;

