-- 1752046968_inqueue_improve.up.sql

CREATE INDEX IF NOT EXISTS idx_inqueue_claimed_at_nonnull
  ON inqueue(claimed_at)
  WHERE claimed_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_inqueue_ready
  ON inqueue(telemetry_type, priority DESC, queue_ts)
  WHERE claimed_at IS NULL;
