CREATE INDEX IF NOT EXISTS idx_metric_seg_fingerprints ON metric_seg USING gin (fingerprints);
