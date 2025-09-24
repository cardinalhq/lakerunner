DROP INDEX IF EXISTS idx_trace_seg_published;

CREATE INDEX idx_log_seg_cleanup ON log_seg
USING btree (organization_id, dateint, created_at)
WHERE published = false;

CREATE INDEX idx_metric_seg_cleanup ON metric_seg
USING btree (organization_id, dateint, created_at)
WHERE published = false;

CREATE INDEX idx_trace_seg_cleanup ON trace_seg
USING btree (organization_id, dateint, created_at)
WHERE published = false;
