-- Drop the new cleanup indexes
DROP INDEX IF EXISTS idx_trace_seg_cleanup;
DROP INDEX IF EXISTS idx_metric_seg_cleanup;
DROP INDEX IF EXISTS idx_log_seg_cleanup;

-- Restore the original trace_seg published index
CREATE INDEX idx_trace_seg_published ON trace_seg
USING btree (organization_id, dateint, instance_num, published)
WHERE NOT published;