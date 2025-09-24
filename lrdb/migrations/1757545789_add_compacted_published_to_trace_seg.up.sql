ALTER TABLE trace_seg
ADD COLUMN compacted bool NOT NULL DEFAULT false,
ADD COLUMN published bool NOT NULL DEFAULT false;

CREATE INDEX IF NOT EXISTS idx_trace_seg_compacted
ON trace_seg (organization_id, dateint, instance_num, compacted)
WHERE NOT compacted;

CREATE INDEX IF NOT EXISTS idx_trace_seg_published
ON trace_seg (organization_id, dateint, instance_num, published)
WHERE NOT published;
