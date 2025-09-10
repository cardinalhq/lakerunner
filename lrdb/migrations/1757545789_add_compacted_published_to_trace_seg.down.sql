-- Remove compacted and published columns from trace_seg table

-- Drop the indexes first
DROP INDEX IF EXISTS idx_trace_seg_compacted;
DROP INDEX IF EXISTS idx_trace_seg_published;

-- Remove the columns
ALTER TABLE trace_seg 
DROP COLUMN compacted,
DROP COLUMN published;