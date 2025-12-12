-- Add index on stream_id_field to enable fast lookups when querying tag values.
-- This enables the query-api to quickly find segments where stream_id_field matches
-- the requested tag name, allowing PostgreSQL to return distinct stream_ids values
-- directly from segment metadata instead of fetching parquet files.
-- The index omits organization_id and dateint since it's created per partition table.
CREATE INDEX IF NOT EXISTS idx_log_seg_stream_id_field ON log_seg (stream_id_field) WHERE stream_id_field IS NOT NULL;
