-- Add agg_fields column to log_seg for tracking pre-calculated aggregation fields
ALTER TABLE log_seg ADD COLUMN IF NOT EXISTS agg_fields TEXT[] DEFAULT NULL;
