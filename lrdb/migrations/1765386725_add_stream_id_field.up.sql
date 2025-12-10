-- Add stream_id_field column to track which field (resource_customer_domain or resource_service_name)
-- was used to derive the stream identifiers stored in stream_ids.
ALTER TABLE log_seg ADD COLUMN IF NOT EXISTS stream_id_field TEXT DEFAULT NULL;

-- Backfill: set stream_id_field='stream_id' for existing rows that have stream_ids populated.
-- This provides backward compatibility for legacy data.
UPDATE log_seg SET stream_id_field = 'stream_id' WHERE stream_ids IS NOT NULL;

-- Add constraint: stream_id_field and stream_ids must both be NULL or both be NOT NULL.
-- This ensures data consistency - we can't have field name without values or values without field name.
ALTER TABLE log_seg ADD CONSTRAINT stream_id_consistency
  CHECK ((stream_id_field IS NULL AND stream_ids IS NULL)
      OR (stream_id_field IS NOT NULL AND stream_ids IS NOT NULL));
