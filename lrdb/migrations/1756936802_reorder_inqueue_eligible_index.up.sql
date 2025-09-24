-- 1756936802_reorder_inqueue_eligible_index.up.sql

-- Reorder idx_inqueue_eligible to put eligible_at first for better range scan performance
-- This improves the seed selection query in inqueue bundle claiming

-- NOTE: Removed operations on inqueue since table is never created (later dropped)

-- Drop the old index
-- DROP INDEX IF EXISTS idx_inqueue_eligible;

-- Create the new index with eligible_at as the leading column
-- CREATE INDEX idx_inqueue_eligible ON inqueue (eligible_at, signal, priority, queue_ts, id)
-- WHERE claimed_at IS NULL;
