-- 1755823992_add_slot_id_to_logs_and_metrics.down.sql

-- Remove slot_id column from metric_seg table
ALTER TABLE metric_seg DROP COLUMN IF EXISTS slot_id;

-- Remove slot_id column from log_seg table
ALTER TABLE log_seg DROP COLUMN IF EXISTS slot_id;