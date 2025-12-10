-- 1765323283_add_sort_version_to_log_seg.down.sql

ALTER TABLE log_seg
  DROP COLUMN IF EXISTS sort_version;
