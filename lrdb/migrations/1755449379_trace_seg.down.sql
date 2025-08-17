-- 1755449379_trace_seg.down.sql

DROP FUNCTION IF EXISTS create_tracefpseg_partition(TEXT, TEXT, UUID, INTEGER, INTEGER);
DROP TABLE IF EXISTS trace_seg CASCADE;