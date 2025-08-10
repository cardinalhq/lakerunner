-- 1754806177_workqueue_depth_index.up.sql

CREATE INDEX IF NOT EXISTS work_queue_depth_index ON work_queue (signal, action) WHERE needs_run = true;
