-- 1757555753_drop_work_queue_and_signal_locks.up.sql

-- Drop work_queue and signal_locks tables as they are no longer needed

-- Drop signal_locks table (has foreign key to work_queue)
DROP TABLE IF EXISTS signal_locks;

-- Drop work_queue table
DROP TABLE IF EXISTS work_queue;
