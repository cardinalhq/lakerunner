-- 1754722651_workqueue_index.up.sql

-- This index makes workqueue cleanup checks go from 500ms to 0.3ms
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wq_expirable ON public.work_queue (heartbeated_at) INCLUDE (id) WHERE claimed_by <> -1;
