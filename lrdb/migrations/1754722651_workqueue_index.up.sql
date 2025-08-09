-- 1754722651_workqueue_index.up.sql

-- This index makes workqueue cleanup checks go from 500ms to 0.3ms
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wq_expirable ON public.work_queue (heartbeated_at) INCLUDE (id) WHERE claimed_by <> -1;

-- This makes sure we can delete signal locks fast.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_signal_locks_work_id ON public.signal_locks(work_id);

-- Adjust the autovacuum settings for the work_queue to handle a small-row-count-hot table.
ALTER TABLE public.work_queue
  SET (autovacuum_vacuum_scale_factor = 0.02,
       autovacuum_analyze_scale_factor = 0.02,
       autovacuum_vacuum_threshold = 1000,
       autovacuum_analyze_threshold = 1000);

-- for GC'ing old work queue items
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_wq_gc_runnable_at
  ON public.work_queue (runnable_at, id)
  WHERE claimed_by = -1
    AND NOT needs_run;
