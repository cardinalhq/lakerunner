-- 1755757162_prevent_instance_num_zero.up.sql

-- Add constraint to prevent instance_num=0 which causes bucket mismatches
-- NOTE: Removed operations on work_queue since table is never created (later dropped)
-- DELETE FROM public.work_queue  WHERE instance_num = 0;

-- ALTER TABLE public.work_queue
-- ADD CONSTRAINT work_queue_instance_num_positive
-- CHECK (instance_num > 0);
