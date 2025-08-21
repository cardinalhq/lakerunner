-- 1755757162_prevent_instance_num_zero.up.sql

-- Add constraint to prevent instance_num=0 which causes bucket mismatches
ALTER TABLE public.work_queue 
ADD CONSTRAINT work_queue_instance_num_positive 
CHECK (instance_num > 0);
