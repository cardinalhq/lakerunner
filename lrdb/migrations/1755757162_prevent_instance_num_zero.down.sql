-- 1755757162_prevent_instance_num_zero.down.sql

-- Remove constraint that prevents instance_num=0
ALTER TABLE public.work_queue 
DROP CONSTRAINT work_queue_instance_num_positive;

