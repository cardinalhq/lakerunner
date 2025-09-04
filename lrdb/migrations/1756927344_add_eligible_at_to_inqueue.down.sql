-- Drop indexes first
DROP INDEX IF EXISTS idx_inqueue_eligible_group;
DROP INDEX IF EXISTS idx_inqueue_eligible;

-- Remove the eligible_at column
ALTER TABLE public.inqueue DROP COLUMN IF EXISTS eligible_at;