-- Add eligibility column to inqueue for deferred processing
ALTER TABLE public.inqueue
  ADD COLUMN IF NOT EXISTS eligible_at timestamptz NOT NULL DEFAULT now();

-- Index for efficient queries on eligible items per signal
CREATE INDEX IF NOT EXISTS idx_inqueue_eligible
ON public.inqueue (signal, eligible_at, priority ASC, queue_ts, id)
WHERE claimed_at IS NULL;

-- Index for finding items by grouping key when eligible
CREATE INDEX IF NOT EXISTS idx_inqueue_eligible_group
ON public.inqueue (signal, organization_id, instance_num, eligible_at, priority ASC, queue_ts, id)
WHERE claimed_at IS NULL;