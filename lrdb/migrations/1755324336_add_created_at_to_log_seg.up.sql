ALTER TABLE log_seg ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP;

-- Create index for efficient pagination within each partition
-- Since the table is partitioned by organization_id and dateint, we only need instance_num, created_at, segment_id
CREATE INDEX IF NOT EXISTS idx_log_seg_created_at_segment_id ON log_seg (instance_num, created_at, segment_id);
