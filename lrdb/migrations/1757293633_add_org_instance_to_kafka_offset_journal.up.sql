-- Add organization_id and instance_num columns to kafka_offset_journal table
-- These columns are needed to track offsets per organization and instance for compaction

TRUNCATE TABLE kafka_offset_journal;

-- Add the new columns with defaults
ALTER TABLE kafka_offset_journal
ADD COLUMN organization_id UUID NOT NULL,
ADD COLUMN instance_num SMALLINT NOT NULL;

-- Drop the existing primary key constraint
ALTER TABLE kafka_offset_journal DROP CONSTRAINT kafka_offset_journal_pkey;

-- Create new primary key with all required columns
ALTER TABLE kafka_offset_journal
ADD CONSTRAINT kafka_offset_journal_pkey
PRIMARY KEY (consumer_group, topic, partition, organization_id, instance_num);

-- Update comments to document the new columns
COMMENT ON COLUMN kafka_offset_journal.organization_id IS 'Organization UUID for multi-tenant offset tracking';
COMMENT ON COLUMN kafka_offset_journal.instance_num IS 'Instance number for distributed processing offset tracking';
