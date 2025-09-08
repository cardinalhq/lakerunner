-- Remove organization_id and instance_num columns from kafka_offset_journal table

-- Drop the current primary key
ALTER TABLE kafka_offset_journal DROP CONSTRAINT kafka_offset_journal_pkey;

-- Remove the added columns
ALTER TABLE kafka_offset_journal 
DROP COLUMN organization_id,
DROP COLUMN instance_num;

-- Restore the original primary key
ALTER TABLE kafka_offset_journal 
ADD CONSTRAINT kafka_offset_journal_pkey 
PRIMARY KEY (consumer_group, topic, partition);