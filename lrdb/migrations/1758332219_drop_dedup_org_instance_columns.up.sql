-- Drop organization_id and instance_num columns from pubsub_message_history
-- These are no longer needed since we're doing early deduplication based only on bucket/object/source

-- First drop the existing primary key constraint
ALTER TABLE pubsub_message_history DROP CONSTRAINT pubsub_message_history_pkey;

-- Drop the columns
ALTER TABLE pubsub_message_history
DROP COLUMN organization_id,
DROP COLUMN instance_num;

-- Add new primary key constraint based on bucket, object_id, source
ALTER TABLE pubsub_message_history ADD CONSTRAINT pubsub_message_history_pkey PRIMARY KEY (bucket, object_id, source);
