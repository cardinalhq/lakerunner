-- Restore organization_id and instance_num columns to pubsub_message_history

-- Drop current primary key
ALTER TABLE pubsub_message_history DROP CONSTRAINT pubsub_message_history_pkey;

-- Add back the columns
ALTER TABLE pubsub_message_history
ADD COLUMN organization_id UUID NOT NULL DEFAULT '00000000-0000-0000-0000-000000000000',
ADD COLUMN instance_num SMALLINT NOT NULL DEFAULT 0;

-- Restore original primary key constraint
ALTER TABLE pubsub_message_history ADD CONSTRAINT pubsub_message_history_pkey PRIMARY KEY (organization_id, instance_num, bucket, object_id, source);