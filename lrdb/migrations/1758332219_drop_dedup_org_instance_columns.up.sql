
ALTER TABLE pubsub_message_history DROP CONSTRAINT pubsub_message_history_pkey;

ALTER TABLE pubsub_message_history
DROP COLUMN organization_id,
DROP COLUMN instance_num;

ALTER TABLE pubsub_message_history ADD CONSTRAINT pubsub_message_history_pkey PRIMARY KEY (bucket, object_id, source);
