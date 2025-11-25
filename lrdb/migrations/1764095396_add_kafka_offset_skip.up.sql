-- kafka_offset_skip stores target offsets that consumers should skip to.
-- When a consumer sees an entry for its group/topic/partition, it commits
-- that offset to Kafka and deletes the entry.
CREATE TABLE kafka_offset_skip (
  id              bigserial   PRIMARY KEY,
  consumer_group  text        NOT NULL,
  topic           text        NOT NULL,
  partition_id    integer     NOT NULL,
  skip_to_offset  bigint      NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  UNIQUE (consumer_group, topic, partition_id)
);

CREATE INDEX kafka_offset_skip_lookup_idx
  ON kafka_offset_skip (consumer_group, topic);
