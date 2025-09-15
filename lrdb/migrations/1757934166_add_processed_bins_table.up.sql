CREATE TABLE IF NOT EXISTS kafka_offset_tracker (
  id              bigserial   PRIMARY KEY,
  consumer_group  text        NOT NULL,
  topic           text        NOT NULL,
  partition_id    integer     NOT NULL,
  min_offset      bigint      NOT NULL,
  max_offset      bigint      NOT NULL,
  offsets         bigint[]    NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  CHECK (min_offset <= max_offset),
  CHECK (array_length(offsets, 1) > 0)
);

CREATE INDEX IF NOT EXISTS kafka_offset_tracker_lookup_idx
  ON kafka_offset_tracker (consumer_group, topic, partition_id, max_offset)
  INCLUDE (min_offset, offsets, created_at);

CREATE INDEX IF NOT EXISTS kafka_offset_tracker_created_at_idx
  ON kafka_offset_tracker (created_at);