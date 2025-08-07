-- 1751058923_initial_inqueue.up.sql

CREATE TABLE IF NOT EXISTS inqueue (
  id              UUID        NOT NULL PRIMARY KEY DEFAULT gen_random_uuid(),
  queue_ts        TIMESTAMPTZ NOT NULL DEFAULT now(),
  priority        INTEGER     NOT NULL DEFAULT 0,
  organization_id UUID        NOT NULL,
  collector_name  TEXT        NOT NULL,
  instance_num    SMALLINT    NOT NULL,
  bucket          TEXT        NOT NULL,
  object_id       TEXT        NOT NULL,
  telemetry_type  TEXT        NOT NULL,
  tries           INTEGER     NOT NULL DEFAULT 0,
  claimed_by      BIGINT      NOT NULL DEFAULT -1,
  claimed_at      TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_inqueue_pri_ts ON inqueue (priority, queue_ts);

CREATE UNIQUE INDEX IF NOT EXISTS inqueue_unique_idx ON inqueue (
  organization_id,
  bucket,
  object_id
);

DROP INDEX IF EXISTS inqueue_queue_ts;
