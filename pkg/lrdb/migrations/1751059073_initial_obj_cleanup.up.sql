-- 1751059073_initial_obj_cleanup.up.sql

CREATE TABLE IF NOT EXISTS obj_cleanup (
  id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  delete_at       TIMESTAMPTZ NOT NULL DEFAULT now() + INTERVAL '30 minutes',
  organization_id UUID        NOT NULL,
  instance_num    SMALLINT    NOT NULL,
  bucket_id       TEXT        NOT NULL,
  object_id       TEXT        NOT NULL,
  tries           INTEGER     NOT NULL DEFAULT 0
);

CREATE UNIQUE INDEX IF NOT EXISTS obj_cleanup_conflict
  ON obj_cleanup (organization_id, bucket_id, object_id, instance_num);

CREATE INDEX IF NOT EXISTS idx_obj_cleanup_delete_at
  ON obj_cleanup (delete_at);
