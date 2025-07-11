-- 1751060066_initial_inqueue_journal.up.sql

CREATE TABLE IF NOT EXISTS inqueue_journal (
  id BIGSERIAL PRIMARY KEY,
  organization_id UUID NOT NULL,
  bucket TEXT NOT NULL,
  object_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (organization_id, bucket, object_id)
);
