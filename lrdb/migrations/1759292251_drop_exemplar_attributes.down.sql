-- Re-add attributes column to exemplar tables
-- This is the rollback for dropping the attributes column

ALTER TABLE lrdb_exemplar_metrics ADD COLUMN IF NOT EXISTS attributes JSONB NOT NULL DEFAULT '{}';
ALTER TABLE lrdb_exemplar_logs ADD COLUMN IF NOT EXISTS attributes JSONB NOT NULL DEFAULT '{}';
ALTER TABLE lrdb_exemplar_traces ADD COLUMN IF NOT EXISTS attributes JSONB NOT NULL DEFAULT '{}';
