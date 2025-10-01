-- Drop attributes column from exemplar tables
-- The attributes column is redundant as the exemplar JSONB column already contains all the data

ALTER TABLE lrdb_exemplar_metrics DROP COLUMN IF EXISTS attributes;
ALTER TABLE lrdb_exemplar_logs DROP COLUMN IF EXISTS attributes;
ALTER TABLE lrdb_exemplar_traces DROP COLUMN IF EXISTS attributes;
