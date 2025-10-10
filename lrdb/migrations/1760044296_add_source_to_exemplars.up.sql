-- Add source column to exemplar tables with default value
ALTER TABLE lrdb_exemplar_logs ADD COLUMN source TEXT DEFAULT 'lakerunner';
ALTER TABLE lrdb_exemplar_metrics ADD COLUMN source TEXT DEFAULT 'lakerunner';
ALTER TABLE lrdb_exemplar_traces ADD COLUMN source TEXT DEFAULT 'lakerunner';

-- Make the column NOT NULL
ALTER TABLE lrdb_exemplar_logs ALTER COLUMN source SET NOT NULL;
ALTER TABLE lrdb_exemplar_metrics ALTER COLUMN source SET NOT NULL;
ALTER TABLE lrdb_exemplar_traces ALTER COLUMN source SET NOT NULL;
