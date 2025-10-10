-- Add source column to exemplar tables with default value
ALTER TABLE lrdb_exemplar_logs ADD COLUMN source TEXT DEFAULT 'lakerunner';
ALTER TABLE lrdb_exemplar_metrics ADD COLUMN source TEXT DEFAULT 'lakerunner';
ALTER TABLE lrdb_exemplar_traces ADD COLUMN source TEXT DEFAULT 'lakerunner';

-- Make the column NOT NULL
ALTER TABLE lrdb_exemplar_logs ALTER COLUMN source SET NOT NULL;
ALTER TABLE lrdb_exemplar_metrics ALTER COLUMN source SET NOT NULL;
ALTER TABLE lrdb_exemplar_traces ALTER COLUMN source SET NOT NULL;

-- Drop the default so new inserts must explicitly provide a value
ALTER TABLE lrdb_exemplar_logs ALTER COLUMN source DROP DEFAULT;
ALTER TABLE lrdb_exemplar_metrics ALTER COLUMN source DROP DEFAULT;
ALTER TABLE lrdb_exemplar_traces ALTER COLUMN source DROP DEFAULT;

-- Update primary keys to include source column
-- This allows multiple sources to store exemplars with the same fingerprint

-- lrdb_exemplar_logs
ALTER TABLE lrdb_exemplar_logs DROP CONSTRAINT lrdb_exemplar_logs_pkey;
ALTER TABLE lrdb_exemplar_logs ADD PRIMARY KEY (organization_id, service_identifier_id, fingerprint, source);

-- lrdb_exemplar_metrics
ALTER TABLE lrdb_exemplar_metrics DROP CONSTRAINT lrdb_exemplar_metrics_pkey;
ALTER TABLE lrdb_exemplar_metrics ADD PRIMARY KEY (organization_id, service_identifier_id, metric_name, metric_type, source);

-- lrdb_exemplar_traces
ALTER TABLE lrdb_exemplar_traces DROP CONSTRAINT lrdb_exemplar_traces_pkey;
ALTER TABLE lrdb_exemplar_traces ADD PRIMARY KEY (organization_id, service_identifier_id, fingerprint, source);
