-- Restore original primary keys (without source column)
ALTER TABLE lrdb_exemplar_logs DROP CONSTRAINT lrdb_exemplar_logs_pkey;
ALTER TABLE lrdb_exemplar_logs ADD PRIMARY KEY (organization_id, service_identifier_id, fingerprint);

ALTER TABLE lrdb_exemplar_metrics DROP CONSTRAINT lrdb_exemplar_metrics_pkey;
ALTER TABLE lrdb_exemplar_metrics ADD PRIMARY KEY (organization_id, service_identifier_id, metric_name, metric_type);

ALTER TABLE lrdb_exemplar_traces DROP CONSTRAINT lrdb_exemplar_traces_pkey;
ALTER TABLE lrdb_exemplar_traces ADD PRIMARY KEY (organization_id, service_identifier_id, fingerprint);

-- Remove source column from exemplar tables
ALTER TABLE lrdb_exemplar_logs DROP COLUMN source;
ALTER TABLE lrdb_exemplar_metrics DROP COLUMN source;
ALTER TABLE lrdb_exemplar_traces DROP COLUMN source;
