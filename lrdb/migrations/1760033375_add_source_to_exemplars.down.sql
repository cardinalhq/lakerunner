-- Remove source column from exemplar tables
ALTER TABLE lrdb_exemplar_logs DROP COLUMN source;
ALTER TABLE lrdb_exemplar_metrics DROP COLUMN source;
ALTER TABLE lrdb_exemplar_traces DROP COLUMN source;
