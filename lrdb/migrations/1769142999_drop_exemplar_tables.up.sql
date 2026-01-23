-- 1769142999_drop_exemplar_tables.up.sql

-- Drop exemplar tables and related function
DROP TABLE IF EXISTS lrdb_exemplar_logs CASCADE;
DROP TABLE IF EXISTS lrdb_exemplar_metrics CASCADE;
DROP TABLE IF EXISTS lrdb_exemplar_traces CASCADE;
DROP FUNCTION IF EXISTS add_to_bigint_list(BIGINT[], BIGINT, INTEGER);
