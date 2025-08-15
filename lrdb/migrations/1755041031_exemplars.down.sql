-- 1755041031_exemplar-metrics.down.sql
-- Reverse migration for comprehensive exemplar tables

-- Drop helper functions
DROP FUNCTION IF EXISTS add_to_bigint_list(BIGINT[], BIGINT, INTEGER);
DROP FUNCTION IF EXISTS merge_dedup_text(TEXT[], TEXT[]);

-- Drop exemplar tables
DROP TABLE IF EXISTS exemplar_traces CASCADE;
DROP TABLE IF EXISTS exemplar_logs CASCADE;
DROP TABLE IF EXISTS exemplar_metrics CASCADE;
DROP TABLE IF EXISTS service_identifiers CASCADE;
