-- 1755582182_bucket_management_tables.down.sql

-- Drop tables in reverse order (respecting foreign key dependencies)
DROP TABLE IF EXISTS bucket_prefix_mappings;
DROP TABLE IF EXISTS organization_buckets;
DROP TABLE IF EXISTS bucket_configurations;
