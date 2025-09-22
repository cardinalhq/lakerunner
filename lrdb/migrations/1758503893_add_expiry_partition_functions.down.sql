-- Drop functions
DROP FUNCTION IF EXISTS expire_published_by_ingest_cutoff(regclass, uuid, integer, integer);
DROP FUNCTION IF EXISTS find_org_partition(regclass, uuid);