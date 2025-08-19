-- 1755582182_bucket_management_tables.up.sql

-- Bucket configurations table - stores bucket-level metadata
CREATE TABLE bucket_configurations (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  bucket_name TEXT NOT NULL UNIQUE,
  cloud_provider TEXT NOT NULL,
  region TEXT NOT NULL,
  endpoint TEXT,
  role TEXT
);

-- Index for bucket name lookups (primary access pattern)
CREATE INDEX idx_bucket_configs_name ON bucket_configurations(bucket_name);

-- Organization to bucket mappings - permissions table
CREATE TABLE organization_buckets (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  organization_id UUID NOT NULL,
  bucket_id UUID NOT NULL REFERENCES bucket_configurations(id) ON DELETE CASCADE,
  UNIQUE(organization_id, bucket_id)
);

-- Indexes for organization and bucket lookups
CREATE INDEX idx_org_buckets_org ON organization_buckets(organization_id);
CREATE INDEX idx_org_buckets_bucket ON organization_buckets(bucket_id);

-- Bucket prefix mappings for custom path routing
CREATE TABLE bucket_prefix_mappings (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  bucket_id UUID NOT NULL REFERENCES bucket_configurations(id) ON DELETE CASCADE,
  organization_id UUID NOT NULL,
  path_prefix TEXT NOT NULL,
  UNIQUE(bucket_id, path_prefix)
);

-- Index for prefix matching (order by length for longest match)
CREATE INDEX idx_prefix_bucket_prefix ON bucket_prefix_mappings(bucket_id, path_prefix text_pattern_ops);
-- Index for organization lookups
CREATE INDEX idx_prefix_org ON bucket_prefix_mappings(organization_id);

-- Constraint to ensure organization has access to the bucket in prefix mappings
ALTER TABLE bucket_prefix_mappings 
ADD CONSTRAINT fk_prefix_org_bucket 
FOREIGN KEY (organization_id, bucket_id) 
REFERENCES organization_buckets(organization_id, bucket_id) 
ON DELETE CASCADE 
DEFERRABLE INITIALLY DEFERRED;
