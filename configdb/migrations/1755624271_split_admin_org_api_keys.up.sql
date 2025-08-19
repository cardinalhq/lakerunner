-- 1755624271_split_admin_org_api_keys.up.sql

-- Rename existing API key tables to be organization-specific
ALTER TABLE lrconfig_api_keys RENAME TO lrconfig_organization_api_keys;
ALTER TABLE lrconfig_api_key_organizations RENAME TO lrconfig_organization_api_key_mappings;

-- Update indexes to match new table names
DROP INDEX IF EXISTS idx_api_keys_hash;
DROP INDEX IF EXISTS idx_api_key_orgs_key;
DROP INDEX IF EXISTS idx_api_key_orgs_org;

CREATE INDEX idx_org_api_keys_hash ON lrconfig_organization_api_keys(key_hash);
CREATE INDEX idx_org_api_key_mappings_key ON lrconfig_organization_api_key_mappings(api_key_id);
CREATE INDEX idx_org_api_key_mappings_org ON lrconfig_organization_api_key_mappings(organization_id);

-- Create dedicated admin API keys table (no organization mapping needed)
CREATE TABLE lrconfig_admin_api_keys (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  key_hash TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  description TEXT,
  created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Index for admin API key hash lookups
CREATE INDEX idx_admin_api_keys_hash ON lrconfig_admin_api_keys(key_hash);