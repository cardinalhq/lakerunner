CREATE TABLE c_collectors (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  deleted_at TIMESTAMP WITH TIME ZONE,
  organization_id UUID NOT NULL,
  storage_profile_id UUID NOT NULL,
  instance_num SMALLINT NOT NULL,
  external_id TEXT NOT NULL,
  type INTEGER NOT NULL
);

CREATE TABLE c_storage_profiles (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  organization_id UUID NOT NULL,
  cloud_provider TEXT NOT NULL,
  bucket TEXT NOT NULL,
  region TEXT NOT NULL,
  role TEXT
);

CREATE TABLE c_organization_api_keys (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  created_at TIMESTAMP WITHOUT TIME ZONE,
  updated_at TIMESTAMP WITHOUT TIME ZONE,
  created_by TEXT,
  updated_by TEXT,
  organization_id UUID,
  name TEXT,
  api_key TEXT,
  enabled BOOLEAN
);
