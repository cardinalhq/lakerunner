CREATE TABLE c_organizations (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  created_at TIMESTAMP WITHOUT TIME ZONE,
  updated_at TIMESTAMP WITHOUT TIME ZONE,
  created_by TEXT,
  updated_by TEXT,
  name TEXT,
  enabled BOOLEAN,
  notification_slack TEXT,
  notification_email TEXT,
  notification_pager_duty_service TEXT,
  features JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE c_collectors (
  id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
  created_at TIMESTAMP WITHOUT TIME ZONE,
  updated_at TIMESTAMP WITHOUT TIME ZONE,
  created_by TEXT,
  updated_by TEXT,
  organization_id UUID,
  storage_profile_id UUID,
  instance_num SMALLINT NOT NULL,
  external_id TEXT NOT NULL,
  deleted_at TIMESTAMP WITHOUT TIME ZONE,
  deleted_by TEXT,
  type INTEGER NOT NULL DEFAULT 0,
  trusted BOOLEAN,
  version TEXT,
  saas BOOLEAN,
  style TEXT DEFAULT 'processor'::text,
  dag_hash_seed INTEGER,
  config_hash_seed INTEGER,
  collector_group TEXT NOT NULL DEFAULT 'UNKNOWN'::text,
  flags JSONB NOT NULL DEFAULT '{}'::jsonb
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
