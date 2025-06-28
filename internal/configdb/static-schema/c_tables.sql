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
  hosted BOOLEAN NOT NULL,
  properties JSONB NOT NULL,
  role TEXT
);
