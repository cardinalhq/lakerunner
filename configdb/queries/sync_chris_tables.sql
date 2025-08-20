-- This file contains queries for syncing data from legacy c_ tables (managed externally)
-- to our own lrconfig_ tables that we control.

-- name: SyncOrganizations :exec
INSERT INTO lrconfig_organizations (id, name, enabled, created_at, synced_at)
SELECT id, name, COALESCE(enabled, true), COALESCE(created_at, NOW()), NOW()
FROM c_organizations
ON CONFLICT (id) DO UPDATE SET
  name = EXCLUDED.name,
  enabled = EXCLUDED.enabled,
  synced_at = NOW();