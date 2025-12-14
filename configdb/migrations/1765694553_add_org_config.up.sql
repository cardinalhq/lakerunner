-- organization_config stores per-organization configuration with system-wide defaults.
-- The nil UUID (00000000-0000-0000-0000-000000000000) is used for system defaults.
-- Config values are stored as JSONB; the accessor knows the expected structure.

CREATE TABLE IF NOT EXISTS organization_config (
    organization_id UUID NOT NULL,
    key TEXT NOT NULL,
    value JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (organization_id, key)
);

-- Index for looking up all configs for an org
CREATE INDEX IF NOT EXISTS idx_organization_config_org_id ON organization_config (organization_id);

-- Insert system default for log stream field
INSERT INTO organization_config (organization_id, key, value)
VALUES (
    '00000000-0000-0000-0000-000000000000',
    'log.stream',
    '{"field_name": "resource_service_name"}'
) ON CONFLICT (organization_id, key) DO NOTHING;
