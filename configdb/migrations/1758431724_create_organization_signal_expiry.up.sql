-- Table to configure data retention policies per organization and signal type
-- A value of -1 means use system default, 0 means never expire, >0 means specific retention days
CREATE TABLE organization_signal_expiry (
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    signal_type TEXT NOT NULL CHECK (signal_type IN ('logs', 'metrics', 'traces')),
    max_age_days INTEGER NOT NULL CHECK (max_age_days >= -1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (organization_id, signal_type)
);

-- Table to track when expiry cleanup was last run
-- Separate from policy to avoid creating policy records just for tracking
CREATE TABLE expiry_run_tracking (
    organization_id UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    signal_type VARCHAR(50) NOT NULL CHECK (signal_type IN ('logs', 'metrics', 'traces')),
    last_run_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (organization_id, signal_type)
);

-- Indexes for efficient queries
CREATE INDEX idx_organization_signal_expiry_signal_type ON organization_signal_expiry(signal_type);
CREATE INDEX idx_expiry_run_tracking_last_run ON expiry_run_tracking (last_run_at);

-- Add comments to clarify table purposes
COMMENT ON TABLE organization_signal_expiry IS 'Stores data retention policies for each organization and signal type. A value of -1 means use system default, 0 means never expire, >0 means specific retention days.';
COMMENT ON TABLE expiry_run_tracking IS 'Tracks when expiry cleanup was last run for each organization and signal type. Separate from policy to avoid creating policy records just for tracking.';
