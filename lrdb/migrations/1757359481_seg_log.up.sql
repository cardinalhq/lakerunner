-- Create seg_log table for debugging segment operations across all signals
CREATE TABLE IF NOT EXISTS seg_log (
    id              BIGSERIAL       PRIMARY KEY,

    -- Required fields
    signal          SMALLINT        NOT NULL CHECK (signal > 0),
    action          SMALLINT        NOT NULL CHECK (action > 0),
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    organization_id UUID            NOT NULL,
    instance_num    SMALLINT        NOT NULL CHECK (instance_num > 0),
    dateint         INTEGER NOT NULL CHECK (dateint > 0),
    frequency_ms    INTEGER NOT NULL CHECK (frequency_ms > 0),

    -- Source files/segments
    source_count            INTEGER NOT NULL CHECK (source_count >= 0),
    source_object_keys      TEXT[] NOT NULL,
    source_total_records    BIGINT NOT NULL CHECK (source_total_records >= 0),
    source_total_size       BIGINT NOT NULL CHECK (source_total_size >= 0),

    -- Destination files/segments
    dest_count              INTEGER NOT NULL CHECK (dest_count >= 0),
    dest_object_keys        TEXT[] NOT NULL,
    dest_total_records      BIGINT NOT NULL CHECK (dest_total_records >= 0),
    dest_total_size         BIGINT NOT NULL CHECK (dest_total_size >= 0),

    -- Additional debugging context
    metadata        JSONB NOT NULL DEFAULT '{}'::JSONB
);

-- Create indexes for common query patterns
-- Primary index for expiration/cleanup queries
CREATE INDEX IF NOT EXISTS idx_seg_log_created_at
ON seg_log (created_at DESC);

-- Organization-focused queries with signal/action
CREATE INDEX IF NOT EXISTS idx_seg_log_org
ON seg_log (organization_id, instance_num, signal, action, created_at DESC);

-- Add table comment
COMMENT ON TABLE seg_log IS 'Debugging log for segment operations across all signals (logs, metrics, traces) and actions (ingest, compaction, rollups). Not typically enabled in production.';

-- Add column comments for key fields
COMMENT ON COLUMN seg_log.signal IS 'Signal type being processed (1=logs, 2=metrics, 3=traces)';
COMMENT ON COLUMN seg_log.action IS 'Action being performed (1=ingest, 2=compact, 3=rollup)';
COMMENT ON COLUMN seg_log.organization_id IS 'Organization ID for determining storage bucket/profile';
COMMENT ON COLUMN seg_log.instance_num IS 'Instance number for determining storage bucket/profile';
COMMENT ON COLUMN seg_log.dateint IS 'Date being processed (YYYYMMDD format)';
COMMENT ON COLUMN seg_log.frequency_ms IS 'Frequency in milliseconds (primarily for metrics operations)';
COMMENT ON COLUMN seg_log.source_object_keys IS 'S3 object keys for source files (for fetching and testing)';
COMMENT ON COLUMN seg_log.dest_object_keys IS 'S3 object keys for destination files (for verification)';
COMMENT ON COLUMN seg_log.source_total_records IS 'Total records from all source files';
COMMENT ON COLUMN seg_log.source_total_size IS 'Total bytes from all source files';
COMMENT ON COLUMN seg_log.dest_total_records IS 'Total records in all destination files';
COMMENT ON COLUMN seg_log.dest_total_size IS 'Total bytes in all destination files';
COMMENT ON COLUMN seg_log.metadata IS 'Additional debugging information in JSON format';
