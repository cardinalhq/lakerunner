-- This migration cannot be reversed as we are permanently dropping SegmentJournal
-- The table structure was:
-- CREATE TABLE segment_journal (
--     id BIGSERIAL PRIMARY KEY,
--     organization_id UUID NOT NULL,
--     dateint INTEGER NOT NULL,
--     instance_num SMALLINT NOT NULL,
--     segment_id BIGINT NOT NULL,
--     status INTEGER NOT NULL DEFAULT 0,
--     created_at TIMESTAMP DEFAULT NOW(),
--     updated_at TIMESTAMP DEFAULT NOW(),
--     first_message_timestamp TIMESTAMP,
--     last_message_timestamp TIMESTAMP,
--     debugging_info JSONB
-- ) PARTITION BY HASH (organization_id);

SELECT 'segment_journal table drop cannot be reversed' as error;