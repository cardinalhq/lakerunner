-- 1765323283_add_sort_version_to_log_seg.up.sql

-- Add sort_version column to track log file sort order
-- See lrdb.LogSortVersion* constants for values:
-- 0: LogSortVersionUnknown - Unknown/unsorted (default for existing data)
-- 1: LogSortVersionTimestamp - Sorted by [timestamp] only (legacy)
-- 2: LogSortVersionFingerprintServiceTimestamp - Sorted by [fingerprint, service_identifier, timestamp]
ALTER TABLE log_seg
  ADD COLUMN sort_version SMALLINT NOT NULL DEFAULT 0;
