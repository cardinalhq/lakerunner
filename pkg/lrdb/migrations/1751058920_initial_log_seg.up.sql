-- 1751058920_initial_log_seg.up.sql

CREATE TABLE IF NOT EXISTS log_seg (
  organization_id   UUID        NOT NULL,
  dateint           INTEGER     NOT NULL,
  segment_id        BIGINT      NOT NULL,
  instance_num      SMALLINT    NOT NULL,
  fingerprints      BIGINT[]    NOT NULL DEFAULT '{}',
  record_count      BIGINT      NOT NULL,
  file_size         BIGINT      NOT NULL,
  ingest_dateint    INTEGER     NOT NULL DEFAULT to_char(current_date, 'YYYYMMDD')::INTEGER,
  ts_range          int8range   NOT NULL,
  PRIMARY KEY (organization_id, dateint, segment_id, instance_num),
  CONSTRAINT log_seg_tsrange_not_empty CHECK (NOT isempty(ts_range)),
  CONSTRAINT log_seg_ingest_dateint_check CHECK (ingest_dateint > 0)
)
PARTITION BY LIST (organization_id);

CREATE INDEX IF NOT EXISTS log_seg_tsrange_gist ON log_seg USING gist (ts_range);

CREATE INDEX IF NOT EXISTS idx_log_seg_fingerprints ON log_seg USING gin (fingerprints);

CREATE INDEX IF NOT EXISTS log_seg_ingest_dateint_idx ON log_seg (ingest_dateint);

CREATE OR REPLACE FUNCTION create_logfpseg_partition(
  base_table TEXT,
  base_table_name TEXT,
  organization_id UUID,
  dateint_start INTEGER,
  dateint_end INTEGER
) RETURNS VOID AS $$
DECLARE
  partition_name TEXT;
  subpartition_name TEXT;
BEGIN
  -- 1st level partition by organization_id
  partition_name := base_table_name;
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES IN (%L) PARTITION BY RANGE (dateint);',
    partition_name, base_table, organization_id
  );

  -- 2nd level partition by dateint range
  subpartition_name := base_table_name || '_' || dateint_start || '_' || (dateint_end - dateint_start);
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s);',
    subpartition_name, partition_name, dateint_start, dateint_end
  );
END;
$$ LANGUAGE plpgsql;
