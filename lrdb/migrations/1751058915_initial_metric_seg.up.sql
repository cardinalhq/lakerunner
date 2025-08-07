-- 1751058915_initial_metric_seg.up.sql

CREATE TABLE IF NOT EXISTS metric_seg (
  organization_id   UUID        NOT NULL,
  dateint           INTEGER     NOT NULL,
  frequency_ms      INTEGER     NOT NULL,
  segment_id        BIGINT      NOT NULL,
  instance_num      SMALLINT    NOT NULL,
  tid_partition     SMALLINT    NOT NULL,
  ts_range          int8range   NOT NULL,
  record_count      BIGINT      NOT NULL,
  file_size         BIGINT      NOT NULL,
  tid_count         INTEGER     NOT NULL DEFAULT 0,
  ingest_dateint    INTEGER     NOT NULL DEFAULT to_char(current_date, 'YYYYMMDD')::INTEGER,
  published         BOOLEAN     NOT NULL,
  rolledup          BOOLEAN     NOT NULL DEFAULT FALSE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (organization_id, dateint, frequency_ms, segment_id, instance_num, tid_partition),
  CONSTRAINT metric_seg_tsrange_not_empty CHECK (NOT isempty(ts_range)),
  CONSTRAINT metric_seg_ingest_dateint_check CHECK (ingest_dateint > 0)
)
PARTITION BY LIST (organization_id);

CREATE INDEX IF NOT EXISTS metric_seg_tsrange_gist ON metric_seg USING gist (ts_range);

CREATE INDEX IF NOT EXISTS metric_seg_ingest_dateint_idx ON metric_seg (ingest_dateint);

CREATE INDEX IF NOT EXISTS idx_queryapi_lookup
  ON metric_seg (organization_id, dateint, frequency_ms)
  INCLUDE (instance_num, segment_id, published)
  WHERE published = TRUE;

CREATE OR REPLACE FUNCTION create_metricseg_partition(
  base_table TEXT,
  partition_name TEXT,
  organization_id UUID,
  dateint_start INTEGER,
  dateint_end INTEGER
) RETURNS VOID AS $$
DECLARE
  subpartition_name TEXT;
BEGIN
  -- 1st level partition by organization_id
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES IN (%L) PARTITION BY RANGE (dateint);',
    partition_name, base_table, organization_id
  );

  -- 2nd level partition by dateint range
  subpartition_name := partition_name || '_' || dateint_start || '_' || (dateint_end - dateint_start);
  EXECUTE format(
    'CREATE TABLE IF NOT EXISTS %I PARTITION OF %I FOR VALUES FROM (%s) TO (%s);',
    subpartition_name, partition_name, dateint_start, dateint_end
  );
END;
$$ LANGUAGE plpgsql;
