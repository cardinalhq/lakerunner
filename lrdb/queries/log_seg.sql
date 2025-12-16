-- name: insertLogSegmentDirect :exec
INSERT INTO log_seg (
  organization_id,
  dateint,
  segment_id,
  instance_num,
  ts_range,
  record_count,
  file_size,
  created_by,
  fingerprints,
  published,
  compacted,
  label_name_map,
  stream_ids,
  stream_id_field,
  sort_version,
  agg_fields
)
VALUES (
  @organization_id,
  @dateint,
  @segment_id,
  @instance_num,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @created_by,
  @fingerprints::bigint[],
  @published,
  @compacted,
  @label_name_map,
  @stream_ids::text[],
  @stream_id_field,
  @sort_version,
  @agg_fields::text[]
);

-- name: GetLogSeg :one
SELECT *
FROM log_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND segment_id = @segment_id
  AND instance_num = @instance_num;

-- name: batchInsertLogSegsDirect :batchexec
INSERT INTO log_seg (
  organization_id,
  dateint,
  segment_id,
  instance_num,
  ts_range,
  record_count,
  file_size,
  created_by,
  fingerprints,
  published,
  compacted,
  label_name_map,
  stream_ids,
  stream_id_field,
  sort_version,
  agg_fields
)
VALUES (
  @organization_id,
  @dateint,
  @segment_id,
  @instance_num,
  int8range(@start_ts, @end_ts, '[)'),
  @record_count,
  @file_size,
  @created_by,
  @fingerprints::bigint[],
  @published,
  @compacted,
  @label_name_map,
  @stream_ids::text[],
  @stream_id_field,
  @sort_version,
  @agg_fields::text[]
);

-- name: MarkLogSegsCompactedByKeys :exec
UPDATE log_seg
SET compacted = true
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND compacted       = false;

-- name: markLogSegsCompactedUnpublishedByKeys :exec
UPDATE log_seg
SET compacted = true, published = false
WHERE organization_id = @organization_id
  AND dateint         = @dateint
  AND instance_num    = @instance_num
  AND segment_id      = ANY(@segment_ids::bigint[])
  AND compacted       = false;

-- name: ListLogSegmentsForQuery :many
SELECT
    t.fp::bigint                    AS fingerprint,
    s.instance_num,
    s.segment_id,
    lower(s.ts_range)::bigint        AS start_ts,
    (upper(s.ts_range) - 1)::bigint  AS end_ts,
    s.agg_fields
FROM log_seg AS s
         CROSS JOIN LATERAL
    unnest(s.fingerprints) AS t(fp)
WHERE
    s.organization_id = @organization_id
  AND s.dateint       = @dateint
  AND s.published     = true
  AND s.fingerprints && @fingerprints::BIGINT[]
  AND t.fp            = ANY(@fingerprints::BIGINT[])
  AND ts_range && int8range(@s, @e, '[)');

-- name: GetLabelNameMaps :many
SELECT
    segment_id,
    label_name_map
FROM log_seg
WHERE organization_id = @organization_id
  AND dateint = @dateint
  AND segment_id = ANY(@segment_ids::BIGINT[])
  AND label_name_map IS NOT NULL;

-- name: ListLogStreams :many
-- Returns distinct stream values with their source field for an organization within a time range.
-- Used by /api/v1/logs/series endpoint (Loki-compatible).
-- Returns both the field name (resource_customer_domain, resource_service_name, or stream_id for legacy)
-- and the distinct values for that field.
SELECT DISTINCT
    stream_id_field AS field_name,
    unnest(stream_ids)::text AS stream_value
FROM log_seg
WHERE organization_id = @organization_id
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND ts_range && int8range(@start_ts, @end_ts, '[)')
  AND published = true
  AND stream_ids IS NOT NULL
  AND stream_id_field IS NOT NULL;

-- name: ListLogSegsForRecompact :many
-- Returns log segments that need recompaction based on filter criteria.
-- Used by lakectl logs recompact command to queue segments for reprocessing.
-- Segments are returned in reverse timestamp order (newest first) so that
-- recompaction benefits the most recent data first.
SELECT *
FROM log_seg
WHERE organization_id = @organization_id
  AND published = true
  AND compacted = true
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND (
    (@filter_agg_fields_null = true AND agg_fields IS NULL)
    OR (@filter_sort_version = true AND sort_version < @min_sort_version)
  )
ORDER BY upper(ts_range) DESC;
