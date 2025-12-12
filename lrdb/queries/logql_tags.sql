-- name: ListLogQLTags :many
-- Extract tag keys from label_name_map in log_seg table
-- Returns all keys from label_name_map (for v2 APIs)
-- Handler code can filter by non-empty values for v1 legacy API support
-- Includes today's and yesterday's dateint for partition pruning
SELECT DISTINCT key::text AS tag_key
FROM log_seg,
     LATERAL jsonb_object_keys(label_name_map) AS key
WHERE organization_id = @organization_id
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND label_name_map IS NOT NULL
ORDER BY tag_key;

-- name: GetLogStreamIdValues :many
-- Fast path for getting tag values when the requested tag matches stream_id_field.
-- Returns distinct stream_ids values directly from segment metadata, avoiding
-- expensive parquet file fetches. Returns empty if no segments have this tag
-- as their stream_id_field (caller should fall back to query workers).
-- Uses ts_range for accurate time filtering within the dateint range.
SELECT DISTINCT unnest(stream_ids)::text AS tag_value
FROM log_seg
WHERE organization_id = @organization_id
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND ts_range && int8range(@start_ts, @end_ts, '[)')
  AND published = true
  AND stream_id_field = @tag_name
  AND stream_ids IS NOT NULL
ORDER BY tag_value;
