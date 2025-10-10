-- name: ListSpanTags :many
-- Extract tag keys from label_name_map in trace_seg table
-- Returns all keys from label_name_map (for v2 APIs)
-- Handler code can filter by non-empty values for v1 legacy API support
-- Includes today's and yesterday's dateint for partition pruning
SELECT DISTINCT key::text AS tag_key
FROM trace_seg,
     LATERAL jsonb_object_keys(label_name_map) AS key
WHERE organization_id = @organization_id
  AND dateint >= @start_dateint
  AND dateint <= @end_dateint
  AND label_name_map IS NOT NULL
ORDER BY tag_key;
