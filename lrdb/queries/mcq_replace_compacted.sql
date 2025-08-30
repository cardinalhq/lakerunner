-- name: ReplaceCompactedMetricSegs :exec
-- Replace metric segments after compaction: mark old as compacted=true, published=false
-- and insert new segments as compacted=false, published=true
WITH
old_segments_update AS (
  UPDATE metric_seg 
  SET compacted = true, published = false
  WHERE organization_id = @organization_id
    AND dateint = @dateint  
    AND frequency_ms = @frequency_ms
    AND instance_num = @instance_num
    AND segment_id = ANY(@old_segment_ids::bigint[])
    AND compacted = false  -- Safety: only update non-compacted segments
  RETURNING segment_id
)
INSERT INTO metric_seg (
  organization_id, dateint, frequency_ms, segment_id, instance_num,
  ts_range, record_count, file_size, ingest_dateint, 
  compacted, published, created_by, slot_id, fingerprints, sort_version, slot_count
)
SELECT 
  @organization_id::uuid,
  @dateint::int,
  @frequency_ms::int,
  unnest(@new_segment_ids::bigint[]),
  @instance_num::smallint,
  unnest(@new_ts_ranges::int8range[]),
  unnest(@new_record_counts::bigint[]),
  unnest(@new_file_sizes::bigint[]),
  @ingest_dateint::int,
  false,  -- compacted=false
  true,   -- published=true  
  @created_by,
  @slot_id::int,
  unnest(@new_fingerprints::bigint[][]),
  @sort_version::smallint,
  @slot_count::int;