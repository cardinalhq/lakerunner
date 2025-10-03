-- name: ListSpanTags :many
-- Extract tag keys from flat exemplar format for spans
-- Only return keys that start with chq_, resource_, scope_, span_, or attr_
SELECT DISTINCT key::text AS tag_key
FROM lrdb_exemplar_traces,
    LATERAL jsonb_object_keys(exemplar) AS key
WHERE organization_id = @organization_id
  AND key ~ '^(chq_|resource_|scope_|span_|attr_)'
ORDER BY tag_key;
