-- name: ListLogQLTags :many
-- Extract tag keys from flat exemplar format
-- Only return keys that start with chq_, resource_, scope_, log_, or attr_
SELECT DISTINCT key::text AS tag_key
FROM lrdb_exemplar_logs,
     LATERAL jsonb_object_keys(exemplar) AS key
WHERE organization_id = @organization_id
  AND key ~ '^(chq_|resource_|scope_|log_|attr_)'
ORDER BY tag_key;
