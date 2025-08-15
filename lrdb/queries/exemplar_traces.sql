-- name: BatchUpsertExemplarTraces :batchone
INSERT INTO exemplar_traces
( organization_id
, service_identifier_id
, fingerprint
, attributes
, exemplar
, span_name
, span_kind
)
VALUES      ( @organization_id
            , @service_identifier_id
            , @fingerprint
            , @attributes
            , @exemplar
            , @span_name
            , @span_kind
            )
    ON CONFLICT ( organization_id
            , service_identifier_id
            , fingerprint
            )
DO UPDATE SET
           attributes        = EXCLUDED.attributes,
           exemplar          = EXCLUDED.exemplar,
           span_name         = EXCLUDED.span_name,
           span_kind         = EXCLUDED.span_kind,
           updated_at        = now()
RETURNING (created_at = updated_at) AS is_new;

-- name: GetExemplarTracesCreatedAfter :many
SELECT * FROM exemplar_traces WHERE created_at > @ts;

-- name: GetExemplarTracesByService :many
SELECT * FROM exemplar_traces 
WHERE organization_id = @organization_id 
  AND service_identifier_id = @service_identifier_id
ORDER BY created_at DESC;

-- name: GetSpanInfoByFingerprint :one
SELECT exemplar, span_name, span_kind
FROM exemplar_traces
WHERE organization_id = @organization_id AND fingerprint = @fingerprint
LIMIT 1;

-- name: GetExemplarTracesByFingerprint :one
SELECT * FROM exemplar_traces 
WHERE organization_id = @organization_id 
  AND fingerprint = @fingerprint
LIMIT 1; 