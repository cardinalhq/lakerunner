-- name: BatchUpsertExemplarTraces :batchone
INSERT INTO lrdb_exemplar_traces
( organization_id
, service_identifier_id
, fingerprint
, exemplar
, span_name
, span_kind
, source
)
VALUES      ( @organization_id
            , @service_identifier_id
            , @fingerprint
            , @exemplar
            , @span_name
            , @span_kind
            , @source
            )
    ON CONFLICT ( organization_id
            , service_identifier_id
            , fingerprint
            , source
            )
DO UPDATE SET
           exemplar          = EXCLUDED.exemplar,
           span_name         = EXCLUDED.span_name,
           span_kind         = EXCLUDED.span_kind,
           updated_at        = now()
RETURNING (created_at = updated_at) AS is_new;

 