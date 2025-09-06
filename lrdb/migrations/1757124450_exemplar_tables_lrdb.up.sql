-- 1757124450_exemplar_tables_lrdb.up.sql

CREATE TABLE IF NOT EXISTS lrdb_service_identifiers (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    organization_id UUID,
    service_name TEXT,
    cluster_name TEXT,
    namespace TEXT,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS lrdb_service_identifiers_upsertidx
    ON lrdb_service_identifiers
    USING btree (organization_id, service_name, cluster_name, namespace);

-- Create new lrdb_exemplar_metrics table
CREATE TABLE IF NOT EXISTS lrdb_exemplar_metrics (
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    organization_id UUID NOT NULL,
    service_identifier_id UUID NOT NULL REFERENCES lrdb_service_identifiers(id) ON UPDATE CASCADE ON DELETE CASCADE,
    attributes JSONB NOT NULL DEFAULT '{}',
    exemplar JSONB NOT NULL,
    metric_name TEXT NOT NULL,
    metric_type TEXT NOT NULL,
    PRIMARY KEY (organization_id, service_identifier_id, metric_name, metric_type)
);

CREATE INDEX IF NOT EXISTS idx_lrdb_exemplar_metrics_organization_id 
    ON lrdb_exemplar_metrics (organization_id);
CREATE INDEX IF NOT EXISTS idx_lrdb_exemplar_metrics_service_identifier_id 
    ON lrdb_exemplar_metrics (service_identifier_id);

CREATE TABLE IF NOT EXISTS lrdb_exemplar_logs (
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    organization_id UUID NOT NULL,
    service_identifier_id UUID NOT NULL REFERENCES lrdb_service_identifiers(id) ON UPDATE CASCADE ON DELETE CASCADE,
    attributes JSONB NOT NULL DEFAULT '{}',
    exemplar JSONB NOT NULL,
    fingerprint BIGINT NOT NULL,
    related_fingerprints BIGINT[],
    PRIMARY KEY (organization_id, service_identifier_id, fingerprint)
);

CREATE INDEX IF NOT EXISTS idx_lrdb_exemplar_logs_organization_id 
    ON lrdb_exemplar_logs (organization_id);
CREATE INDEX IF NOT EXISTS idx_lrdb_exemplar_logs_service_identifier_id 
    ON lrdb_exemplar_logs (service_identifier_id);
CREATE INDEX IF NOT EXISTS idx_lrdb_exemplar_logs_fingerprint 
    ON lrdb_exemplar_logs (fingerprint);

-- Create new lrdb_exemplar_traces table (without the extra test fields)
CREATE TABLE IF NOT EXISTS lrdb_exemplar_traces (
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    organization_id UUID NOT NULL,
    service_identifier_id UUID NOT NULL REFERENCES lrdb_service_identifiers(id) ON UPDATE CASCADE ON DELETE CASCADE,
    attributes JSONB NOT NULL DEFAULT '{}',
    exemplar JSONB NOT NULL,
    fingerprint BIGINT NOT NULL,
    span_name TEXT NOT NULL DEFAULT '',
    span_kind INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (organization_id, service_identifier_id, fingerprint)
);

-- add_to_bigint_list takes a list of BIGINTs, a new element, and a maximum length, and returns a new list.
-- If the new element is already in the list, the list is left unchanged.
CREATE OR REPLACE FUNCTION add_to_bigint_list(
    arr BIGINT[],
    new_elem BIGINT,
    max_len INTEGER
)
RETURNS BIGINT[] AS $$
DECLARE
    result BIGINT[];
BEGIN
    IF arr IS NULL THEN
        result := ARRAY[new_elem];
    ELSIF new_elem = ANY(arr) THEN
        result := arr;
    ELSE
        result := ARRAY[new_elem] || arr;
    END IF;

    IF array_length(result, 1) > max_len THEN
        result := result[1:max_len];
    END IF;

    RETURN result;
END;
$$ LANGUAGE plpgsql;