-- 1755041031_exemplar-metrics.up.sql

CREATE TABLE IF NOT EXISTS service_identifiers (
    id UUID NOT NULL DEFAULT uuid_generate_v4(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    organization_id UUID,
    service_name TEXT,
    cluster_name TEXT,
    namespace TEXT,
    PRIMARY KEY (id)
);

CREATE UNIQUE INDEX IF NOT EXISTS service_identifiers_upsertidx
    ON service_identifiers
    USING btree (organization_id, service_name, cluster_name, namespace);

CREATE TABLE IF NOT EXISTS exemplar_metrics (
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    organization_id UUID NOT NULL,
    service_identifier_id UUID NOT NULL REFERENCES service_identifiers(id) ON UPDATE CASCADE ON DELETE CASCADE,
    attributes JSONB NOT NULL DEFAULT '{}',
    exemplar JSONB NOT NULL,
    metric_name TEXT NOT NULL,
    metric_type TEXT NOT NULL,
    PRIMARY KEY (organization_id, service_identifier_id, metric_name, metric_type)
) PARTITION BY LIST (organization_id);

CREATE INDEX IF NOT EXISTS idx_exemplar_metrics_organization_id 
    ON exemplar_metrics (organization_id);
CREATE INDEX IF NOT EXISTS idx_exemplar_metrics_service_identifier_id 
    ON exemplar_metrics (service_identifier_id);
CREATE INDEX IF NOT EXISTS idx_exemplar_metrics_created_at 
    ON exemplar_metrics (created_at);

CREATE TABLE IF NOT EXISTS exemplar_logs (
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    organization_id UUID NOT NULL,
    service_identifier_id UUID NOT NULL REFERENCES service_identifiers(id) ON UPDATE CASCADE ON DELETE CASCADE,
    attributes JSONB NOT NULL DEFAULT '{}',
    exemplar JSONB NOT NULL,
    fingerprint BIGINT NOT NULL,
    related_fingerprints BIGINT[],
    PRIMARY KEY (organization_id, service_identifier_id, fingerprint)
);

CREATE TABLE IF NOT EXISTS exemplar_traces (
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    organization_id UUID NOT NULL,
    service_identifier_id UUID NOT NULL REFERENCES service_identifiers(id) ON UPDATE CASCADE ON DELETE CASCADE,
    attributes JSONB NOT NULL DEFAULT '{}',
    exemplar JSONB NOT NULL,
    fingerprint BIGINT NOT NULL,
    span_name TEXT NOT NULL DEFAULT '',
    span_kind INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (organization_id, service_identifier_id, fingerprint)
);
