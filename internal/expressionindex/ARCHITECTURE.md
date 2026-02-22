# Expression Index Architecture

## Scope
- Maintain org-scoped expression metadata from catalog config.
- Use that metadata in ingest to emit derived metrics into the normal metrics pipeline.
- Use catalog rules in query-api to rewrite queries to materialized metric names when available.
- Keep queryworker on a single parquet path (`tbl_*.parquet`); no runtime `agg_` split path.

## Core Modules
- `ExpressionCache`: org-level container (`orgID -> QueryIndex`).
- `QueryIndex`: per-signal postings + matcher evaluation for fast may-match lookup.
- `CatalogRefresher`: loads `expression.catalog.v1` and atomically replaces org indexes.
- `Adapters`: normalize PromQL/LogQL leaves into canonical `Expression`.
- `configservice`: cached config access with org + default fallback.

## Config (`expression.catalog.v1`)
```json
{
  "metrics": [
    {
      "id": "m.http.prod",
      "metric": "http_requests_total",
      "materialized_metric": "http_requests_total_10s_rollup",
      "matchers": [
        { "label": "env", "op": "=", "value": "prod" }
      ]
    }
  ],
  "derived_metrics": [
    {
      "id": "logs.events",
      "source_signal": "logs",
      "metric_name": "log_events"
    }
  ],
  "promql": [{ "query": "sum(rate(http_requests_total[5m]))" }],
  "logql": [{ "signal": "logs", "query": "{service=\"payments\"}" }]
}
```

## Control-Plane Flow
```text
configservice(expression.catalog.v1)
             |
             v
      CatalogRefresher (per org)
             |
             v
      ExpressionCache (org -> QueryIndex)
             |
             +-----------------------------+
             |                             |
             v                             v
   ingest-time candidate checks      shared expression metadata
```

## Ingest Data Path (Logs -> Metrics)
```text
log rows
  -> 10s bucket aggregation (log_level + configured stream field)
  -> derived metric parquet files under metrics/
  -> insert metric_seg rows
  -> publish compaction + rollup notifications
  -> normal metrics pipeline (ingest/compact/rollup/query)
```

Notes:
- Derived metric names come from catalog `derived_metrics` (`source_signal` + `metric_name`), not hardcoded.
- `agg_*.parquet` generation path is removed for the active flow.

## Query Rewrite Path (Materialized Metrics)
Query-api now does catalog-based metric rewrite before compile/eval:

1. **PromQL endpoint**
   - Parse PromQL.
   - Rewrite selector metric names using catalog `metrics[].materialized_metric`.
   - Compile and evaluate.

2. **LogQL/Spans aggregate endpoints**
   - `LogQL -> RewriteToPromQL -> parse PromQL`.
   - Apply same materialized rewrite.
   - Compile, attach log leaves, evaluate.

Rewrite semantics:
- Rule match requires exact source metric match (`metrics[].metric`).
- If rule has `matchers`, all must be present in selector.
- If multiple rules match, most specific rule wins (largest matcher set).
- For synthetic log rewrite families (`__logql_*`) rewritten to real materialized metrics, `__leaf` matcher is dropped so execution stays on metric segments.

## Runtime Query Path
- Queryworker pushdown reads segment parquet (`tbl_*.parquet`) for logs/metrics/traces.
- No `agg_` file branch in worker request execution.

## Code-Level Reference Map
- Expression catalog schema and normalization:
  - `internal/configservice/expression_catalog.go`
  - `ExpressionCatalogEntry` (`materialized_metric`)
  - `GetExpressionCatalogConfig`
- Optional global config access used by query rewrite:
  - `internal/configservice/service.go`
  - `MaybeGlobal`

- Query index core:
  - `internal/expressionindex/query_index.go`
  - `QueryIndex`, `FindCandidates`
- Org-scoped cache wrapper:
  - `internal/expressionindex/expression_cache.go`
  - `ExpressionCache`, `ReplaceOrg`, `FindCandidates`
- Catalog to index refresh:
  - `internal/expressionindex/catalog_refresher.go`
  - `CatalogRefresher`, `MaybeRefreshOrg`, `BuildExpressionsFromCatalogConfig`
- PromQL/LogQL leaf normalization into canonical expressions:
  - `internal/expressionindex/adapters.go`
  - `ExpressionFromPromBaseExpr`, `ExpressionFromLogLeaf`

- Logs-to-metrics derived metric emission:
  - `internal/metricsprocessing/logs_ingest_processor.go`
  - `configuredDerivedMetricNames`
  - `processBundleWithDuckDB`
  - `insertAndPublishMetricSegments`
  - `internal/metricsprocessing/log_derived_metric_segments.go`
  - `createLogDerivedMetricParquet`
- Log bucket aggregation keys and stats collection:
  - `internal/parquetwriter/factories/logs.go`
  - `LogAggKey`, `LogsStatsProvider`

- Query-side materialized metric rewrite (leaf selectors only):
  - `queryapi/materialized_metric_rewrite.go`
  - `rewritePromExprWithMaterializedCatalog`
  - `rewritePromExprWithMaterializedRules`
  - `rewriteSelectorMetric`
- Query-api wiring points:
  - `queryapi/querier.go`
  - `handlePromQuery`
  - `handleLogQuery` (aggregate branch after `LogQL -> PromQL` rewrite)
  - `handleSpansQuery` (aggregate branch after `LogQL -> PromQL` rewrite)
