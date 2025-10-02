# Legacy Query API Support

## Overview

This document describes the plan to support the old Scala-based query API (`/api/v1/graph`) in the new Go-based query engine. The legacy API uses a different AST format and, critically, uses **dotted label names** (e.g., `resource.bucket.name`) while the new LogQL-based system uses **underscored names** (e.g., `resource_bucket_name`).

**Important**: This implementation focuses on **logs only**. Existing customers only use logs via the legacy API, not metrics or traces.

## The Challenge: Bidirectional Label Translation

### Forward Translation (Query: dots → underscores)

**Easy**: String replacement during AST translation

- Input: `resource.bucket.name="mybucket"`
- Output: `resource_bucket_name="mybucket"` in LogQL

### Reverse Translation (Response: underscores → dots) ⚠️ **CRITICAL**

**Hard**: Query results return ALL labels from data, not just those in the filter.

**Problem Example:**

```json
{
  "resource_bucket_name": "mybucket",
  "resource_file": "access_log.parquet",
  "resource_file_name": "access_log.parquet",
  "resource_file_type": "accesslog",
  "_cardinalhq_fingerprint": "-8813296358087405541",
  "_cardinalhq_message": "POST /v1/api HTTP/1.1 200 2956"
}
```

**Which underscores should become dots?**

- `resource_file_name` → `resource.file.name` ✓
- `resource_file_type` → `resource.file.type` ✓
- `_cardinalhq_fingerprint` → `_cardinalhq.fingerprint` ✓
- `access_log` → `access.log` ✗ (NO! This is a value, not a label)

We cannot scan petabytes of data to build a comprehensive catalog. We need a **per-segment mapping table**.

## Solution: Per-Segment Label Mapping

### Database Schema Enhancement

Add a column to the log segment metadata table to store the original label names for each segment:

```sql
-- For log segments only (logs are the only dataset used by legacy customers)
ALTER TABLE log_seg ADD COLUMN label_name_map JSONB;

-- Example data:
{
  "resource_bucket_name": "resource.bucket.name",
  "resource_file": "resource.file",
  "resource_file_name": "resource.file.name",
  "resource_file_type": "resource.file.type",
  "resource_protocol": "resource.protocol",
  "_cardinalhq_fingerprint": "_cardinalhq.fingerprint",
  "_cardinalhq_timestamp": "_cardinalhq.timestamp",
  "_cardinalhq_message": "_cardinalhq.message"
}
```

### How It Works

1. **During Ingestion** (already stores underscored names):
   - Extract all unique label names from the incoming data
   - Build mapping: `underscored → dotted` for this segment
   - Store in `label_name_map` JSONB column

2. **During Query Execution**:
   - Translate query filters: dots → underscores
   - Identify segments to query (existing logic)
   - **Load `label_name_map` for each segment**
   - Execute LogQL query (returns underscored labels)

3. **During Response Transformation**:
   - For each result row, look up the segment's `label_name_map`
   - Transform ALL labels: underscored → dotted using the map
   - Return legacy SSE format with dotted labels

### Migration Strategy for Maping Data

- New segments get `label_name_map` populated during ingestion
- Existing segments: populate on first query
  - Query returns underscored labels
  - Build map from query results (heuristic-based)
  - Cache in segment metadata for future queries

## Implementation Plan

### Phase 1: Database Schema & Ingestion

#### 1.1 Migration (`lrdb/migrations/`)

```sql
-- File: NNNN_add_label_name_map.up.sql
ALTER TABLE log_seg ADD COLUMN label_name_map JSONB;
CREATE INDEX idx_log_seg_label_map ON log_seg USING gin(label_name_map);

-- File: NNNN_add_label_name_map.down.sql
ALTER TABLE log_seg DROP COLUMN label_name_map;
```

#### 1.2 Ingestion Enhancement

Modify ingestion pipeline to populate `label_name_map`:

**File: `cmd/ingest_logs.go` or relevant processor**

```go
func extractLabelNameMap(parquetFile string) (map[string]string, error) {
    // Read Parquet schema
    // For each column name:
    //   - If it's a tag/label column
    //   - Store: underscored → dotted mapping

    // Example:
    mapping := make(map[string]string)
    mapping["resource_bucket_name"] = "resource.bucket.name"
    mapping["_cardinalhq_fingerprint"] = "_cardinalhq.fingerprint"
    // ...
    return mapping, nil
}

// When creating segment record:
labelNameMap, err := extractLabelNameMap(parquetFilePath)
if err != nil {
    slog.Warn("failed to extract label name map", "error", err)
}

// Store in database (update SQL query to include label_name_map)
```

### Phase 2: Query Translation

#### 2.1 Legacy AST Types (`queryapi/legacy_ast.go`)

```go
type GraphRequest struct {
    BaseExpressions map[string]BaseExpression `json:"baseExpressions"`
    Formulae        []string                  `json:"formulae,omitempty"`
}

type BaseExpression struct {
    Dataset       string      `json:"dataset"` // Must be "logs"
    Limit         int         `json:"limit"`
    Order         string      `json:"order"` // "DESC" or "ASC"
    ReturnResults bool        `json:"returnResults"`
    Filter        QueryClause `json:"filter"`
}

type QueryClause interface {
    isQueryClause()
}

type Filter struct {
    K         string   `json:"k"`        // Label key with dots
    V         []string `json:"v"`        // Values
    Op        string   `json:"op"`       // eq, in, contains, gt, gte, lt, lte, regex
    DataType  string   `json:"dataType"` // string, number, etc.
    Extracted bool     `json:"extracted"`
    Computed  bool     `json:"computed"`
}

type BinaryClause struct {
    Q1 QueryClause `json:"q1"`
    Q2 QueryClause `json:"q2"`
    Op string      `json:"op"` // "and" or "or"
}
```

#### 2.2 AST to LogQL Translator (`queryapi/legacy_translator.go`)

```go
type TranslationContext struct {
    // Track labels seen during translation for debugging
    QueryLabels map[string]string // underscored → dotted
}

func TranslateToLogQL(baseExpr BaseExpression) (string, *TranslationContext, error) {
    // Validate dataset
    if baseExpr.Dataset != "logs" {
        return "", nil, fmt.Errorf("only 'logs' dataset is supported, got: %s", baseExpr.Dataset)
    }

    ctx := &TranslationContext{QueryLabels: make(map[string]string)}

    matchers, pipeline, err := filterToLogQL(baseExpr.Filter, ctx)
    if err != nil {
        return "", nil, err
    }

    logql := "{" + strings.Join(matchers, ",") + "}"
    if len(pipeline) > 0 {
        logql += " " + strings.Join(pipeline, " ")
    }

    return logql, ctx, nil
}

func filterToLogQL(clause QueryClause, ctx *TranslationContext) ([]string, []string, error) {
    matchers := []string{}
    pipeline := []string{}

    switch c := clause.(type) {
    case Filter:
        normalized := normalizeLabelName(c.K) // dots → underscores
        ctx.QueryLabels[normalized] = c.K     // Remember original

        switch c.Op {
        case "eq":
            if len(c.V) == 0 {
                return nil, nil, fmt.Errorf("eq operator requires at least one value")
            }
            matchers = append(matchers, fmt.Sprintf(`%s="%s"`, normalized, c.V[0]))

        case "in":
            // LogQL: label=~"val1|val2|val3"
            if len(c.V) == 0 {
                return nil, nil, fmt.Errorf("in operator requires at least one value")
            }
            pattern := strings.Join(c.V, "|")
            matchers = append(matchers, fmt.Sprintf(`%s=~"%s"`, normalized, pattern))

        case "contains":
            // LogQL line filter: |~ "pattern"
            if len(c.V) == 0 {
                return nil, nil, fmt.Errorf("contains operator requires a value")
            }
            pipeline = append(pipeline, fmt.Sprintf(`|~ "%s"`, c.V[0]))

        case "regex":
            if len(c.V) == 0 {
                return nil, nil, fmt.Errorf("regex operator requires a pattern")
            }
            matchers = append(matchers, fmt.Sprintf(`%s=~"%s"`, normalized, c.V[0]))

        case "gt", "gte", "lt", "lte":
            // These would need label_format or parser stage
            return nil, nil, fmt.Errorf("comparison operators not yet supported")

        default:
            return nil, nil, fmt.Errorf("unsupported operator: %s", c.Op)
        }

    case BinaryClause:
        m1, p1, err := filterToLogQL(c.Q1, ctx)
        if err != nil {
            return nil, nil, err
        }
        m2, p2, err := filterToLogQL(c.Q2, ctx)
        if err != nil {
            return nil, nil, err
        }

        // Combine matchers and pipelines
        matchers = append(matchers, m1...)
        matchers = append(matchers, m2...)
        pipeline = append(pipeline, p1...)
        pipeline = append(pipeline, p2...)

        // Note: LogQL doesn't support boolean OR in matchers
        // May need to run multiple queries for OR logic
    }

    return matchers, pipeline, nil
}

func normalizeLabelName(dotted string) string {
    return strings.ReplaceAll(dotted, ".", "_")
}
```

### Phase 3: Response Transformation

#### 3.1 Segment Label Map Loader (`queryapi/segment_labels.go`)

```go
// Load label name mappings for a set of segments
func (q *QuerierService) loadSegmentLabelMaps(
    ctx context.Context,
    segmentIDs []int64,
) (map[int64]map[string]string, error) {

    // Query database for label_name_map for each segment
    rows, err := q.mdb.Query(ctx, `
        SELECT segment_id, label_name_map
        FROM log_seg
        WHERE segment_id = ANY($1)
        AND label_name_map IS NOT NULL
    `, segmentIDs)

    if err != nil {
        return nil, err
    }
    defer rows.Close()

    result := make(map[int64]map[string]string)
    for rows.Next() {
        var segmentID int64
        var mapJSON []byte
        if err := rows.Scan(&segmentID, &mapJSON); err != nil {
            return nil, err
        }

        var mapping map[string]string
        if err := json.Unmarshal(mapJSON, &mapping); err != nil {
            slog.Warn("failed to unmarshal label_name_map", "segmentID", segmentID)
            continue
        }

        result[segmentID] = mapping
    }

    return result, nil
}
```

#### 3.2 Label Denormalizer (`queryapi/label_denormalizer.go`)

```go
type LabelDenormalizer struct {
    segmentMaps map[int64]map[string]string
    fallback    map[string]string // Known common labels
}

func NewLabelDenormalizer(segmentMaps map[int64]map[string]string) *LabelDenormalizer {
    return &LabelDenormalizer{
        segmentMaps: segmentMaps,
        fallback:    getCommonLabels(), // Hardcoded known labels
    }
}

// Denormalize a single label name
func (ld *LabelDenormalizer) Denormalize(segmentID int64, underscored string) string {
    // Try segment-specific map first
    if segMap, ok := ld.segmentMaps[segmentID]; ok {
        if dotted, ok := segMap[underscored]; ok {
            return dotted
        }
    }

    // Try fallback for common labels
    if dotted, ok := ld.fallback[underscored]; ok {
        return dotted
    }

    // Last resort: heuristic conversion
    return heuristicDenormalize(underscored)
}

// Denormalize all labels in a tag map
func (ld *LabelDenormalizer) DenormalizeMap(segmentID int64, tags map[string]any) map[string]any {
    result := make(map[string]any, len(tags))
    for k, v := range tags {
        dottedKey := ld.Denormalize(segmentID, k)
        result[dottedKey] = v
    }
    return result
}

func heuristicDenormalize(underscored string) string {
    // Known prefixes that should have dots
    prefixes := []string{"_cardinalhq_", "resource_", "log_"}

    for _, prefix := range prefixes {
        if strings.HasPrefix(underscored, prefix) {
            return strings.ReplaceAll(underscored, "_", ".")
        }
    }

    // Unknown pattern: keep as-is (safer than guessing)
    return underscored
}

func getCommonLabels() map[string]string {
    // Hardcoded known labels as fallback
    return map[string]string{
        "_cardinalhq_fingerprint": "_cardinalhq.fingerprint",
        "_cardinalhq_timestamp":   "_cardinalhq.timestamp",
        "_cardinalhq_message":     "_cardinalhq.message",
        "_cardinalhq_level":       "_cardinalhq.level",
        "_cardinalhq_name":        "_cardinalhq.name",
        // Add more common ones...
    }
}
```

#### 3.3 Legacy Response Format (`queryapi/legacy_response.go`)

```go
type LegacyEvent struct {
    ID      string        `json:"id"`
    Type    string        `json:"type"`
    Message LegacyMessage `json:"message"`
}

type LegacyMessage struct {
    Timestamp int64          `json:"timestamp"`
    Value     float64        `json:"value"`
    Tags      map[string]any `json:"tags"`
}

func ToLegacySSEEvent(
    queryID string,
    segmentID int64,
    ts promql.Timestamped,
    denormalizer *LabelDenormalizer,
) LegacyEvent {

    dottedTags := denormalizer.DenormalizeMap(segmentID, ts.Tags)

    return LegacyEvent{
        ID:   queryID,
        Type: "event",
        Message: LegacyMessage{
            Timestamp: ts.Timestamp,
            Value:     ts.Value,
            Tags:      dottedTags,
        },
    }
}
```

### Phase 4: Query Endpoint

#### 4.1 Graph Query Handler (`queryapi/querier.go`)

```go
func (q *QuerierService) handleGraphQuery(w http.ResponseWriter, r *http.Request) {
    // Parse request
    var req GraphRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        writeAPIError(w, http.StatusBadRequest, InvalidJSON, "invalid JSON: "+err.Error())
        return
    }

    // Get org from context
    orgID, ok := GetOrgIDFromContext(r.Context())
    if !ok {
        writeAPIError(w, http.StatusUnauthorized, ErrUnauthorized, "missing organization")
        return
    }

    // Parse time range
    s := r.URL.Query().Get("s")
    e := r.URL.Query().Get("e")
    startTs, endTs, err := dateutils.ToStartEnd(s, e)
    if err != nil {
        writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid time range: "+err.Error())
        return
    }

    // Setup SSE
    writeSSE, ok := q.sseWriter(w)
    if !ok {
        return
    }

    // Process each base expression
    for exprID, baseExpr := range req.BaseExpressions {
        if !baseExpr.ReturnResults {
            continue // Skip non-returning expressions
        }

        // Translate to LogQL
        logqlQuery, translationCtx, err := TranslateToLogQL(baseExpr)
        if err != nil {
            writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "translation failed: "+err.Error())
            return
        }

        // Parse and compile LogQL
        logAst, err := logql.FromLogQL(logqlQuery)
        if err != nil {
            writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid LogQL: "+err.Error())
            return
        }

        lplan, err := logql.CompileLog(logAst)
        if err != nil {
            writeAPIError(w, http.StatusUnprocessableEntity, ErrCompileError, "compilation error: "+err.Error())
            return
        }

        // Execute query
        reverse := baseExpr.Order == "DESC"
        limit := baseExpr.Limit
        if limit == 0 {
            limit = 1000 // Default
        }

        resultsCh, err := q.EvaluateLogsQuery(
            r.Context(),
            orgID,
            startTs,
            endTs,
            reverse,
            limit,
            lplan,
            nil, // fields
        )
        if err != nil {
            writeAPIError(w, http.StatusInternalServerError, ErrInternalError, "query execution failed: "+err.Error())
            return
        }

        // Load segment label maps
        // NOTE: Need to track which segments are being queried
        // This requires modification to EvaluateLogsQuery to return segment info
        // For now, use heuristic fallback
        denormalizer := NewLabelDenormalizer(nil)

        // Stream results
        for ts := range resultsCh {
            segmentID := int64(0) // TODO: Get actual segment ID from result
            event := ToLegacySSEEvent(exprID, segmentID, ts, denormalizer)

            if err := writeSSE("event", event); err != nil {
                slog.Error("failed to write SSE event", "error", err)
                return
            }
        }
    }

    // Send done event
    _ = writeSSE("done", map[string]string{"status": "ok"})
}

// Register endpoint
func (q *QuerierService) Run(doneCtx context.Context) error {
    // ... existing setup ...

    mux.HandleFunc("/api/v1/graph", q.apiKeyMiddleware(q.handleGraphQuery))

    // ... rest of setup ...
}
```

## Testing Strategy

### Unit Tests

1. **Label Translation Tests** (`queryapi/legacy_translator_test.go`)
   - Test all operator types: eq, in, contains, regex, etc.
   - Test nested boolean logic: and, or combinations
   - Test edge cases: empty values, special characters

2. **Label Denormalization Tests** (`queryapi/label_denormalizer_test.go`)
   - Test segment-specific mappings
   - Test fallback to common labels
   - Test heuristic conversion
   - Test round-trip: dotted → underscored → dotted

3. **Response Format Tests** (`queryapi/legacy_response_test.go`)
   - Verify SSE format matches Scala implementation
   - Test multiple query IDs
   - Test done event

### Integration Tests

1. **End-to-End Query Tests**
   - Use real queries from Scala system
   - Compare results between old and new implementations
   - Verify exact label name matching

2. **Segment Mapping Tests**
   - Test with segments that have `label_name_map`
   - Test with segments without mapping (fallback behavior)

### Performance Tests

1. **Label Map Loading**
   - Benchmark loading maps for 1000+ segments
   - Test caching strategies

2. **Query Translation**
   - Benchmark complex nested filters
   - Profile memory allocations

## Migration & Rollout

### Phase 1: Development & Testing (Week 1-2)

- Implement database schema changes
- Update ingestion to populate `label_name_map`
- Implement translation and denormalization logic
- Write comprehensive tests

### Phase 2: Staging Deployment (Week 3)

- Deploy to staging environment
- Let new ingestion populate mappings for new data
- Test with real queries from production
- Monitor for unknown labels (should be rare with fallbacks)

### Phase 3: Backfill Old Data (Week 4)

- Run background job to populate `label_name_map` for existing segments
- Sample approach: read first 100 rows from each Parquet file
- Update database in batches (10,000 segments at a time)

### Phase 4: Production Rollout (Week 5)

- Deploy to production with feature flag
- Enable for beta customers first
- Monitor error rates and label mapping accuracy
- Gradually enable for all customers

### Phase 5: Deprecation (Month 3+)

- Announce deprecation of Scala-based API
- Provide migration guide for remaining customers
- Set sunset date
- Eventually remove old infrastructure

## Open Questions & Future Enhancements

1. **Segment ID in Results**
   - Current: Results don't include segment ID
   - Need: Modify `promql.Timestamped` to include segment metadata
   - Or: Enhance `EvaluateLogsQuery` to return segment info alongside results

2. **Caching Label Maps**
   - Per-request cache of segment maps
   - Global cache with TTL
   - Memory vs. query latency tradeoff

3. **Formulae Support**
   - Scala API supports formulae (e.g., `a + b`, `a / b`)
   - Not covered in initial implementation
   - May need separate translation logic

4. **Advanced Operators**
   - Comparison operators (gt, gte, lt, lte) on extracted fields
   - Would require LogQL parser stages
   - Defer to later phase

5. **Boolean OR in Matchers**
   - LogQL doesn't support OR in stream selectors
   - May need to run multiple queries and merge results
   - Performance implications

## Files to Create/Modify

### New Files

1. `docs/LEGACY-QUERY-API.md` - This document
2. `queryapi/legacy_ast.go` - Type definitions
3. `queryapi/legacy_translator.go` - AST → LogQL translation
4. `queryapi/legacy_response.go` - Legacy SSE response format
5. `queryapi/label_denormalizer.go` - Label normalization/denormalization
6. `queryapi/segment_labels.go` - Segment label map loading
7. `queryapi/legacy_test.go` - Comprehensive tests
8. `lrdb/migrations/NNNN_add_label_name_map.up.sql` - Schema migration

### Modified Files

1. `queryapi/querier.go` - Add `/api/v1/graph` endpoint
2. `cmd/ingest_logs.go` - Populate `label_name_map` during ingestion
3. `queryapi/querier_service.go` - May need to enhance result types

## Success Criteria

1. **Functional Correctness**
   - All legacy log queries execute successfully
   - Results match Scala implementation (same labels, values, timestamps)
   - All label names correctly converted to dotted format

2. **Reliability**
   - Zero data loss during migration
   - Graceful degradation for missing mappings
   - Comprehensive error handling and logging

3. **Maintainability**
   - Well-tested with > 80% coverage
   - Clear documentation
   - Easy to extend for new operators
