# SQL Query Analysis Report

## Unused SQL Query Functions (11 total)

These functions are generated but not referenced anywhere in the codebase:

1. **GetExemplarTracesByFingerprint** (`lrdb/exemplar_traces.sql.go:27`)
2. **GetExemplarTracesByService** (`lrdb/exemplar_traces.sql.go:56`)
3. **GetExemplarTracesCreatedAfter** (`lrdb/exemplar_traces.sql.go:90`)
4. **GetSpanInfoByFingerprint** (`lrdb/exemplar_traces.sql.go:138`)
5. **GetExemplarMetricsByService** (`lrdb/exemplar_metrics.sql.go`)
6. **GetExemplarMetricsCreatedAfter** (`lrdb/exemplar_metrics.sql.go`)
7. **GetExemplarLogsByFingerprint** (`lrdb/exemplar_logs.sql.go:27`)
8. **GetExemplarLogsByService** (`lrdb/exemplar_logs.sql.go:55`)
9. **GetExemplarLogsCreatedAfter** (`lrdb/exemplar_logs.sql.go:88`)
10. **SignalLockCleanup** (`lrdb/signal_locks.sql.go:16`)
11. **TouchInqueueWork** (`lrdb/inqueue_touch.sql.go:28`)

---

## SQL Query Test Coverage

### Thoroughly Tested Functions (Direct unit tests)

#### Metric Compaction Queue (MCQ) Operations

Tests were removed when functions were migrated to new MCQ bundle-based approach

- **McqQueueWork**: Queues metric compaction work items
- **ClaimCompactionBundle**: Claims bundles of work items for processing with dynamic estimation
- **McqHeartbeat**: Updates heartbeat timestamps for claimed items
- **McqCompleteDelete**: Removes completed work items
- **McqDeferKey**: Defers work items with backoff
- **McqCleanupExpired**: Cleans up expired work items

#### Inqueue Operations

*Tested in `lrdb/queries/inqueue_claim_batch_test.go:241` and `inqueue_claim_batch_enhanced_test.go:416`*

- **ClaimInqueueWorkBatch**: 9 test scenarios, including:
  - Basic batch claiming with organization grouping
  - Signal type isolation
  - Empty queue handling
  - Enhanced batch logic (oversized files, age thresholds, size limits, batch count limits, priority ordering)
- **PutInqueueWork**: Tested indirectly through all claim batch tests (22+ test cases)

#### Metric Segment Compaction

*Tested in `lrdb/queries/compact_metric_segs_test.go:455`*

- **CompactMetricSegs** (calls `ReplaceMetricSegs`): 5 test scenarios (basic compaction, empty old/new records, multiple new segments, idempotent replays)
- **GetMetricSegsForCompaction**: Tested indirectly through compaction tests
- **InsertMetricSegment**: Tested indirectly through compaction setup

---

### Integration Tested Functions

Several other SQL functions are covered by integration tests in:

- `internal/workqueue/queue_integration_test.go` — Tests work queue operations
- `internal/workqueue/deadlock_torture_test.go` — Tests work queue under stress
- `internal/metricsprocessing/compaction/coordinate_test.go` — Tests metric compaction coordination

---

## Untested Functions (No dedicated tests found)

### High-Risk Untested Operations

- **DeleteInqueueWork** — Used in production but no direct tests
- **McqCompleteDelete** — Used in compaction manager but no direct tests
- **MarkMetricSegsCompactedByKeys** — Critical for metric compaction workflow
- **WorkQueue functions** (Add, Complete, Fail, Cleanup, etc.) — Core work queue operations
- **Object cleanup functions** — Important for S3 cleanup workflow
- **Service identifier functions** — Used for exemplar processing
- **Estimator functions** — Used for query optimization
- **All ConfigDB functions** — No dedicated SQL-level tests found

### Medium-Risk Untested Operations

- **Log/Trace segment operations** — Similar to metric operations but untested
- **Inqueue operations** (Delete, Release, Touch, Summary, Cleanup) — Various workflow functions
- **Signal lock operations** — Distributed coordination functions

---

## Summary Statistics

- **Total SQL Functions:** ~135
- **Functions with Direct Unit Tests:** 12 (9%)
- **Functions with Integration Test Coverage:** ~25 (18%)
- **Completely Untested Functions:** ~98 (73%)
- **Unused Functions:** 11 (8%)

---

## Recommendations

1. **Prioritize testing high-risk untested operations** — especially work queue, object cleanup, and ConfigDB operations.
2. **Consider removing unused exemplar and utility functions** — they represent dead code.
3. **Add integration tests for ConfigDB operations** — critical for system configuration.
4. **Test error conditions and edge cases** — most current tests focus on happy path scenarios.
