# Metrics Query/Worker Architecture (Succinct)

This describes the `EvaluateMetricsQuery` path and the `query-worker` behavior behind it.

```text
Client
  |
  | POST /api/v1/metrics/query
  v
+---------------------------- query-api (:8080) -----------------------------+
| parse + compile PromQL -> QueryPlan                                        |
|                                                                             |
| EvaluateMetricsQuery                                                        |
|   1) Segment lookup (lrdb) per leaf, with range/offset-adjusted windows    |
|   2) Build time groups: ComputeReplayBatchesWithWorkers(...)               |
|   3) Launch groups concurrently (bounded by max(3, worker_count))          |
|      - split group segments by leaf                                         |
|      - map segments -> workers (consistent hash on org+segment)             |
|      - pushDown to workers (/api/v1/pushDown)                               |
|      - MergeSorted(worker streams) per group                                |
|      - register group channel with index                                    |
|                                                                             |
|   runOrderedCoordinator: concat groups strictly by index (0..N)             |
|   EvalFlow: bucketed sketch aggregation + root expression eval              |
+----------------------------------+------------------------------------------+
                                   |
                                   | SSE result/done
                                   v
                                Client
```

```text
Per worker (/api/v1/pushDown, :8081):
  request -> choose SQL template -> group segments by org/instance
  -> resolve storage profile -> download/cache parquet files (tbl_/agg_)
  -> DuckDB read_parquet(local files, union_by_name=true)
  -> map rows to SketchInput/Exemplar/TagValue -> SSE stream back
```

## Key invariants
- Worker discovery is env-backed (`local`, `kubernetes`, `ecs`) and maintains a live worker snapshot.
- Segment assignment is stable via consistent hashing (`orgID + segmentID` -> worker).
- Within each group, streams are timestamp-merged (`MergeSorted`).
- Across groups, output is concatenated by group index (not globally merged again), so correctness relies on non-overlapping time groups.
- Group parallelism is bounded; pushdown context is canceled only after `EvalFlow` completes.
