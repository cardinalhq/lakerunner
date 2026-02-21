# Query Execution + Autoscaling Spec

## Objective
Build a single-path interactive query architecture that preserves current PromQL correctness and cache affinity while achieving fast failure recovery without database queue latency.

## Durable Stream Definition
A durable stream is a persistent bidirectional control connection with heartbeat-based liveness detection, reconnect, and ordered message delivery.

## Query Invariants (Must Preserve)
1. Every expression is decomposed into `BaseExpr` leaves and pushed down to workers.
2. Query API performs global aggregation/evaluation.
3. Work must not be split into independent parallel time batches for range/sliding-window expressions.
4. `MergeSorted` merges N worker outputs in timestamp order.
5. `EvalFlow` sequences N `BaseExpr`/leaf streams so expressions like `A / B` evaluate at matching timestamps.
6. `ExecNode.eval()` remains the PromQL evaluation engine for final time-series computation.

## Non-Goals
- No query language changes.
- No compatibility adapter layer for old internals.
- Do not remove local parquet cache.
- Do not use `work_queue` or any DB-backed queue for interactive query execution.
- Do not introduce separate fast/slow query paths.

## Success Criteria
1. Most queries complete in sub-second timeframes under normal conditions.
2. Worker failure triggers reassignment without lease/polling delays.
3. Query cancellation and pod shutdown are propagated quickly through a single connection mechanism.
4. Query correctness remains identical to existing `MergeSorted` + `EvalFlow` + `ExecNode.eval()` behavior.
5. Cache hit rate remains high via affinity assignment, with acceptable temporary duplication during API convergence.
6. Client-facing transport from client to API remains existing SSE behavior.

## Target Architecture
1. Core Work Coordination Component (self-contained, shared library).
- Owns worker membership state and liveness model.
- Owns work state machine and transitions.
- Owns assignment/reassignment rules and idempotency rules.
- Exposes a narrow interface used by both API and worker managers.

2. API is planner, scheduler, coordinator, and in-memory query state owner.
- Parse query and compile leaves.
- Use API Work Manager (built on core component) to assign/reassign leaf work.
- Track in-flight query state in process memory only.
- Merge/evaluate and stream SSE.

3. Worker is execution + cache node.
- Maintain durable gRPC bidirectional control stream with each API instance.
- Use Worker Work Manager (built on core component) to accept/reject/cancel work.
- Execute work, materialize Parquet artifact, notify API immediately.
- Serve artifact over HTTP for immediate fetch.

4. API↔worker durable control connection is the single control plane.
- Used for heartbeat/liveness.
- Used for admission (`accepting_work`).
- Used for work assignment/ack/completion/failure notifications.
- Used for cancellation propagation.
- Connection loss is treated as immediate worker failure for scheduling.

5. Result data plane is separate from control plane.
- Control/work messages stay on durable control stream.
- Result payload transfer is HTTP artifact fetch only.

6. No DB query queue.
- Query dispatch/retry/reassignment state does not persist in DB.
- If API crashes, outstanding query work is aborted via control stream disconnect.

## Component Boundaries
1. `core/workcoord` (new major component).
- Pure coordination logic and protocol model.
- No query parser logic.
- No DuckDB logic.
- High unit-test coverage target.

2. `queryapi/workmanager`.
- Converts query plan/leaves to work units.
- Calls `core/workcoord` for assignment and lifecycle.
- Triggers artifact fetch and feeds merge/eval pipeline.

3. `queryworker/workmanager`.
- Handles assignment messages from control stream.
- Calls `core/workcoord` for lifecycle transitions.
- Orchestrates execution, artifact creation, and completion notifications.

## Worker Membership and Convergence
1. Each API maintains its own live worker view from durable streams.
2. API views are eventually consistent across API replicas.
3. Temporary divergence can cause duplicate cache fetches; correctness is unaffected.
4. Scheduling uses current local view plus affinity hashing.
5. Affinity algorithm is rendezvous hashing; worker set changes remap only a fraction of work.

## Worker Admission and Drain Semantics
1. Worker status includes:
- `alive`
- `accepting_work`
- `draining`

2. On pod shutdown signal:
- Worker switches to `draining=true`.
- Worker sets `accepting_work=false` immediately and rejects all new assignments.
- Worker continues processing only already accepted/in-flight work.
- Worker exits after in-flight work completes (or max drain timeout, if configured).

3. API scheduling rules:
- Prefer workers with `accepting_work=true` and `draining=false`.
- Treat `accepting_work=false` workers as out of rotation immediately.
- Reassign unstarted work from draining workers to healthy workers immediately.

## Work Assignment Model
1. API compiles query into `BaseExpr` leaves.
2. API resolves segment universe.
3. API assigns leaf segment groups to workers by affinity hash over its current worker view.
4. API sends work items over durable stream (`AssignWork`).
5. Worker acknowledges receipt (`WorkAccepted`) or rejects only when not accepting work (`WorkRejected`).
6. Worker executes and emits `WorkReady` immediately when artifact is complete.
7. API fetches artifact over HTTP immediately after `WorkReady`.

## Work Sizing
1. Initial work-unit sizing uses existing planner behavior (current segment grouping strategy).
2. Leaf work is not treated as globally atomic; planner can emit multiple work items per leaf using current planning logic.
3. Additional rebalancing heuristics can be added later without changing control/data plane contracts.

## Artifact Contract
1. Worker outputs Parquet artifacts only.
2. `WorkReady` includes:
- `work_id`
- `artifact_url`
- `artifact_size_bytes`
- `artifact_checksum`
- `row_count`
- `min_ts`
- `max_ts`
- `parquet_schema_version`

3. API verifies checksum on fetch.
4. API ingests artifact into merge/eval pipeline.
5. Worker deletes artifact on explicit API ACK or TTL expiry.

## Artifact Storage and Retention
1. Artifacts are worker-local only (same node local disk as worker process).
2. Artifacts do not survive worker death; unfinished/not-yet-fetched artifacts are re-executed on another worker.
3. Artifact TTL defaults:
- `artifact_ttl = 10m` after `WorkReady` if not ACKed.
- `artifact_ttl_after_ack = 30s` for cleanup lag tolerance.
4. Disk budget policy:
- Artifact spool and parquet download cache share the same disk budget manager.
- Reserve a minimum artifact-spool floor for in-flight query completion.
- Parquet cache yields first when free space is required to maintain artifact floor.
- Enforce high/low watermarks and evict in this order: parquet cache entries, ACKed artifacts, expired artifacts, then oldest unacked artifacts (with corresponding work retry).

## Failure and Recovery Semantics
1. Worker failure detection.
- Durable stream heartbeat interval: 1s.
- Failure timeout: 3 missed heartbeats (target ~3s detection).

2. On worker failure/stream disconnect.
- API marks worker unavailable immediately.
- API reassigns uncompleted work to remaining workers immediately.
- Not-yet-fetched work from the failed worker is re-executed on another worker.

3. API failure.
- Worker detects stream disconnect and cancels all work owned by that API session.
- No orphaned execution should continue after control stream loss.

4. Reconnect behavior.
- API initiates and maintains gRPC streams to workers (API dials worker endpoints).
- Worker discovery remains API-driven via existing environment-specific discovery integrations (Kubernetes, ECS; others can be added).
- On transient disconnect, API retries with exponential backoff + jitter.
- Reconnected worker resumes as same worker identity.
- In-flight work from disconnected session is treated as failed/canceled and may be reassigned.

5. Idempotency.
- API deduplicates by `(query_id, leaf_id, work_id, artifact_checksum)`.
- First accepted completion for a work item is committed; later duplicates are ignored.

## Metrics Pipeline Compatibility
1. Preserve current API-side semantics.
- Keep `runOrderedCoordinator`, `MergeSorted`, `EvalFlow`, and `ExecNode.eval()`.

2. Adapter requirement.
- Artifact reader converts fetched Parquet rows into the same timestamped channel contracts used today.

3. Ordering requirement.
- Artifact boundaries are transport boundaries only.
- API must reconstruct globally correct timestamp order per leaf before `EvalFlow`.

4. Client protocol.
- Client-to-API result delivery remains existing SSE endpoints and event format.

## Observability Requirements
1. Control stream metrics.
- Active streams per API.
- Heartbeat RTT.
- Disconnect count/rate.
- Reconnect latency.

2. Scheduling metrics.
- Work assignment latency.
- Work accepted/rejected counts.
- Reassignment count.
- Affinity hit ratio.
- API local work queue depth.
- API local oldest-work age.

3. Execution metrics.
- Worker queue depth (local).
- Task runtime.
- Download duration.
- Query duration.
- Cancel latency.

4. Artifact metrics.
- Ready-to-fetch latency.
- Fetch latency.
- Checksum mismatch rate.
- Artifact TTL cleanup counts.
- Artifact spool bytes used.
- Parquet cache bytes used.
- Total free bytes in shared cache disk.

5. End-user metrics.
- Query p50/p95/p99 latency.
- Time-to-first-result.
- Error rate by reason.

6. Trace/log correlation.
- Every query has `query_id`.
- Every work unit has `work_id`.
- `query_id` is present in all API and worker logs for that query.
- `work_id` is present on all work-specific logs, metrics, and spans.

7. Autoscaling export.
- API exposes queue-depth and oldest-age metrics from a monitor endpoint suitable for KEDA polling.

## Implementation Plan
1. Phase 1: extract and build core coordination component.
- Create `core/workcoord` with explicit work/worker state machine.
- Add exhaustive unit tests for transitions, assignment, reassignment, and idempotency.
- Define message schema for control stream (`AssignWork`, `Accepted`, `Rejected`, `Ready`, `Failed`, `Cancel`, heartbeat/status).
- Lock control transport decision: gRPC bidirectional stream.

2. Phase 2: reliability gates for new architecture.
- Enforce timeout-bounded shutdown and force-close fallback in API/worker.
- Add readiness/liveness checks required by new control stream flow.
- Add static/code-review gates to prevent context-unsafe blocking channel sends in new managers.

3. Phase 3: durable control stream with managers.
- Implement bidirectional API↔worker stream with heartbeat, assignment, completion, cancellation.
- Add worker admission states (`accepting_work`, `draining`).
- Add immediate reassignment on stream loss.
- Implement `queryapi/workmanager` and `queryworker/workmanager` on top of `core/workcoord`.
- Add reconnect loop with backoff/jitter and stable worker identity handling.

4. Phase 4: artifact completion flow.
- Worker writes Parquet artifact and emits `WorkReady`.
- API immediate fetch + checksum verify + ACK.
- Worker artifact GC on ACK/TTL.
- Implement shared disk-budget manager for artifact spool + parquet cache.

5. Phase 5: cutover and cleanup.
- Remove DB-queue query orchestration assumptions.
- Keep ingest on existing `work_queue`.
- Tighten SLO alerts for control stream and query latency.
- Wire KEDA-facing monitor metrics for API local queue depth/age.

## Test Plan
1. Unit tests.
- `core/workcoord` state machine transition tests (major focus).
- `core/workcoord` assignment/reassignment/idempotency tests (major focus).
- Assignment and reassignment logic.
- Admission/drain state transitions.
- Cancellation propagation on stream disconnect.
- Duplicate completion suppression.

2. Integration tests.
- API + multiple workers, single query with multi-worker fanout.
- Worker failure mid-query with immediate reassignment.
- API failure causing worker-side cancellation.
- Drain behavior: worker signaled to stop rejects new work, finishes in-flight work, exits cleanly.
- Verify no DB queue writes for query orchestration.
- Transient network blip: worker disconnect/reconnect with API retry loop and successful continued operation.
- Artifact expiration path: expired artifact forces deterministic work re-execution.

3. Regression tests.
- Query correctness parity with existing evaluator behavior.
- Sub-second latency for representative light queries.
- No goroutine/connection leaks under cancel/failure storms.
