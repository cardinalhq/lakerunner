
## Key Invariants
1.	Groups are time-disjoint and chronological by index.
We rely on ComputeReplayBatchesWithWorkers(...) to produce non-overlapping windows and we use the order they are produced as the coordinator index.
2.	Per-leaf start shifting:
For each leaf:
•	baseStart = startTs - `RangeMsFromRange(leaf.Range)` (if any)
•	effStart = baseStart - offsetMs(leaf.Offset)
•	effEnd   = endTs - offsetMs(leaf.Offset)
This avoids gaps on the left edge for range functions while respecting offsets.
3.	Within a group we merge multiple worker streams by timestamp; across groups we concatenate by index (no global merge).


## Stages (with important details)

                +----------------------------+
                | EvaluateMetricsQuery       |
                | (entry)                    |
                +-------------+--------------+
                              |
                              v
                  enumerateAndLaunchGroups
                              |
                              v
                +----------------------------+      (N groups; bounded by sem)
                | launchOneGroup (per group) |-----> per-worker pushdowns
                +----------------------------+                  |
                              |                                 v
                              |                      MergeSorted(worker chans)
                              |                                 |
                              +--------- register (idx) --------+
                                                |
                                                v
                                  runOrderedCoordinator (concat by idx)
                                                |
                                                v
                                          EvalFlow.Run
                                                |
                                                v
                                        streamed EvalResult

### Plumbing & Concurrency
   •	maxParallel = max(3, numWorkers) → prevents underutilization on tiny clusters.
   •	sem := make(chan struct{}, maxParallel) bounds group goroutines.
   •	groupRegs := chan groupReg is a registry; each launched group sends one (idx, start, end, mergedChan) to it as soon as ready, not after completion.

### Enumerate & Launch
•	For each leaf:
•	Parse offset, compute baseStart using the leaf’s range, then get effStart/effEnd.
•	Look up segments per hour (dateIntHoursRange), stamp ExprID.
•	ComputeReplayBatchesWithWorkers(segments, step, effStart, effEnd, /*workers*/ cap(sem), reverse=true)
We intentionally pass cap(sem) as the “workers” hint so batch sizing roughly matches how many groups can run concurrently.
•	For each group:
•	Reserve the semaphore.
•	Launch launchOneGroup.

### launchOneGroup (per group)
•	Worker assignment via GetWorkersForSegments.
•	For each worker, call metricsPushDown → returns a channel of SketchInput.
•	Wrap each channel in shiftTimestamps if the leaf had an offset.
•	Local merge of worker channels: promql.MergeSorted(ctx, 1024, /*reverse*/ false, /*limit*/ 0, ...).
•	Register immediately: send groupReg{idx, startTs, endTs, mergedCh} to groupRegs.

If anything fails: we still register a closed/empty channel so the coordinator doesn’t stall.

### Ordered Coordinator
   •	runOrderedCoordinator concatenates groups strictly by ascending idx:
   •	Buffers out-of-order registrations in pending[idx].
   •	When it has want (starting at 0), it drains that group’s channel fully to coordinated.
   •	Then it increments want and repeats.
   •	Assumption: groups are time-disjoint and idx order matches chronology. If your grouping ever overlaps, switch back to a final MergeSorted across groups.

### EvalFlow
   •	Single EvalFlow.Run(ctx, coordinated) consumes the concatenated stream.


### Ordering & Streaming Guarantees
•	Within a group: strictly timestamp-sorted (due to MergeSorted).
•	Across groups: streamed as groups finish registration but emitted in idx order.
Practically, you start seeing results as soon as idx=0 is registered and begins producing—even while later groups are still downloading/processing.
•	No global time sort across groups—by design—for lowest latency and fewer buffers. This is why non-overlap of groups is important.


### Range & Offset Handling (Why we mutate per leaf)
•	PromQL range functions need previous buckets. If we started each leaf at the global startTs, we’d drop early coverage.
•	We therefore compute baseStart = startTs - range(leaf) per leaf, not mutate the global startTs.
•	Then apply the offset to produce effStart/effEnd used for segment discovery.


### Backpressure & Buffers
•	Channels:
•	Worker→group merge buffers: 1024
•	Coordinator output: 4096
•	EvalFlow output to caller: 1024
•	If downstream (client) is slow, backpressure propagates and naturally slows ingestion.

⸻

### Cancellation & Errors
•	All goroutines select on ctx.Done().
•	On failure, a closed empty channel is registered so the coordinator can advance.
•	EvalFlow loop closes out cleanly on completion or cancellation.

⸻

### Tuning Knobs
•	Parallelism: computeMaxParallel(len(workers)) → tweak rule if needed.
•	Per-group merge buffer: MergeSorted(..., 1024, ...).
•	Coordinator buffer: coordinated := make(chan promql.SketchInput, 4096).
•	Batch sizing in ComputeReplayBatchesWithWorkers (internally uses a target size derived from segment count & worker hint).

⸻

### When to Add a Final Merge

Add a final MergeSorted across all group streams if any is true:
•	Groups could overlap in time.
•	You must guarantee globally time-sorted emission across the entire query range.
•	You’re willing to trade slightly higher latency & memory for ordering guarantees.

(Our current implementation assumes disjoint groups and prioritizes streaming latency.)
