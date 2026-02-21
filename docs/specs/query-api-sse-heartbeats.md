# Query API SSE Heartbeats

## Status
- Proposed and implemented in `queryapi/querier.go` on branch `feat/query-api-sse-heartbeats`.

## Summary
Long-running SSE queries in `query-api` can appear idle while the API waits for workers to return results. Some intermediaries and clients treat idle connections as dead and may terminate them.

This spec defines periodic SSE heartbeat events for streaming query endpoints so connections remain active while waiting for worker output.

## Goals
- Keep SSE query connections active during periods with no query results.
- Reduce proxy/client idle timeouts that lead to premature stream termination.
- Preserve existing SSE result and completion semantics.

## Non-Goals
- No changes to non-SSE endpoints.
- No changes to worker-side SSE protocol.
- No guarantee that all upstream infrastructure timeout policies are fully bypassed.

## Scope
Heartbeat emission applies to the query-api SSE endpoints used for long-running query streams:
- Metrics query stream: `/api/v1/metrics/query`
- Logs query stream: `/api/v1/logs/query`
- Spans query stream: `/api/v1/spans/query`

Out of scope:
- Endpoints that do not use SSE.
- One-shot JSON endpoints.

## Protocol
### Event type
- `heartbeat`

### Payload
Heartbeat events use the same SSE envelope pattern as existing stream events:

```json
{
  "type": "heartbeat",
  "data": {
    "status": "waiting"
  }
}
```

### Cadence
- Heartbeat interval is fixed at `15s` (`sseHeartbeatInterval`).
- Heartbeats are emitted only while the stream is open and waiting.

### Existing events
Existing stream behavior remains unchanged:
- `result` events carry query result data.
- `done` is emitted once at normal stream completion.

## Server Behavior
For each SSE loop in scope:
- Start a ticker at stream setup.
- On each ticker tick, emit one `heartbeat` event.
- If heartbeat write fails, stop stream processing for that request.
- Stop ticker when stream exits.

Priority rules in the select loop are unchanged; heartbeat may interleave with results naturally.

## Backward Compatibility
- Clients that ignore unknown SSE event types continue to work unchanged.
- Clients that parse the envelope `type` field can optionally handle `heartbeat`.
- No schema changes to existing `result`/`done` payloads.

## Operational Notes
- Heartbeats add minimal overhead (small JSON payload every 15 seconds per active stream).
- This improves survivability through idle-sensitive proxies but does not replace correct timeout configuration in all intermediaries.

## Validation
Minimum validation for this change:
- Unit/package tests for `queryapi` pass.
- Manual stream test confirms:
  - periodic `heartbeat` while no results are available,
  - continued delivery of `result` events,
  - final `done` event on completion.

## Implementation Reference
Current implementation points:
- `queryapi/querier.go`
  - `sseHeartbeatInterval`
  - `writeHeartbeatEvent(...)`
  - heartbeat ticker integrated in:
    - metrics stream loop (`sendEvalResults`)
    - logs stream loop (`handleLogQuery`, raw logs path)
    - spans stream loop (`handleSpansQuery`, raw spans path)
