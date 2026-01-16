---
sidebar_position: 1
---

# Health Checks

Worker services provide HTTP health check endpoints for Kubernetes monitoring.

## Endpoints

- **`/healthz`** – Main health check endpoint (200 when healthy, 503 when not)
- **`/readyz`** – Readiness probe (200 when ready to accept traffic)
- **`/livez`** – Liveness probe (200 unless service is completely broken)

## Services with Health Checks

- **query-api** – Query API server
- **query-worker** – Query worker service
- **sweeper** – Background cleanup service

Health check responses include JSON with status, timestamp, and service name.
