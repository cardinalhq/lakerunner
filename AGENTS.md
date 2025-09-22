# AGENTS.md - Agent Instructions for Lakerunner Codebase

## Project Overview

Lakerunner is a **real-time telemetry ingestion engine** that transforms S3-compatible object stores into high-performance observability backends. It's a batch processing system that:

- Ingests structured telemetry data (CSV, Parquet, JSON.gz) from sources like OpenTelemetry collectors
- Converts data into optimized Apache Parquet with indexing, aggregation, and compaction
- Currently processes logs and metrics (traces/spans and profile data may be added in future)

## Core Rules - NEVER BREAK THESE

1. **Do not edit generated files** (e.g., `*.pb.go`, `*.sql.go`, sqlc output, or any file marked as generated)
2. **Do not include promotional text** in commits (e.g., "Generated with Claude Code")
3. **Do not remove `partial:` sections** from `goreleaser` config—required for CI
4. **Do not edit old migrations** (ones changed in current session are OK)
5. **Always run `make check` before completing any coding task** - Work is NOT complete until it passes

## Development Workflow

### Essential Commands

- **Build:** `make` (binary in `./bin/lakerunner`)
- **Test:** `make test` (full suite with regeneration)
- **Test Only:** `make test-only` (tests without regeneration)
- **Integration:** `make test-integration` (DB-requiring tests)
- **Quality Check:** `make check` (MUST pass before work is complete)
- **Generate:** `make generate` (after SQL query changes)
- **Lint:** `make lint` (golangci-lint, 15m timeout)
- **Proto tools:** `make bin/buf` (installs Buf in `./bin`)

### Code Generation

- Run `make generate` after modifying SQL queries
- Never modify generated files manually
- Install Buf CLI with `make bin/buf` for protobuf generation

### Database Migrations

- **New lrdb migration:** `make new-lrdb-migration name=migration_name`
- **New configdb migration:** `make new-configdb-migration name=migration_name`
- Migrations run inside transactions (no CREATE INDEX CONCURRENTLY)
- `*_seg` tables are partitioned by `organization_id` then `dateint`
- Test databases available: `testing_configdb` and `testing_lrdb`
- Use `psql-17` command for database access

## Code Standards

### Go Guidelines

- **Go 1.25** - use modern features:
  - `for i := range 1234`
  - `maps.Copy()`
  - `slices.Sort()`
- Follow idiomatic Go style
- `make fmt` fixes imports and formatting
- Use `github.com/stretchr/testify` for testing

### Testing Requirements

- Table-driven tests for simple cases
- Dedicated tests for complex setups
- All new/changed functions must include tests
- Public methods need GoDoc comments (avoid redundant ones)
- Update existing tests when modifying functions

### Code Comments

- Write comments for senior engineers
- No historical comments ("removed...", "this used to...")
- We use git for history

### Licensing

All source files must include AGPL v3 header:

```go
// Copyright (C) 2025 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
```

## Architecture & Components

### Data Flow Pipeline

1. **PubSub Handler** – S3 notifications via SQS or webhooks
2. **Ingestion** – Raw files → Parquet conversion
3. **Processing** – Compaction, rollups, cleanup
4. **Query** – API + worker nodes serve data

### Storage Structure

- **Raw files**: S3 under `otel-raw/`, `logs-raw/`, `metrics-raw/`
- **Cooked files**: Optimized Parquet back to S3
- **Segment index**: PostgreSQL tables for query optimization

### Databases

- **lrdb** – main DB (segments, work queues), migrations in `lrdb/migrations/`
- **configdb** – configuration & storage profiles, schema fixed and externally managed
- Type-safe SQL via sqlc
- Pooled connections for performance

### Major Components

- **cmd/pubsub/** – S3 event handling
- **cmd/ingest_*/** – ingestion & conversion
- **cmd/compact_*/** – data compaction
- **cmd/rollup_metrics.go** – metric rollups
- **cmd/sweeper.go** – cleanup tasks
- **fileconv/** – conversion helpers (JSON.gz, Proto, Parquet)
- **internal/buffet/** – Parquet utilities
- **internal/logcrunch/** – log compaction/fingerprints
- **lockmgr/** – distributed coordination

## Debug Tools

### Parquet Debug Commands

- `./bin/lakerunner debug parquet cat --file <file>` – Output contents as JSON lines
- `./bin/lakerunner debug parquet schema --file <file>` – Analyze column types (JSON format)
- `./bin/lakerunner debug parquet schema-raw --file <file>` – Show raw metadata schema

## Environment Variables

### Core Configuration

- `AWS_ACCESS_KEY`, `AWS_SECRET_KEY` – S3 credentials
- `S3_BUCKET` – target bucket
- `S3_PROVIDER` – provider (`aws`, `gcp`)
- `GOGC` – GC tuning (default 50%)

### Migration Checks

- `LRDB_MIGRATION_CHECK_ENABLED` – enable/disable (default: true)
- `CONFIGDB_MIGRATION_CHECK_ENABLED` – enable/disable (default: true)
- `MIGRATION_CHECK_TIMEOUT` – max wait time (default: 60s)
- `MIGRATION_CHECK_RETRY_INTERVAL` – check interval (default: 5s)
- `MIGRATION_CHECK_ALLOW_DIRTY` – allow dirty state (default: false)

### Health Checks

- `HEALTH_CHECK_PORT` – HTTP server port (default: 8090)
- `HEALTH_CHECK_SERVICE_NAME` – service name in responses

## Pull Request Guidelines

- Focus on specific, testable, reviewable changes
- Leave code in working state
- Main branch must always be releasable
- Run `make check` before submitting
- Use conventional commit style when applicable

## Health Checks

Worker services provide HTTP health check endpoints for Kubernetes monitoring:

- **`/healthz`** – Main health check endpoint (200 when healthy, 503 when not)
- **`/readyz`** – Readiness probe (200 when ready to accept traffic)
- **`/livez`** – Liveness probe (200 unless service is completely broken)

Services with health checks:

- **query-api** – Query API server
- **query-worker** – Query worker service
- **sweeper** – Background cleanup service

Health check responses include JSON with status, timestamp, and service name.

## Storage Profiles & Configuration

Configuration is YAML-based:

- **Storage Profiles** – mapping S3 buckets to orgs/collectors
- **API Keys** – for authentication
- **Cloud Settings** – AWS/GCP/Azure integration

### Container Runtime

Inside the container, memory and CPU limits for Go will match the constraints of the container.

## Query Architecture

- **Query API** – REST interface
- **Query Workers** – scalable workers
- **Segment Index** – PostgreSQL metadata
- **DuckDB Integration** – high-performance queries over Parquet

## Docker & Release

- Docker images built via GitHub Actions
- Image tag management handled automatically
- CI uses **partial builds** (`partial:` section in goreleaser)
- Developers cannot use `partial:` locally, but must keep it in config

## Important Reminders

- This is a monorepo - no backwards compatibility needed
- Always prefer editing existing files over creating new ones
- Never proactively create documentation unless requested
- When working on SQL, ensure `make test-integration` passes
- Do what has been asked; nothing more, nothing less
- "make test-integration" should be run via the sub-agent named "integration-test-runner"
- "make check" should be run via the sub-agent named check-runner.
