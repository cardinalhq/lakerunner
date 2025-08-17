# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Core Rules

1. **Never break these rules:**
   - Do not edit generated files (e.g., `*.pb.go`, `*.sql.go`, sqlc output, or any file marked as generated).
   - Do not include Claude Code promotional text in commits (e.g., “Generated with Claude Code”).
   - Do not remove `partial:` sections from `goreleaser` config—these are required for CI.
   - Do not edit old migrations.  One you just changed in this session is OK.

---

## Project Overview

LakeRunner is a **real-time telemetry ingestion engine** that transforms S3-compatible object stores into high-performance observability backends.

It ingests structured telemetry data (CSV, Parquet, JSON.gz) from sources like OpenTelemetry collectors, DataDog, and FluentBit. Data is converted into optimized **Apache Parquet** with indexing, aggregation, and compaction.

---

## Development Workflow

1. **Code Generation**
   - Run `make generate` after modifying SQL queries.
   - Never modify generated files manually.

2. **Testing & Validation**
   - `make test` for full suite with regeneration.
   - `make test-only` to run tests without regeneration.
   - `make check` for full validation: tests, lint, license headers.

3. **Quality & Compliance**
   - `make lint` for golangci-lint (15m timeout).
   - `make license-check` for license headers.
   - `make imports-fix` and `gofmt` to keep code tidy.
   - All source files must include AGPL v3 headers.

4. **Migrations**
   - `make new-migration name=migration_name` for lrdb migrations.
   - No migrations permitted in `configdb` (externally managed schema).

5. **Commit Messages**
   - Keep clean, technical, and focused.
   - Use conventional commit style when applicable.

---

## Development Commands

- **Build:** `make local` (binary in `./bin/lakerunner`)
- **Check & Test:** `make test`, `make test-only`, `make check`
- **Codegen:** `make generate`
- **Lint & Format:** `make lint`, `make imports-fix`
- **License:** `make license-check`
- **Migrations:** `make new-migration name=migration_name`

---

## Architecture Overview

### Data Flow Pipeline

1. **PubSub Handler** – S3 notifications via SQS or webhooks
2. **Ingestion** – Raw files → Parquet conversion
3. **Processing** – Compaction, rollups, cleanup
4. **Query** – API + worker nodes serve data

### Storage

- **Raw files**: S3 under `otel-raw/`, `logs-raw/`, `metrics-raw/`
- **Cooked files**: Optimized Parquet back to S3
- **Segment index**: PostgreSQL tables for query optimization

### Databases

- **lrdb** – main DB (segments, work queues), migrations in `lrdb/migrations/`
- **configdb** – configuration & storage profiles, schema fixed and externally managed

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

---

## Code Conventions

### Go Standards

- Go 1.25 (use new language features).
- Follow idiomatic Go style; imports fixed with `make fmt` which will gofmt and order imports.
- Generated SQL via sqlc and protobufs.  Do not edit generated code.

### Testing

- Table-driven tests for simple cases.
- Dedicated tests for complex setups.
- All new/changed functions must include tests.
- New public methods require GoDoc comments.

### Database Schema

- Type-safe SQL via sqlc.
- `lrdb/migrations/` for schema changes.
- `internal/configdb/static-schema/` contains fixed schema snapshots (no migrations allowed).
- Connections are pooled for performance.

Rules:

- Migrations run inside a transaction, so CREATE INDEX CONCURRENTLY will not work for instance.
- The `*_seg` tables are partitioned first by `organization_id`, and then those are further partitioned by `dateint`.  Use this information when crafting queries for table pruning to be efficient, and when making indexes.

---

## Query Architecture

- **Query API** – REST interface
- **Query Workers** – scalable workers
- **Segment Index** – PostgreSQL metadata
- **DuckDB Integration** – high-performance queries over Parquet

---

## Storage Profiles & Configuration

Configuration is YAML-based:

- **Storage Profiles** – mapping S3 buckets to orgs/collectors
- **API Keys** – for authentication
- **Cloud Settings** – AWS/GCP/Azure integration

**Environment Variables:**

- `AWS_ACCESS_KEY`, `AWS_SECRET_KEY` – S3 creds
- `S3_BUCKET` – target bucket
- `S3_PROVIDER` – provider (`aws`, `gcp`)
- `GOGC` – GC tuning (default 50%)
- Inside the container, memory and cpu limits for Go will match the constraints of the container.

---

## Docker & Release

- Docker images built via GitHub Actions.
- Image tag management handled automatically.
- CI uses **partial builds** (`partial:` section in goreleaser).
- Developers cannot use `partial:` locally, but must keep it in config.
