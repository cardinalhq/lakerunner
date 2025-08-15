# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LakeRunner is a real-time telemetry ingestion engine that transforms S3-compatible object stores into high-performance observability backends. It processes structured telemetry data (CSV, Parquet, JSON.gz) from sources like OpenTelemetry collectors, DataDog, and FluentBit, converting them into optimized Apache Parquet format with indexing, aggregation, and compaction capabilities.

## Development Commands

### Building and Testing

- `make local` - Build binary locally (outputs to bin/lakerunner)
- `make test` - Run full test suite with code generation
- `make test-only` - Run tests without regenerating code
- `make check` - Run comprehensive checks: tests, linting, and license validation
- `make generate` - Generate code using `go generate ./...`

### Linting and Quality

- `make lint` - Run golangci-lint with 15m timeout
- `make license-check` - Validate license headers using license-eye tool
- `make imports-fix` - ensure the imports statements are sane
- Run gofmt to ensure there are no trailing whitespace or other issues.

### Database Migrations

- `make new-migration name=migration_name` - Create new database migration files

### Docker and Release

- `make images` - Build multi-architecture Docker images using goreleaser
- `make promote-to-prod` - Promote dev images to production

## Architecture Overview

LakeRunner follows an event-driven architecture with these core components:

### Data Flow Pipeline

1. **PubSub Handler** - Receives S3 notifications via SQS or HTTP webhooks
2. **Ingestion** - Processes raw files into cooked Parquet format
3. **Processing** - Handles compaction, rollups, and cleanup operations
4. **Query** - Serves data through API and worker nodes

### Key Databases

- **lrdb** - Main LakeRunner database for segment indexing and work queues
- **configdb** - Configuration and storage profile management
- Uses PostgreSQL with migrations in `lrdb/migrations/`

### Major Components

- **cmd/pubsub/** - S3 event notification handling (SQS, webhooks)
- **cmd/ingest_*/** - File ingestion and format conversion
- **cmd/compact_*/** - Data compaction for logs and metrics
- **cmd/rollup_metrics.go** - Metric aggregation and rollups
- **cmd/sweeper.go** - Cleanup and maintenance operations
- **fileconv/** - Format conversion utilities (JSON.gz, Proto, Parquet)
- **internal/buffet/** - Parquet processing utilities
- **internal/logcrunch/** - Log compaction and fingerprinting
- **lockmgr/** - Distributed work coordination

### Storage Architecture

- **Raw files** - Stored in S3 under `otel-raw/`, `logs-raw/`, `metrics-raw/` prefixes
- **Cooked files** - Optimized Parquet files stored back to S3
- **Segment indexing** - PostgreSQL tables track data segments for query optimization

## Code Conventions

### Go Standards

- Uses Go 1.24+ features (e.g., range over integers)
- All source files require AGPL v3 license headers
- Code generation via `go generate` for SQL queries (using sqlc)

### Testing Approach

- Table-driven tests for simple functions
- Individual tests for complex setup/inspection scenarios
- Test files must accompany changes to existing tested functions
- New public methods require Go documentation

### Database Schema

- Uses sqlc for type-safe SQL query generation
- Migration files in `lrdb/migrations/` and `internal/configdb/static-schema/`
- Database connections managed through connection pools

## Development Workflow

1. **Code Generation** - Run `make generate` after modifying SQL queries
2. **Testing** - Use `make test` for full validation including regeneration
3. **Quality Checks** - `make check` must pass before PR submission
4. **License Compliance** - All new source files need AGPL v3 headers

## Storage Profiles and Configuration

LakeRunner uses YAML-based configuration for:

- **Storage Profiles** - Map S3 buckets to organizations and collectors
- **API Keys** - Authentication for query API access
- **Cloud Provider Settings** - AWS/GCP/Azure integration parameters

Key environment variables:

- `AWS_ACCESS_KEY`, `AWS_SECRET_KEY` - S3 credentials
- `S3_BUCKET` - Target bucket name
- `S3_PROVIDER` - Cloud provider (aws, gcp)
- `GOGC` - Go garbage collection tuning (defaults to 50%)

## Query Architecture

The query system consists of:

- **Query API** - REST API for data access
- **Query Workers** - Scalable processing nodes
- **Segment Index** - PostgreSQL-based metadata for efficient data location
- **DuckDB Integration** - High-performance analytical queries on Parquet data
