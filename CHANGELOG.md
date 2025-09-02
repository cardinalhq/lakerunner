# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Debug Commands**: Added `debug objcleanup status` to show pending and non-pending object counts by bucket
- **Database Indexes**: Added bucket-based index for `obj_cleanup` table for faster bucket summaries

### Changed

- **Record Count Estimation**: Split estimator into signal-specific `MetricEstimator` and `LogEstimator` types with frequency-aware metrics estimation
- **Database Index**: Updated metric_seg index to `(frequency_ms, instance_num)` for improved query performance
- **Queue Indexes**: Consolidated work queue indexes and added eligibility support for faster job retrieval
- **Build Process**: Improved promotion pipeline for production releases
- **Logging**: Removed confusing CONFIGDB fallback message when database is not configured
- **Work Queue Configuration**: Converted all settings from database table to Go parameters (retry count, timeouts, TTLs)

### Removed

- **Settings Table**: Dropped database settings table as all configuration moved to Go code parameters
- **Settings Dependencies**: Updated database functions to use hardcoded defaults instead of settings table lookups

### Fixed

- **Work Queue Cleanup**: Fixed bug where cleanup process didn't reset retry count, causing items to fail immediately after worker recovery

## [v1.1.0-rc3] - 2025-08-17

### Added

- **DuckDB Extension Management**: Added air-gapped extension loading system with `LAKERUNNER_EXTENSIONS_PATH` and `LAKERUNNER_HTTPFS_EXTENSION` environment variables
- **Auto-loading Extensions**: httpfs extension now loads automatically by default in `duckdbx.Open()`
- **Extension Configuration Options**: Added `WithoutExtension()` option to remove default extensions when not needed
- **Debug Commands**: New `debug ddb` command suite:
  - `debug ddb extensions` - Lists all DuckDB extensions with availability status
  - `debug ddb version` - Shows DuckDB version with environment variable status and extension availability
- **PromQL Support**: Added PromQL expression parsing and query planning framework
- **Query Worker Framework**: Implemented distributed query worker system for scalable query processing
- **Metrics Exemplars**: Added exemplar generation and export functionality for metrics
- **HyperLogLog Support**: Added HLL aggregation support with tests and SQL builder integration
- **Database Migration Improvements**: Added slot_id support and improved migration safety with table locking
- **JSON Message Parsing**: Added support for parsing JSON from SQS message bodies

### Changed

- **Command Structure Refactoring**: Major reorganization of CLI commands for better usability
- **Multi-stage Docker Builds**: Updated to CGO=1 builds with optimized Docker images
- **Build System**: Updated to use goreleaser for multi-architecture builds with partial compilation support
- **Extension Loading**: Simplified extension API - removed path parameter from `WithExtension()`
- **Query Performance**: Improved query performance with compound keys and segment filtering optimizations
- **Memory Management**: Reduced allocations in trigram generation and log compaction
- **Database Operations**: Enhanced segment operations with query size limits and unbounded query protections

### Fixed

- **Migration Safety**: Fixed database migration deadlocks with proper table locking
- **Query Bounds**: Fixed unbounded SQL queries in metric segment operations
- **Memory Leaks**: Improved memory management and garbage collection in processing pipelines
- **Test Coverage**: Enhanced test suite with better coverage and reliability
- **Lint Compliance**: Fixed various linting issues and added import order checking

### Security

- **License Compliance**: Added AGPL v3 license headers to all source files
- **Air-gapped Deployments**: Implemented secure extension loading for air-gapped environments
- **Environment Variable Namespacing**: Prefixed internal environment variables with `LAKERUNNER_` to avoid conflicts

## [v1.0.1] - 2025-08-05

### Added

- Initial stable release with core telemetry ingestion functionality
- S3-compatible object store integration
- Basic log and metric processing capabilities
- PostgreSQL-based segment indexing
- Docker container support

[Unreleased]: https://github.com/cardinalhq/lakerunner/compare/v1.1.0-rc3...HEAD
[v1.1.0-rc3]: https://github.com/cardinalhq/lakerunner/compare/v1.0.1...v1.1.0-rc3
[v1.0.1]: https://github.com/cardinalhq/lakerunner/releases/tag/v1.0.1
