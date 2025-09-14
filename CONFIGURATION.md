# Lakerunner Configuration Reference

This document describes all configuration options for Lakerunner, including defaults and environment variables for overrides.

Configuration is loaded from:

1. Configuration file (`config.yaml` in current directory)
2. Environment variables (prefix: `LAKERUNNER_`, dots replaced with underscores)

## Core Configuration

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| debug | `false` | `LAKERUNNER_DEBUG` | Enable debug mode for additional logging |

## Kafka Configuration

Kafka settings support migration from legacy `fly.*` prefix (deprecated) to new `kafka.*` prefix.

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| kafka.brokers | `["localhost:9092"]` | `LAKERUNNER_KAFKA_BROKERS` | Comma-separated list of Kafka brokers |
| kafka.sasl_enabled | `false` | `LAKERUNNER_KAFKA_SASL_ENABLED` | Enable SASL authentication |
| kafka.sasl_mechanism | `"SCRAM-SHA-256"` | `LAKERUNNER_KAFKA_SASL_MECHANISM` | SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) |
| kafka.sasl_username | `""` | `LAKERUNNER_KAFKA_SASL_USERNAME` | SASL username |
| kafka.sasl_password | `""` | `LAKERUNNER_KAFKA_SASL_PASSWORD` | SASL password |
| kafka.tls_enabled | `false` | `LAKERUNNER_KAFKA_TLS_ENABLED` | Enable TLS encryption |
| kafka.tls_skip_verify | `false` | `LAKERUNNER_KAFKA_TLS_SKIP_VERIFY` | Skip TLS certificate verification |
| kafka.producer_batch_size | `100` | `LAKERUNNER_KAFKA_PRODUCER_BATCH_SIZE` | Producer batch size |
| kafka.producer_batch_timeout | `10ms` | `LAKERUNNER_KAFKA_PRODUCER_BATCH_TIMEOUT` | Producer batch timeout |
| kafka.producer_compression | `"snappy"` | `LAKERUNNER_KAFKA_PRODUCER_COMPRESSION` | Producer compression algorithm |
| kafka.consumer_group_prefix | `"lakerunner"` | `LAKERUNNER_KAFKA_CONSUMER_GROUP_PREFIX` | Consumer group prefix |
| kafka.consumer_batch_size | `100` | `LAKERUNNER_KAFKA_CONSUMER_BATCH_SIZE` | Consumer batch size |
| kafka.consumer_max_wait | `500ms` | `LAKERUNNER_KAFKA_CONSUMER_MAX_WAIT` | Consumer max wait time |
| kafka.consumer_min_bytes | `10240` (10KB) | `LAKERUNNER_KAFKA_CONSUMER_MIN_BYTES` | Consumer minimum bytes |
| kafka.consumer_max_bytes | `10485760` (10MB) | `LAKERUNNER_KAFKA_CONSUMER_MAX_BYTES` | Consumer maximum bytes |
| kafka.connection_timeout | `10s` | `LAKERUNNER_KAFKA_CONNECTION_TIMEOUT` | Connection timeout |

## Kafka Topic Configuration

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| kafka_topics.topicPrefix | `"lakerunner"` | `LAKERUNNER_KAFKA_TOPIC_PREFIX` or `LAKERUNNER_KAFKA_TOPICS_TOPICPREFIX` | Topic name prefix |
| kafka_topics.defaults.partitionCount | `16` | `LAKERUNNER_KAFKA_TOPICS_DEFAULTS_PARTITIONCOUNT` | Default partition count for new topics |
| kafka_topics.defaults.replicationFactor | `3` | `LAKERUNNER_KAFKA_TOPICS_DEFAULTS_REPLICATIONFACTOR` | Default replication factor |
| kafka_topics.defaults.options.cleanup.policy | `"delete"` | - | Topic cleanup policy |
| kafka_topics.defaults.options.retention.ms | `"604800000"` (7 days) | - | Message retention time |

External override file can be specified via `--kafka-topics-file` flag or `KAFKA_TOPICS_FILE` environment variable.

## Metrics Configuration

### Ingestion Settings

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| metrics.ingestion.process_exemplars | `true` | `LAKERUNNER_METRICS_INGESTION_PROCESS_EXEMPLARS` | Process exemplar data |
| metrics.ingestion.single_instance_mode | `false` | `LAKERUNNER_METRICS_INGESTION_SINGLE_INSTANCE_MODE` | Single instance processing mode |
| metrics.ingestion.max_accumulation_time | `10s` | `LAKERUNNER_METRICS_INGESTION_MAX_ACCUMULATION_TIME` | Maximum accumulation time before processing |

### Compaction Settings

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| metrics.compaction.target_file_size_bytes | `1048576` (1MB) | `LAKERUNNER_METRICS_COMPACTION_TARGET_FILE_SIZE_BYTES` | Target file size for compaction |
| metrics.compaction.max_accumulation_time | `30s` | `LAKERUNNER_METRICS_COMPACTION_MAX_ACCUMULATION_TIME` | Maximum time to accumulate segments |

### Rollup Settings

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| metrics.rollup.batch_limit | `100` | `LAKERUNNER_METRICS_ROLLUP_BATCH_LIMIT` | Batch size limit for rollups |

## Batch Processing Configuration

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| batch.target_size_bytes | `104857600` (100MB) | `LAKERUNNER_BATCH_TARGET_SIZE_BYTES` | Target batch size in bytes |
| batch.max_batch_size | `100` | `LAKERUNNER_BATCH_MAX_BATCH_SIZE` | Maximum number of items in batch |
| batch.max_total_size | `1073741824` (1GB) | `LAKERUNNER_BATCH_MAX_TOTAL_SIZE` | Maximum total batch size |
| batch.max_age_seconds | `300` (5 min) | `LAKERUNNER_BATCH_MAX_AGE_SECONDS` | Maximum batch age before processing |
| batch.min_batch_size | `1` | `LAKERUNNER_BATCH_MIN_BATCH_SIZE` | Minimum batch size |

## DuckDB Configuration

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| duckdb.extensions_path | `""` | `LAKERUNNER_DUCKDB_EXTENSIONS_PATH` or `LAKERUNNER_EXTENSIONS_PATH` | Path to DuckDB extensions for air-gapped mode |
| duckdb.httpfs_extension | `""` | `LAKERUNNER_DUCKDB_HTTPFS_EXTENSION` or `LAKERUNNER_HTTPFS_EXTENSION` | Path to HTTPFS extension |
| duckdb.azure_extension | `""` | `LAKERUNNER_DUCKDB_AZURE_EXTENSION` or `LAKERUNNER_AZURE_EXTENSION` | Path to Azure extension |
| duckdb.aws_extension | `""` | `LAKERUNNER_DUCKDB_AWS_EXTENSION` or `LAKERUNNER_AWS_EXTENSION` | Path to AWS extension |
| duckdb.memory_limit | `0` (unlimited) | `LAKERUNNER_DUCKDB_MEMORY_LIMIT` | Memory limit in MB |
| duckdb.temp_directory | `$TMPDIR` or `/tmp` | `LAKERUNNER_DUCKDB_TEMP_DIRECTORY` or `DUCKDB_TEMP_DIRECTORY` | Directory for temporary files (defaults to TMPDIR environment variable) |
| duckdb.max_temp_directory_size | 90% of volume size | `LAKERUNNER_DUCKDB_MAX_TEMP_DIRECTORY_SIZE` or `DUCKDB_MAX_TEMP_DIRECTORY_SIZE` | Maximum size for temp directory (defaults to 90% of temp volume size) |
| duckdb.s3_pool_size | `0` (auto) | `LAKERUNNER_DUCKDB_S3_POOL_SIZE` | S3 connection pool size |
| duckdb.s3_conn_ttl_seconds | `240` (4 min) | `LAKERUNNER_DUCKDB_S3_CONN_TTL_SECONDS` | S3 connection TTL |
| duckdb.threads_per_conn | `0` (auto) | `LAKERUNNER_DUCKDB_THREADS_PER_CONN` | Threads per DuckDB connection |

## S3 Configuration

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| s3.access_key_id | `""` | `LAKERUNNER_S3_ACCESS_KEY_ID` or `S3_ACCESS_KEY_ID` | AWS access key ID |
| s3.secret_access_key | `""` | `LAKERUNNER_S3_SECRET_ACCESS_KEY` or `S3_SECRET_ACCESS_KEY` | AWS secret access key |
| s3.session_token | `""` | `LAKERUNNER_S3_SESSION_TOKEN` or `AWS_SESSION_TOKEN` | AWS session token (for temporary credentials) |
| s3.region | `""` | `LAKERUNNER_S3_REGION` or `AWS_REGION` or `AWS_DEFAULT_REGION` | AWS region |
| s3.url_style | `""` | `LAKERUNNER_S3_URL_STYLE` or `AWS_S3_URL_STYLE` | URL style ("path" or "vhost") |

## Azure Configuration

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| azure.auth_type | `""` | `LAKERUNNER_AZURE_AUTH_TYPE` or `AZURE_AUTH_TYPE` | Authentication type ("service_principal" or "connection_string") |
| azure.client_id | `""` | `LAKERUNNER_AZURE_CLIENT_ID` or `AZURE_CLIENT_ID` | Service principal client ID |
| azure.client_secret | `""` | `LAKERUNNER_AZURE_CLIENT_SECRET` or `AZURE_CLIENT_SECRET` | Service principal client secret |
| azure.tenant_id | `""` | `LAKERUNNER_AZURE_TENANT_ID` or `AZURE_TENANT_ID` | Azure tenant ID |
| azure.connection_string | `""` | `LAKERUNNER_AZURE_CONNECTION_STRING` or `AZURE_STORAGE_CONNECTION_STRING` | Storage connection string |

Additional Azure settings (not in config file):

- `AZURE_STORAGE_ACCOUNT` - Storage account name (for pubsub)
- `AZURE_QUEUE_NAME` - Queue name (for pubsub)

## Database Configuration

### LRDB (Main Database)

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `LRDB_URL` | - | Complete PostgreSQL URL (overrides all other LRDB_* variables) |
| `LRDB_HOST` | - | Database host (required if URL not set) |
| `LRDB_PORT` | `5432` | Database port |
| `LRDB_USER` | - | Database user |
| `LRDB_PASSWORD` | - | Database password |
| `LRDB_DBNAME` | - | Database name (required if URL not set) |
| `LRDB_SSLMODE` | - | SSL mode (e.g., "require", "disable") |

### ConfigDB (Configuration Database)

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `CONFIGDB_URL` | - | Complete PostgreSQL URL (overrides all other CONFIGDB_* variables) |
| `CONFIGDB_HOST` | - | Database host (required if URL not set) |
| `CONFIGDB_PORT` | `5432` | Database port |
| `CONFIGDB_USER` | - | Database user |
| `CONFIGDB_PASSWORD` | - | Database password |
| `CONFIGDB_DBNAME` | - | Database name (required if URL not set) |
| `CONFIGDB_SSLMODE` | - | SSL mode (e.g., "require", "disable") |

## Migration Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `LRDB_MIGRATION_CHECK_ENABLED` | `true` | Enable LRDB migration version checking |
| `CONFIGDB_MIGRATION_CHECK_ENABLED` | `true` | Enable ConfigDB migration version checking |
| `MIGRATION_CHECK_TIMEOUT` | `60s` | Maximum wait time for migration checks |
| `MIGRATION_CHECK_RETRY_INTERVAL` | `5s` | Retry interval for migration checks |
| `MIGRATION_CHECK_ALLOW_DIRTY` | `false` | Allow running with dirty migration state |

## Admin Configuration

| Setting | Default | Environment Variable | Description |
|---------|---------|---------------------|-------------|
| admin.initial_api_key | `""` | `LAKERUNNER_ADMIN_INITIAL_API_KEY` or `ADMIN_API_KEY` | Initial admin API key |
| - | - | `ADMIN_CONFIG_FILE` | Path to admin configuration file |
| - | - | `STORAGE_PROFILE_FILE` | Path to storage profiles file (or `env:VAR_NAME` to read from environment) |
| - | - | `API_KEYS_FILE` | Path to API keys file (or `env:VAR_NAME` to read from environment) |

## Health Check Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `HEALTH_CHECK_PORT` | `8090` | HTTP port for health check endpoints |
| `HEALTH_CHECK_SERVICE_NAME` | - | Service name in health check responses |

## Monitoring & Debugging

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `PPROF_PORT` | - | Port for pprof profiling server (disabled if not set) |
| `OTEL_SERVICE_NAME` | - | OpenTelemetry service name (also used as PostgreSQL application_name) |
| `GOGC` | `50` | Go garbage collection percentage |
| `GOMEMLIMIT` | (auto: 80% of container/system) | Go memory limit |

## PubSub Configuration

### AWS SQS

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SQS_QUEUE_URL` | - | SQS queue URL |
| `SQS_REGION` | (falls back to `AWS_REGION`) | AWS region for SQS |
| `SQS_ROLE_ARN` | - | IAM role ARN for AssumeRole |

### GCP Pub/Sub

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `GCP_PROJECT_ID` | - | GCP project ID |
| `GCP_SUBSCRIPTION_ID` | - | Pub/Sub subscription ID |
| `GOOGLE_APPLICATION_CREDENTIALS` | - | Path to service account key file |

## Query Service Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `EXECUTION_ENVIRONMENT` | - | Execution environment ("local" or "kubernetes") |
| `POD_NAMESPACE` | - | Kubernetes namespace (auto-detected in K8s) |
| `POD_NAME` | - | Kubernetes pod name (used for ID generation) |
| `POD_IP` | - | Kubernetes pod IP (used for ID generation) |
| `WORKER_POD_LABEL_SELECTOR` | - | Label selector for query worker pods |
| `QUERY_WORKER_PORT` | - | Port for query worker service |

## Configuration Files

### Main Configuration File

Create a `config.yaml` file in the working directory:

```yaml
debug: false

kafka:
  brokers: ["localhost:9092"]
  sasl_enabled: false
  tls_enabled: false

metrics:
  ingestion:
    process_exemplars: true
  compaction:
    target_file_size_bytes: 1048576
  rollup:
    batch_limit: 100

batch:
  target_size_bytes: 104857600
  max_batch_size: 100

duckdb:
  memory_limit: 0
  s3_pool_size: 10

s3:
  region: "us-west-2"

admin:
  initial_api_key: ""
```

### Kafka Topics Override File

Create a topics configuration file (specified via `--kafka-topics-file` or `KAFKA_TOPICS_FILE`):

```yaml
version: 2
defaults:
  partitionCount: 16
  replicationFactor: 3
  options:
    retention.ms: "604800000"
workers:
  logs:
    partitionCount: 32
  metrics:
    partitionCount: 24
```

## Configuration Precedence

Configuration is loaded in the following order (later sources override earlier ones):

1. Default values (hardcoded)
2. Configuration file (`config.yaml`)
3. Environment variables with `LAKERUNNER_` prefix
4. Environment variables without prefix (for certain settings like database connections)
5. Command-line flags (where available)

## Environment Variable Naming

- Main config: `LAKERUNNER_` prefix + uppercase path with underscores
  - Example: `kafka.sasl_username` → `LAKERUNNER_KAFKA_SASL_USERNAME`
- Database connections: Direct prefix without `LAKERUNNER_`
  - Example: `LRDB_HOST`, `CONFIGDB_USER`
- Cloud providers: Often support both prefixed and unprefixed
  - Example: `LAKERUNNER_S3_REGION` or `AWS_REGION`
