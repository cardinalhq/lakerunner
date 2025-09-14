# Environment Variable Changes

The following environment variables were added or removed in this branch. Update Helm charts and values accordingly.

## Added
- `LAKERUNNER_KAFKA_TOPIC_PREFIX` – optional override for Kafka topic prefix.

```yaml
# config.yaml
kafka_topics:
  topicPrefix: lakerunner
```

```bash
# override
LAKERUNNER_KAFKA_TOPIC_PREFIX=my-prefix
```

## Removed / Replaced

### `DEBUG`
Use `LAKERUNNER_DEBUG` or the `debug` config setting.

```yaml
# config.yaml
debug: true
```

```bash
LAKERUNNER_DEBUG=true
```

### `LAKERUNNER_AZURE_EXTENSION`
Azure extension path now configured via `duckdb.azure_extension` in config.

```yaml
# config.yaml
duckdb:
  azure_extension: /opt/duckdb/azure.duckdb_extension
```

```bash
LAKERUNNER_DUCKDB_AZURE_EXTENSION=/opt/duckdb/azure.duckdb_extension
```

### `LAKERUNNER_FLY_BROKERS`
Kafka brokers now set via `kafka.brokers` in config.

```yaml
# config.yaml
kafka:
  brokers:
    - broker1:9092
    - broker2:9092
```

```bash
LAKERUNNER_KAFKA_BROKERS=broker1:9092,broker2:9092
```

### `LAKERUNNER_INITIAL_ADMIN_API_KEY`
Initial admin key now specified via `admin.initial_api_key` in config.

```yaml
# config.yaml
admin:
  initial_api_key: supersecret
```

```bash
LAKERUNNER_ADMIN_INITIAL_API_KEY=supersecret
```

