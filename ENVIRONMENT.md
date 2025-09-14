# Environment Variable Changes

The following environment variables were added or removed in this branch. Update Helm charts and values accordingly.

## Added
- `LAKERUNNER_KAFKA_TOPIC_PREFIX` – optional override for Kafka topic prefix.

## Removed / Replaced
- `DEBUG` – use `LAKERUNNER_DEBUG` or the `debug` config setting.
- `LAKERUNNER_AZURE_EXTENSION` – Azure extension path now configured via `duckdb.azure_extension` in config.
- `LAKERUNNER_FLY_BROKERS` – Kafka brokers now set via `kafka.brokers` in config.
- `LAKERUNNER_INITITAL_ADMIN_API_KEY` – initial admin key now specified via `admin.initial_api_key` in config.

