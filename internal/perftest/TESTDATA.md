# Performance Test Data Sources

## Overview

Performance tests use real production-format data from a test account to ensure accurate measurements. Data is downloaded on-demand and NOT checked into the repository.

## Data Locations

### Raw Input Data (OTEL format)
- **S3 Path**: `s3://chq-saas-us-east-2-4d79e03f/otel-raw/65928f26-224b-4acb-8e57-9ee628164694/`
- **Format**: Raw OTEL logs (JSON.gz, Proto, or Parquet)
- **Use case**: Testing ingestion pipeline (download → read → process → write)

### Cooked Data (Processed Parquet)
- **S3 Path**: `s3://chq-saas-us-east-2-4d79e03f/db/65928f26-224b-4acb-8e57-9ee628164694/`
- **Format**: Optimized Parquet files
- **Use case**: Testing compaction pipeline (download → merge → write)

## Ground Rules

1. **READ-ONLY**: Make NO changes to cloud infrastructure
2. **S3 ONLY**: Access ONLY S3, no other cloud components
3. **NO COMMITS**: Downloaded data must NOT be checked into repository
4. **TEST ACCOUNT**: This is test account data, safe to use (no PII)

## Data Management

### Local Storage
- Test data stored in: `/tmp/lakerunner-perftest/`
- Raw data: `/tmp/lakerunner-perftest/raw/`
- Cooked data: `/tmp/lakerunner-perftest/cooked/`

### Download Scripts
Use `scripts/download-perf-testdata.sh` to fetch data for testing.

### .gitignore Entries
```
/tmp/
*.parquet
*.gz
**/perftest-data/
```

## Usage

```bash
# Download sample raw data for ingestion tests
./scripts/download-perf-testdata.sh raw 10

# Download sample cooked data for compaction tests
./scripts/download-perf-testdata.sh cooked 20

# Clean up test data
rm -rf /tmp/lakerunner-perftest/
```

## Test Data Characteristics

When selecting test files, consider:
- **File size distribution**: Mix of small (1MB), medium (10MB), large (100MB) files
- **Record count**: Varying densities to test different scenarios
- **Time distribution**: Both sorted and out-of-order timestamps
- **Cardinality**: High and low cardinality label sets

## AWS Credentials Required

Tests require AWS credentials with read access to:
- `s3://chq-saas-us-east-2-4d79e03f/otel-raw/*`
- `s3://chq-saas-us-east-2-4d79e03f/db/*`

Set via:
```bash
export AWS_ACCESS_KEY_ID=<key>
export AWS_SECRET_ACCESS_KEY=<secret>
export AWS_REGION=us-east-2
```

Or use AWS SSO/credential profiles.

## Future Considerations

If we need reproducible tests with frozen test data:
1. Create a separate `lakerunner-testdata` repository
2. Use Git LFS for large Parquet files
3. Reference specific commits in test code
4. Keep test data size under 1GB total
