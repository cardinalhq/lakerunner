// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package filereader

import (
	"context"
	"testing"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// Pre-interned keys for benchmarks to avoid measuring key creation overhead
var (
	benchKeyTimestamp   = wkk.NewRowKey("timestamp")
	benchKeyValue       = wkk.NewRowKey("value")
	benchKeyMetricName  = wkk.NewRowKey("metric_name")
	benchKeyServiceName = wkk.NewRowKey("service_name")
	benchKeyHostname    = wkk.NewRowKey("hostname")
	benchKeyEnv         = wkk.NewRowKey("env")
	benchKeyRegion      = wkk.NewRowKey("region")
	benchKeyCluster     = wkk.NewRowKey("cluster")
	benchKeyPod         = wkk.NewRowKey("pod")
	benchKeyContainer   = wkk.NewRowKey("container")
	benchKeyNamespace   = wkk.NewRowKey("namespace")
	benchKeyOrgID       = wkk.NewRowKey("organization_id")
	benchKeyCollectorID = wkk.NewRowKey("collector_id")
	benchKeyUnit        = wkk.NewRowKey("unit")
	benchKeyType        = wkk.NewRowKey("type")
	benchKeyNullCol     = wkk.NewRowKey("null_col")
)

// createBenchSchema creates a realistic metric schema for benchmarking
func createBenchSchema() *ReaderSchema {
	schema := NewReaderSchema()
	schema.AddColumn(benchKeyTimestamp, benchKeyTimestamp, DataTypeInt64, true)
	schema.AddColumn(benchKeyValue, benchKeyValue, DataTypeFloat64, true)
	schema.AddColumn(benchKeyMetricName, benchKeyMetricName, DataTypeString, true)
	schema.AddColumn(benchKeyServiceName, benchKeyServiceName, DataTypeString, true)
	schema.AddColumn(benchKeyHostname, benchKeyHostname, DataTypeString, true)
	schema.AddColumn(benchKeyEnv, benchKeyEnv, DataTypeString, true)
	schema.AddColumn(benchKeyRegion, benchKeyRegion, DataTypeString, true)
	schema.AddColumn(benchKeyCluster, benchKeyCluster, DataTypeString, true)
	schema.AddColumn(benchKeyPod, benchKeyPod, DataTypeString, true)
	schema.AddColumn(benchKeyContainer, benchKeyContainer, DataTypeString, true)
	schema.AddColumn(benchKeyNamespace, benchKeyNamespace, DataTypeString, true)
	schema.AddColumn(benchKeyOrgID, benchKeyOrgID, DataTypeString, true)
	schema.AddColumn(benchKeyCollectorID, benchKeyCollectorID, DataTypeString, true)
	schema.AddColumn(benchKeyUnit, benchKeyUnit, DataTypeString, true)
	schema.AddColumn(benchKeyType, benchKeyType, DataTypeString, true)
	schema.AddColumn(benchKeyNullCol, benchKeyNullCol, DataTypeString, false)
	return schema
}

// createBenchRowNoConversion creates a row where types already match schema (no conversion needed)
func createBenchRowNoConversion() pipeline.Row {
	row := pipeline.GetPooledRow()
	row[benchKeyTimestamp] = int64(1758397185000)
	row[benchKeyValue] = float64(42.5)
	row[benchKeyMetricName] = "http_requests_total"
	row[benchKeyServiceName] = "api-gateway"
	row[benchKeyHostname] = "prod-node-001.us-west-2.compute.internal"
	row[benchKeyEnv] = "production"
	row[benchKeyRegion] = "us-west-2"
	row[benchKeyCluster] = "prod-cluster-01"
	row[benchKeyPod] = "api-gateway-7f8d9c6b5-xk2j9"
	row[benchKeyContainer] = "api-gateway"
	row[benchKeyNamespace] = "default"
	row[benchKeyOrgID] = "org_12345678"
	row[benchKeyCollectorID] = "collector_abcdef"
	row[benchKeyUnit] = "count"
	row[benchKeyType] = "counter"
	row[benchKeyNullCol] = nil // null value to test deletion
	return row
}

// createBenchRowWithConversions creates a row that needs type conversions
func createBenchRowWithConversions() pipeline.Row {
	row := pipeline.GetPooledRow()
	row[benchKeyTimestamp] = int64(1758397185000)    // already int64, no conversion
	row[benchKeyValue] = int64(42)                   // needs int64 -> float64
	row[benchKeyMetricName] = "http_requests_total"  // already string
	row[benchKeyServiceName] = "api-gateway"         // already string
	row[benchKeyHostname] = "prod-node-001"          // already string
	row[benchKeyEnv] = "production"                  // already string
	row[benchKeyRegion] = "us-west-2"                // already string
	row[benchKeyCluster] = "prod-cluster-01"         // already string
	row[benchKeyPod] = "api-gateway-7f8d9c6b5-xk2j9" // already string
	row[benchKeyContainer] = "api-gateway"           // already string
	row[benchKeyNamespace] = "default"               // already string
	row[benchKeyOrgID] = "org_12345678"              // already string
	row[benchKeyCollectorID] = "collector_abcdef"    // already string
	row[benchKeyUnit] = "count"                      // already string
	row[benchKeyType] = "counter"                    // already string
	row[benchKeyNullCol] = nil                       // null value
	return row
}

// BenchmarkNormalizeRow_NoConversions benchmarks normalizeRow when types already match
func BenchmarkNormalizeRow_NoConversions(b *testing.B) {
	schema := createBenchSchema()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		row := createBenchRowNoConversion()
		_ = normalizeRow(ctx, row, schema)
		pipeline.ReturnPooledRow(row)
	}
}

// BenchmarkNormalizeRow_WithConversions benchmarks normalizeRow with type conversions
func BenchmarkNormalizeRow_WithConversions(b *testing.B) {
	schema := createBenchSchema()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		row := createBenchRowWithConversions()
		_ = normalizeRow(ctx, row, schema)
		pipeline.ReturnPooledRow(row)
	}
}

// BenchmarkNormalizeRow_ManyNulls benchmarks normalizeRow with many null values
func BenchmarkNormalizeRow_ManyNulls(b *testing.B) {
	schema := createBenchSchema()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		row := pipeline.GetPooledRow()
		row[benchKeyTimestamp] = int64(1758397185000)
		row[benchKeyValue] = float64(42.5)
		row[benchKeyMetricName] = "http_requests_total"
		row[benchKeyServiceName] = nil // null
		row[benchKeyHostname] = nil    // null
		row[benchKeyEnv] = nil         // null
		row[benchKeyRegion] = nil      // null
		row[benchKeyCluster] = nil     // null
		row[benchKeyPod] = nil         // null
		row[benchKeyContainer] = nil   // null
		row[benchKeyNamespace] = nil   // null
		row[benchKeyOrgID] = nil       // null
		row[benchKeyCollectorID] = nil // null
		row[benchKeyUnit] = nil        // null
		row[benchKeyType] = nil        // null
		row[benchKeyNullCol] = nil     // null

		_ = normalizeRow(ctx, row, schema)
		pipeline.ReturnPooledRow(row)
	}
}

// BenchmarkNormalizeRow_BatchSimulation simulates processing a batch of 1000 rows
func BenchmarkNormalizeRow_BatchSimulation(b *testing.B) {
	schema := createBenchSchema()
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Simulate a batch of 1000 rows
		for j := 0; j < 1000; j++ {
			row := createBenchRowNoConversion()
			_ = normalizeRow(ctx, row, schema)
			pipeline.ReturnPooledRow(row)
		}
	}
}
