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

package wkk

import (
	"unique"
	"unsafe"
)

type rowkey string

type RowKey = unique.Handle[rowkey]

func NewRowKey(s string) RowKey {
	return unique.Make(rowkey(s))
}

func RowKeyValue(rk RowKey) string {
	return string(rk.Value())
}

// commonKeys maps common field names to pre-allocated RowKeys
var commonKeys = map[string]RowKey{
	// Core telemetry fields
	"timestamp":          unique.Make(rowkey("timestamp")),
	"observed_timestamp": unique.Make(rowkey("observed_timestamp")),
	"trace_id":           unique.Make(rowkey("trace_id")),
	"span_id":            unique.Make(rowkey("span_id")),
	"name":               unique.Make(rowkey("name")),
	"kind":               unique.Make(rowkey("kind")),
	"start_timestamp":    unique.Make(rowkey("start_timestamp")),
	"end_timestamp":      unique.Make(rowkey("end_timestamp")),
	"status_code":        unique.Make(rowkey("status_code")),
	"severity_text":      unique.Make(rowkey("severity_text")),
	"body":               unique.Make(rowkey("body")),

	// Resource fields
	"resource_service_name":    unique.Make(rowkey("resource_service_name")),
	"resource_service_version": unique.Make(rowkey("resource_service_version")),
	"resource_deployment_env":  unique.Make(rowkey("resource_deployment_env")),
	"resource_bucket_name":     unique.Make(rowkey("resource_bucket_name")),
	"resource_file_name":       unique.Make(rowkey("resource_file_name")),
	"resource_file_type":       unique.Make(rowkey("resource_file_type")),

	// Scope fields
	"scope_scope_type": unique.Make(rowkey("scope_scope_type")),

	// Span attributes
	"span_http_method":      unique.Make(rowkey("span_http_method")),
	"span_http_status_code": unique.Make(rowkey("span_http_status_code")),
	"span_db_system":        unique.Make(rowkey("span_db_system")),
	"span_db_operation":     unique.Make(rowkey("span_db_operation")),
	"span_component":        unique.Make(rowkey("span_component")),
	"span_record_count":     unique.Make(rowkey("span_record_count")),

	// Common metric/log fields
	"metric":      unique.Make(rowkey("metric")),
	"value":       unique.Make(rowkey("value")),
	"environment": unique.Make(rowkey("environment")),
	"service":     unique.Make(rowkey("service")),
	"version":     unique.Make(rowkey("version")),
	"host":        unique.Make(rowkey("host")),
	"unit":        unique.Make(rowkey("unit")),
	"type":        unique.Make(rowkey("type")),
	"description": unique.Make(rowkey("description")),
}

// NewRowKeyFromBytes creates a RowKey from bytes without string allocation for common keys
func NewRowKeyFromBytes(keyBytes []byte) RowKey {
	// Fast path: lookup common keys using unsafe string conversion (no allocation)
	keyStr := unsafe.String(unsafe.SliceData(keyBytes), len(keyBytes))
	if key, exists := commonKeys[keyStr]; exists {
		return key
	}

	// Slow path: allocate string for uncommon keys
	return unique.Make(rowkey(string(keyBytes)))
}

var (
	// RowKeyCCollectorID: "_cardinalhq_collector_id"
	RowKeyCCollectorID = NewRowKey("_cardinalhq_collector_id")

	// RowKeyCCustomerID: "_cardinalhq_customer_id"
	RowKeyCCustomerID = NewRowKey("_cardinalhq_customer_id")

	// RowKeyCFingerprint: "_cardinalhq_fingerprint"
	RowKeyCFingerprint = NewRowKey("_cardinalhq_fingerprint")

	// RowKeyCLevel: "_cardinalhq_level"
	RowKeyCLevel = NewRowKey("_cardinalhq_level")

	// RowKeyCMessage: "_cardinalhq_message"
	RowKeyCMessage = NewRowKey("_cardinalhq_message")

	// RowKeyCMetricType: "_cardinalhq_metric_type"
	RowKeyCMetricType = NewRowKey("_cardinalhq_metric_type")

	// RowKeyCName: "_cardinalhq_name"
	RowKeyCName = NewRowKey("_cardinalhq_name")

	// RowKeyCTelemetryType: "_cardinalhq_telemetry_type"
	RowKeyCTelemetryType = NewRowKey("_cardinalhq_telemetry_type")

	// RowKeyCTID: "_cardinalhq_tid"
	RowKeyCTID = NewRowKey("_cardinalhq_tid")

	// RowKeyCTimestamp: "_cardinalhq_timestamp"
	RowKeyCTimestamp = NewRowKey("_cardinalhq_timestamp")

	// RowKeyCTsns: "_cardinalhq_tsns" (nanoseconds)
	RowKeyCTsns = NewRowKey("_cardinalhq_tsns")

	// RowKeyCValue: "_cardinalhq_value" (DEPRECATED)
	RowKeyCValue = NewRowKey("_cardinalhq_value") // Deprecated

	// RowKeyRollupAvg: "rollup_avg"
	RowKeyRollupAvg = NewRowKey("rollup_avg")

	// RowKeyRollupCount: "rollup_count"
	RowKeyRollupCount = NewRowKey("rollup_count")

	// RowKeyRollupMax: "rollup_max"
	RowKeyRollupMax = NewRowKey("rollup_max")

	// RowKeyRollupMin: "rollup_min"
	RowKeyRollupMin = NewRowKey("rollup_min")

	// RowKeyRollupP25: "rollup_p25" (25th percentile)
	RowKeyRollupP25 = NewRowKey("rollup_p25")

	// RowKeyRollupP50: "rollup_p50" (50th percentile/median)
	RowKeyRollupP50 = NewRowKey("rollup_p50")

	// RowKeyRollupP75: "rollup_p75" (75th percentile)
	RowKeyRollupP75 = NewRowKey("rollup_p75")

	// RowKeyRollupP90: "rollup_p90" (90th percentile)
	RowKeyRollupP90 = NewRowKey("rollup_p90")

	// RowKeyRollupP95: "rollup_p95" (95th percentile)
	RowKeyRollupP95 = NewRowKey("rollup_p95")

	// RowKeyRollupP99: "rollup_p99" (99th percentile)
	RowKeyRollupP99 = NewRowKey("rollup_p99")

	// RowKeyRollupSum: "rollup_sum"
	RowKeyRollupSum = NewRowKey("rollup_sum")

	// RowKeySketch: "sketch"
	RowKeySketch = NewRowKey("sketch")

	// RowKeyResourceBucketName: "resource_bucket_name"
	RowKeyResourceBucketName = NewRowKey("resource_bucket_name")

	// RowKeyResourceFileName: "resource_file_name"
	RowKeyResourceFileName = NewRowKey("resource_file_name")

	// RowKeyResourceFile: "resource_file"
	RowKeyResourceFile = NewRowKey("resource_file")

	// RowKeyResourceFileType: "resource_file_type"
	RowKeyResourceFileType = NewRowKey("resource_file_type")
)
