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
	// _cardinalhq_* keys (sorted alphabetically)
	RowKeyCCollectorID   = NewRowKey("_cardinalhq_collector_id")
	RowKeyCCustomerID    = NewRowKey("_cardinalhq_customer_id")
	RowKeyCFingerprint   = NewRowKey("_cardinalhq_fingerprint")
	RowKeyCID            = NewRowKey("_cardinalhq_id")
	RowKeyCMessage       = NewRowKey("_cardinalhq_message")
	RowKeyCMetricType    = NewRowKey("_cardinalhq_metric_type")
	RowKeyCName          = NewRowKey("_cardinalhq_name")
	RowKeyCSpanEvents    = NewRowKey("_cardinalhq_span_events")
	RowKeyCTelemetryType = NewRowKey("_cardinalhq_telemetry_type")
	RowKeyCTID           = NewRowKey("_cardinalhq_tid")
	RowKeyCTimestamp     = NewRowKey("_cardinalhq_timestamp")
	RowKeyCTsns          = NewRowKey("_cardinalhq_tsns")
	RowKeyCValue         = NewRowKey("_cardinalhq_value") // Deprecated

	// histogram handling (TODO: just make histograms)
	RowKeyCBucketBounds   = NewRowKey("_cardinalhq_bucket_bounds")
	RowKeyCNegativeCounts = NewRowKey("_cardinalhq_negative_counts")
	RowKeyCPositiveCounts = NewRowKey("_cardinalhq_positive_counts")
	RowKeyCCounts         = NewRowKey("_cardinalhq_counts")    // Deprecated
	RowKeyCSumValue       = NewRowKey("_cardinalhq_sum_value") // Deprecated

	// rollup_* keys (sorted alphabetically)
	RowKeyRollupAvg   = NewRowKey("rollup_avg")
	RowKeyRollupCount = NewRowKey("rollup_count")
	RowKeyRollupMax   = NewRowKey("rollup_max")
	RowKeyRollupMin   = NewRowKey("rollup_min")
	RowKeyRollupP25   = NewRowKey("rollup_p25")
	RowKeyRollupP50   = NewRowKey("rollup_p50")
	RowKeyRollupP75   = NewRowKey("rollup_p75")
	RowKeyRollupP90   = NewRowKey("rollup_p90")
	RowKeyRollupP95   = NewRowKey("rollup_p95")
	RowKeyRollupP99   = NewRowKey("rollup_p99")
	RowKeyRollupSum   = NewRowKey("rollup_sum")

	// sketch key
	RowKeySketch = NewRowKey("sketch")
)
