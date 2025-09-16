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
	"resource.service.name":    unique.Make(rowkey("resource.service.name")),
	"resource.service.version": unique.Make(rowkey("resource.service.version")),
	"resource.deployment.env":  unique.Make(rowkey("resource.deployment.env")),
	"resource.bucket.name":     unique.Make(rowkey("resource.bucket.name")),
	"resource.file.name":       unique.Make(rowkey("resource.file.name")),
	"resource.file.type":       unique.Make(rowkey("resource.file.type")),

	// Scope fields
	"scope.scope.type": unique.Make(rowkey("scope.scope.type")),

	// Span attributes
	"span.http.method":      unique.Make(rowkey("span.http.method")),
	"span.http.status_code": unique.Make(rowkey("span.http.status_code")),
	"span.db.system":        unique.Make(rowkey("span.db.system")),
	"span.db.operation":     unique.Make(rowkey("span.db.operation")),
	"span.component":        unique.Make(rowkey("span.component")),
	"span.record.count":     unique.Make(rowkey("span.record.count")),

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

	// Test fields (for testing)
	"test":            unique.Make(rowkey("test")),
	"test.translator": unique.Make(rowkey("test.translator")),
	"id":              unique.Make(rowkey("id")),
	"age":             unique.Make(rowkey("age")),
	"city":            unique.Make(rowkey("city")),
	"key":             unique.Make(rowkey("key")),
	"tag":             unique.Make(rowkey("tag")),
	"batch":           unique.Make(rowkey("batch")),
	"final":           unique.Make(rowkey("final")),
	"eof":             unique.Make(rowkey("eof")),
	"good":            unique.Make(rowkey("good")),
	"bad":             unique.Make(rowkey("bad")),
	"more":            unique.Make(rowkey("more")),
	"row":             unique.Make(rowkey("row")),
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
	// _cardinalhq.* keys (sorted alphabetically)
	RowKeyCCollectorID   = NewRowKey("_cardinalhq.collector_id")
	RowKeyCCustomerID    = NewRowKey("_cardinalhq.customer_id")
	RowKeyCFingerprint   = NewRowKey("_cardinalhq.fingerprint")
	RowKeyCID            = NewRowKey("_cardinalhq.id")
	RowKeyCMessage       = NewRowKey("_cardinalhq.message")
	RowKeyCMetricType    = NewRowKey("_cardinalhq.metric_type")
	RowKeyCName          = NewRowKey("_cardinalhq.name")
	RowKeyCSpanEvents    = NewRowKey("_cardinalhq.span_events")
	RowKeyCTelemetryType = NewRowKey("_cardinalhq.telemetry_type")
	RowKeyCTID           = NewRowKey("_cardinalhq.tid")
	RowKeyCTimestamp     = NewRowKey("_cardinalhq.timestamp")
	RowKeyCValue         = NewRowKey("_cardinalhq.value") // Deprecated

	// histogram handling (TODO: just make histograms)
	RowKeyCBucketBounds   = NewRowKey("_cardinalhq.bucket_bounds")
	RowKeyCNegativeCounts = NewRowKey("_cardinalhq.negative_counts")
	RowKeyCPositiveCounts = NewRowKey("_cardinalhq.positive_counts")
	RowKeyCCounts         = NewRowKey("_cardinalhq.counts")    // Deprecated
	RowKeyCSumValue       = NewRowKey("_cardinalhq.sum_value") // Deprecated

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
