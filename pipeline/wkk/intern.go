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
	// RowKeyCCollectorID: "chq_collector_id"
	RowKeyCCollectorID = NewRowKey("chq_collector_id")

	// RowKeyCCustomerID: "chq_customer_id"
	RowKeyCCustomerID = NewRowKey("chq_customer_id")

	// RowKeyCFingerprint: "chq_fingerprint"
	RowKeyCFingerprint = NewRowKey("chq_fingerprint")

	// RowKeyCLevel: "log_level"
	RowKeyCLevel = NewRowKey("log_level")

	// RowKeyCMessage: "log_message"
	RowKeyCMessage = NewRowKey("log_message")

	// RowKeyCMetricType: "chq_metric_type"
	RowKeyCMetricType = NewRowKey("chq_metric_type")

	// RowKeyCName: "metric_name"
	RowKeyCName = NewRowKey("metric_name")

	// RowKeyCTelemetryType: "chq_telemetry_type"
	RowKeyCTelemetryType = NewRowKey("chq_telemetry_type")

	// RowKeyCTID: "chq_tid"
	RowKeyCTID = NewRowKey("chq_tid")

	// RowKeyCTimestamp: "chq_timestamp"
	RowKeyCTimestamp = NewRowKey("chq_timestamp")

	// RowKeyCTsns: "chq_tsns" (nanoseconds)
	RowKeyCTsns = NewRowKey("chq_tsns")

	// RowKeyCValue: "chq_value" (DEPRECATED)
	RowKeyCValue = NewRowKey("chq_value") // Deprecated

	// RowKeyRollupAvg: "chq_rollup_avg"
	RowKeyRollupAvg = NewRowKey("chq_rollup_avg")

	// RowKeyRollupCount: "chq_rollup_count"
	RowKeyRollupCount = NewRowKey("chq_rollup_count")

	// RowKeyRollupMax: "chq_rollup_max"
	RowKeyRollupMax = NewRowKey("chq_rollup_max")

	// RowKeyRollupMin: "chq_rollup_min"
	RowKeyRollupMin = NewRowKey("chq_rollup_min")

	// RowKeyRollupP25: "chq_rollup_p25" (25th percentile)
	RowKeyRollupP25 = NewRowKey("chq_rollup_p25")

	// RowKeyRollupP50: "chq_rollup_p50" (50th percentile/median)
	RowKeyRollupP50 = NewRowKey("chq_rollup_p50")

	// RowKeyRollupP75: "chq_rollup_p75" (75th percentile)
	RowKeyRollupP75 = NewRowKey("chq_rollup_p75")

	// RowKeyRollupP90: "chq_rollup_p90" (90th percentile)
	RowKeyRollupP90 = NewRowKey("chq_rollup_p90")

	// RowKeyRollupP95: "chq_rollup_p95" (95th percentile)
	RowKeyRollupP95 = NewRowKey("chq_rollup_p95")

	// RowKeyRollupP99: "chq_rollup_p99" (99th percentile)
	RowKeyRollupP99 = NewRowKey("chq_rollup_p99")

	// RowKeyRollupSum: "chq_rollup_sum"
	RowKeyRollupSum = NewRowKey("chq_rollup_sum")

	// RowKeySketch: "chq_sketch"
	RowKeySketch = NewRowKey("chq_sketch")

	// RowKeyResourceBucketName: "resource_bucket_name"
	RowKeyResourceBucketName = NewRowKey("resource_bucket_name")

	// RowKeyResourceFileName: "resource_file_name"
	RowKeyResourceFileName = NewRowKey("resource_file_name")

	// RowKeyResourceFile: "resource_file"
	RowKeyResourceFile = NewRowKey("resource_file")

	// RowKeyResourceFileType: "resource_file_type"
	RowKeyResourceFileType = NewRowKey("resource_file_type")

	// RowKeyResourceCustomerDomain: "resource_customer_domain"
	RowKeyResourceCustomerDomain = NewRowKey("resource_customer_domain")

	// Span field keys
	// RowKeySpanTraceID: "span_trace_id"
	RowKeySpanTraceID = NewRowKey("span_trace_id")

	// RowKeySpanID: "span_id"
	RowKeySpanID = NewRowKey("span_id")

	// RowKeySpanParentSpanID: "span_parent_span_id"
	RowKeySpanParentSpanID = NewRowKey("span_parent_span_id")

	// RowKeySpanName: "span_name"
	RowKeySpanName = NewRowKey("span_name")

	// RowKeySpanKind: "span_kind"
	RowKeySpanKind = NewRowKey("span_kind")

	// RowKeySpanStatusCode: "span_status_code"
	RowKeySpanStatusCode = NewRowKey("span_status_code")

	// RowKeySpanStatusMessage: "span_status_message"
	RowKeySpanStatusMessage = NewRowKey("span_status_message")

	// RowKeySpanEndTimestamp: "span_end_timestamp"
	RowKeySpanEndTimestamp = NewRowKey("span_end_timestamp")

	// RowKeySpanDuration: "span_duration"
	RowKeySpanDuration = NewRowKey("span_duration")

	// Resource attribute keys (with k8s prefix as seen in semconv)
	// RowKeyResourceK8sClusterName: "resource_k8s_cluster_name"
	RowKeyResourceK8sClusterName = NewRowKey("resource_k8s_cluster_name")

	// RowKeyResourceK8sNamespaceName: "resource_k8s_namespace_name"
	RowKeyResourceK8sNamespaceName = NewRowKey("resource_k8s_namespace_name")

	// RowKeyResourceServiceName: "resource_service_name"
	RowKeyResourceServiceName = NewRowKey("resource_service_name")

	// Span attribute keys (with attr_ prefix)
	// RowKeyAttrDBSystemName: "attr_db_system_name"
	RowKeyAttrDBSystemName = NewRowKey("attr_db_system_name")

	// RowKeyAttrMessagingSystem: "attr_messaging_system"
	RowKeyAttrMessagingSystem = NewRowKey("attr_messaging_system")

	// RowKeyAttrHTTPRequestMethod: "attr_http_request_method"
	RowKeyAttrHTTPRequestMethod = NewRowKey("attr_http_request_method")

	// RowKeyAttrMessagingOperationType: "attr_messaging_operation_type"
	RowKeyAttrMessagingOperationType = NewRowKey("attr_messaging_operation_type")

	// RowKeyAttrMessagingDestinationName: "attr_messaging_destination_name"
	RowKeyAttrMessagingDestinationName = NewRowKey("attr_messaging_destination_name")

	// RowKeyAttrDBNamespace: "attr_db_namespace"
	RowKeyAttrDBNamespace = NewRowKey("attr_db_namespace")

	// RowKeyAttrDBOperationName: "attr_db_operation_name"
	RowKeyAttrDBOperationName = NewRowKey("attr_db_operation_name")

	// RowKeyAttrServerAddress: "attr_server_address"
	RowKeyAttrServerAddress = NewRowKey("attr_server_address")

	// RowKeyAttrDBCollectionName: "attr_db_collection_name"
	RowKeyAttrDBCollectionName = NewRowKey("attr_db_collection_name")

	// RowKeyAttrURLTemplate: "attr_url_template"
	RowKeyAttrURLTemplate = NewRowKey("attr_url_template")
)
