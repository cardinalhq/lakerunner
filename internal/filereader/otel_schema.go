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

package filereader

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// extractSchemaFromOTELLogs scans all OTEL logs and extracts the complete schema.
func extractSchemaFromOTELLogs(logs *plog.Logs) *ReaderSchema {
	schema := NewReaderSchema()

	// Add core log fields that are always present
	schema.AddColumn(wkk.RowKeyCMessage, DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCTimestamp, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCTsns, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCLevel, DataTypeString, true)

	// Iterate through all logs to discover all attributes and their types
	for i := range logs.ResourceLogs().Len() {
		rl := logs.ResourceLogs().At(i)

		// Extract resource attributes
		rl.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
			dataType := otelValueTypeToDataType(v.Type())
			key := prefixAttributeRowKey(name, "resource")
			schema.AddColumn(key, dataType, true)
			return true
		})

		for j := range rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(j)

			// Extract scope attributes
			sl.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
				dataType := otelValueTypeToDataType(v.Type())
				key := prefixAttributeRowKey(name, "scope")
				schema.AddColumn(key, dataType, true)
				return true
			})

			for k := range sl.LogRecords().Len() {
				logRecord := sl.LogRecords().At(k)

				// Extract log record attributes
				logRecord.Attributes().Range(func(name string, v pcommon.Value) bool {
					dataType := otelValueTypeToDataType(v.Type())
					key := prefixAttributeRowKey(name, "attr")
					schema.AddColumn(key, dataType, true)
					return true
				})
			}
		}
	}

	return schema
}

// extractSchemaFromOTELTraces scans all OTEL traces and extracts the complete schema.
func extractSchemaFromOTELTraces(traces *ptrace.Traces) *ReaderSchema {
	schema := NewReaderSchema()

	// Add core span fields that are always present
	schema.AddColumn(wkk.RowKeySpanTraceID, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanID, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanParentSpanID, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanName, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanKind, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanStatusCode, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanStatusMessage, DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCTimestamp, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCTsns, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeySpanEndTimestamp, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeySpanDuration, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCFingerprint, DataTypeInt64, true)

	// Scan all spans to discover attributes
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)

		// Resource attributes
		rs.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
			key := prefixAttributeRowKey(name, "resource")
			dataType := otelValueTypeToDataType(v.Type())
			schema.AddColumn(key, dataType, true)
			return true
		})

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)

			// Scope attributes
			ss.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
				key := prefixAttributeRowKey(name, "scope")
				dataType := otelValueTypeToDataType(v.Type())
				schema.AddColumn(key, dataType, true)
				return true
			})

			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)

				// Span attributes
				span.Attributes().Range(func(name string, v pcommon.Value) bool {
					key := prefixAttributeRowKey(name, "attr")
					dataType := otelValueTypeToDataType(v.Type())
					schema.AddColumn(key, dataType, true)
					return true
				})
			}
		}
	}

	return schema
}

// extractSchemaFromOTELMetrics scans all OTEL metrics and extracts the complete schema.
func extractSchemaFromOTELMetrics(metrics *pmetric.Metrics) *ReaderSchema {
	schema := NewReaderSchema()

	// Add core metric fields that are always present
	schema.AddColumn(wkk.RowKeyCName, DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCTID, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCTimestamp, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCTsns, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCMetricType, DataTypeString, true)

	// Rollup fields
	schema.AddColumn(wkk.RowKeySketch, DataTypeBytes, true)
	schema.AddColumn(wkk.RowKeyRollupAvg, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupMax, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupMin, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupCount, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupSum, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupP25, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupP50, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupP75, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupP90, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupP95, DataTypeFloat64, true)
	schema.AddColumn(wkk.RowKeyRollupP99, DataTypeFloat64, true)

	// Scan all metrics to discover attributes
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		rm := metrics.ResourceMetrics().At(i)

		// Resource attributes
		rm.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
			key := prefixAttributeRowKey(name, "resource")
			dataType := otelValueTypeToDataType(v.Type())
			schema.AddColumn(key, dataType, true)
			return true
		})

		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)

			// Scope attributes
			sm.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
				key := prefixAttributeRowKey(name, "scope")
				dataType := otelValueTypeToDataType(v.Type())
				schema.AddColumn(key, dataType, true)
				return true
			})

			for k := 0; k < sm.Metrics().Len(); k++ {
				metric := sm.Metrics().At(k)

				// Scan datapoints for attributes (different datapoint types)
				switch metric.Type() {
				case pmetric.MetricTypeGauge:
					for l := 0; l < metric.Gauge().DataPoints().Len(); l++ {
						dp := metric.Gauge().DataPoints().At(l)
						dp.Attributes().Range(func(name string, v pcommon.Value) bool {
							key := prefixAttributeRowKey(name, "attr")
							dataType := otelValueTypeToDataType(v.Type())
							schema.AddColumn(key, dataType, true)
							return true
						})
					}
				case pmetric.MetricTypeSum:
					for l := 0; l < metric.Sum().DataPoints().Len(); l++ {
						dp := metric.Sum().DataPoints().At(l)
						dp.Attributes().Range(func(name string, v pcommon.Value) bool {
							key := prefixAttributeRowKey(name, "attr")
							dataType := otelValueTypeToDataType(v.Type())
							schema.AddColumn(key, dataType, true)
							return true
						})
					}
				case pmetric.MetricTypeHistogram:
					for l := 0; l < metric.Histogram().DataPoints().Len(); l++ {
						dp := metric.Histogram().DataPoints().At(l)
						dp.Attributes().Range(func(name string, v pcommon.Value) bool {
							key := prefixAttributeRowKey(name, "attr")
							dataType := otelValueTypeToDataType(v.Type())
							schema.AddColumn(key, dataType, true)
							return true
						})
					}
				case pmetric.MetricTypeSummary:
					for l := 0; l < metric.Summary().DataPoints().Len(); l++ {
						dp := metric.Summary().DataPoints().At(l)
						dp.Attributes().Range(func(name string, v pcommon.Value) bool {
							key := prefixAttributeRowKey(name, "attr")
							dataType := otelValueTypeToDataType(v.Type())
							schema.AddColumn(key, dataType, true)
							return true
						})
					}
				case pmetric.MetricTypeExponentialHistogram:
					for l := 0; l < metric.ExponentialHistogram().DataPoints().Len(); l++ {
						dp := metric.ExponentialHistogram().DataPoints().At(l)
						dp.Attributes().Range(func(name string, v pcommon.Value) bool {
							key := prefixAttributeRowKey(name, "attr")
							dataType := otelValueTypeToDataType(v.Type())
							schema.AddColumn(key, dataType, true)
							return true
						})
					}
				}
			}
		}
	}

	return schema
}

// otelValueTypeToDataType converts OTEL pcommon.ValueType to our DataType.
func otelValueTypeToDataType(vt pcommon.ValueType) DataType {
	switch vt {
	case pcommon.ValueTypeStr:
		return DataTypeString
	case pcommon.ValueTypeInt:
		return DataTypeInt64
	case pcommon.ValueTypeDouble:
		return DataTypeFloat64
	case pcommon.ValueTypeBool:
		return DataTypeBool
	case pcommon.ValueTypeBytes:
		return DataTypeBytes
	case pcommon.ValueTypeMap:
		// Maps are converted to JSON strings
		return DataTypeString
	case pcommon.ValueTypeSlice:
		// Slices are converted to JSON strings
		return DataTypeString
	case pcommon.ValueTypeEmpty:
		// Empty values are treated as strings
		return DataTypeString
	default:
		// Unknown types default to string
		return DataTypeString
	}
}
