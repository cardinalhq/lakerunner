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
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// extractSchemaFromOTELLogs scans all OTEL logs and extracts the complete schema.
func extractSchemaFromOTELLogs(logs *plog.Logs) *ReaderSchema {
	schema := NewReaderSchema()

	// Add core log fields that are always present (with identity mappings)
	schema.AddColumn(wkk.RowKeyCMessage, wkk.RowKeyCMessage, DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCTimestamp, wkk.RowKeyCTimestamp, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCTsns, wkk.RowKeyCTsns, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCLevel, wkk.RowKeyCLevel, DataTypeString, true)

	// Iterate through all logs to discover all attributes and their types
	for i := range logs.ResourceLogs().Len() {
		rl := logs.ResourceLogs().At(i)

		// Extract resource attributes
		rl.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
			dataType := otelValueTypeToDataType(v.Type())
			key := prefixAttributeRowKey(name, "resource")
			// Add with identity mapping (prefixed name -> prefixed name)
			schema.AddColumn(key, key, dataType, true)
			return true
		})

		for j := range rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(j)

			// Extract scope attributes
			sl.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
				dataType := otelValueTypeToDataType(v.Type())
				key := prefixAttributeRowKey(name, "scope")
				// Add with identity mapping (prefixed name -> prefixed name)
				schema.AddColumn(key, key, dataType, true)
				return true
			})

			for k := range sl.LogRecords().Len() {
				logRecord := sl.LogRecords().At(k)

				// Extract log record attributes
				logRecord.Attributes().Range(func(name string, v pcommon.Value) bool {
					dataType := otelValueTypeToDataType(v.Type())
					key := prefixAttributeRowKey(name, "attr")
					// Add with identity mapping (prefixed name -> prefixed name)
					schema.AddColumn(key, key, dataType, true)
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
	schema.AddColumn(wkk.RowKeySpanTraceID, wkk.RowKeySpanTraceID, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanID, wkk.RowKeySpanID, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanParentSpanID, wkk.RowKeySpanParentSpanID, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanName, wkk.RowKeySpanName, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanKind, wkk.RowKeySpanKind, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanStatusCode, wkk.RowKeySpanStatusCode, DataTypeString, true)
	schema.AddColumn(wkk.RowKeySpanStatusMessage, wkk.RowKeySpanStatusMessage, DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCTimestamp, wkk.RowKeyCTimestamp, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCTsns, wkk.RowKeyCTsns, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeySpanEndTimestamp, wkk.RowKeySpanEndTimestamp, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeySpanDuration, wkk.RowKeySpanDuration, DataTypeInt64, true)
	schema.AddColumn(wkk.RowKeyCFingerprint, wkk.RowKeyCFingerprint, DataTypeInt64, true)

	// Scan all spans to discover attributes
	for i := 0; i < traces.ResourceSpans().Len(); i++ {
		rs := traces.ResourceSpans().At(i)

		// Resource attributes
		rs.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
			key := prefixAttributeRowKey(name, "resource")
			dataType := otelValueTypeToDataType(v.Type())
			// Identity mapping for dynamic attribute keys
			schema.AddColumn(key, key, dataType, true)
			return true
		})

		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)

			// Scope attributes
			ss.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
				key := prefixAttributeRowKey(name, "scope")
				dataType := otelValueTypeToDataType(v.Type())
				schema.AddColumn(key, key, dataType, true)
				return true
			})

			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)

				// Span attributes
				span.Attributes().Range(func(name string, v pcommon.Value) bool {
					key := prefixAttributeRowKey(name, "attr")
					dataType := otelValueTypeToDataType(v.Type())
					schema.AddColumn(key, key, dataType, true)
					return true
				})
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
		// Convert bools to strings in schema because otelValueToGoValue() uses AsString()
		// to avoid panics on malformed OTEL data where type tags don't match actual values
		return DataTypeString
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
