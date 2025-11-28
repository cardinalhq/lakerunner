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
