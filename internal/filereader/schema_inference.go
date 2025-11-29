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
	"strconv"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// InferTypeFromValue determines the DataType from a Go value.
// This handles values from JSON unmarshaling or CSV parsing.
func InferTypeFromValue(value any) DataType {
	if value == nil {
		// Nil values don't provide type information
		return DataTypeAny
	}

	switch v := value.(type) {
	case bool:
		return DataTypeBool
	case int64:
		return DataTypeInt64
	case float64:
		// JSON unmarshals all numbers as float64
		// Check if it's actually an integer
		if v == float64(int64(v)) {
			return DataTypeInt64
		}
		return DataTypeFloat64
	case string:
		return DataTypeString
	case []byte:
		return DataTypeBytes
	case []any:
		// Arrays - use DataTypeAny since we'd need to inspect elements
		return DataTypeAny
	case map[string]any:
		// Objects - use DataTypeAny
		return DataTypeAny
	default:
		return DataTypeAny
	}
}

// InferTypeFromString attempts to parse a string and determine its type.
// Returns the inferred DataType and the parsed value.
// This is useful for CSV readers where all values start as strings.
func InferTypeFromString(s string) (DataType, any) {
	trimmed := s

	// Empty strings are strings
	if trimmed == "" {
		return DataTypeString, ""
	}

	// Try to parse as integer BEFORE bool
	// (ParseBool accepts "1" and "0" which we want to treat as integers)
	if i, err := strconv.ParseInt(trimmed, 10, 64); err == nil {
		return DataTypeInt64, i
	}

	// Try to parse as float
	if f, err := strconv.ParseFloat(trimmed, 64); err == nil {
		return DataTypeFloat64, f
	}

	// Try to parse as bool (true/false/t/f/T/F only, not "1"/"0")
	if b, err := strconv.ParseBool(trimmed); err == nil {
		return DataTypeBool, b
	}

	// Keep as string
	return DataTypeString, s
}

// SchemaBuilder helps build schemas by scanning data and tracking type promotions.
type SchemaBuilder struct {
	schema *ReaderSchema
}

// NewSchemaBuilder creates a new schema builder.
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{
		schema: NewReaderSchema(),
	}
}

// AddValue adds a value to the schema, inferring its type and promoting if needed.
func (sb *SchemaBuilder) AddValue(columnName string, value any) {
	if value == nil {
		// Nil values don't tell us about type, but do tell us the column exists
		// Only add if we haven't seen this column yet
		key := wkk.NewRowKey(columnName)
		if len(sb.schema.columns) == 0 || sb.schema.columns[key] == nil {
			sb.schema.AddColumn(key, DataTypeAny, false)
		}
		return
	}

	dataType := InferTypeFromValue(value)
	sb.schema.AddColumn(wkk.NewRowKey(columnName), dataType, true)
}

// AddStringValue parses a string value, infers its type, and adds to schema.
func (sb *SchemaBuilder) AddStringValue(columnName string, stringValue string) {
	dataType, _ := InferTypeFromString(stringValue)
	sb.schema.AddColumn(wkk.NewRowKey(columnName), dataType, true)
}

// Build returns the built schema.
func (sb *SchemaBuilder) Build() *ReaderSchema {
	return sb.schema
}
