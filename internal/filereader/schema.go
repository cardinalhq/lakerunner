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
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// DataType represents the type of data in a column.
type DataType int

const (
	DataTypeString DataType = iota
	DataTypeInt64
	DataTypeFloat64
	DataTypeBool
	DataTypeBytes
)

func (dt DataType) String() string {
	switch dt {
	case DataTypeString:
		return "string"
	case DataTypeInt64:
		return "int64"
	case DataTypeFloat64:
		return "float64"
	case DataTypeBool:
		return "bool"
	case DataTypeBytes:
		return "bytes"
	default:
		return "unknown"
	}
}

// ColumnSchema describes a single column in the schema.
type ColumnSchema struct {
	Name       wkk.RowKey
	DataType   DataType
	HasNonNull bool
}

// ReaderSchema represents the complete schema for a reader.
type ReaderSchema struct {
	columns map[wkk.RowKey]*ColumnSchema
}

// NewReaderSchema creates a new empty schema.
func NewReaderSchema() *ReaderSchema {
	return &ReaderSchema{
		columns: make(map[wkk.RowKey]*ColumnSchema),
	}
}

// AddColumn adds or updates a column in the schema.
func (s *ReaderSchema) AddColumn(name wkk.RowKey, dataType DataType, hasNonNull bool) {
	if existing, ok := s.columns[name]; ok {
		// Promote type if needed
		existing.DataType = promoteType(existing.DataType, dataType)
		existing.HasNonNull = existing.HasNonNull || hasNonNull
	} else {
		s.columns[name] = &ColumnSchema{
			Name:       name,
			DataType:   dataType,
			HasNonNull: hasNonNull,
		}
	}
}

// GetColumnType returns the data type for a column name.
func (s *ReaderSchema) GetColumnType(name string) DataType {
	key := wkk.NewRowKey(name)
	if col, ok := s.columns[key]; ok {
		return col.DataType
	}
	return DataTypeString // Default to string if not found
}

// HasColumn returns true if the schema has the specified column.
func (s *ReaderSchema) HasColumn(name string) bool {
	key := wkk.NewRowKey(name)
	_, ok := s.columns[key]
	return ok
}

// Columns returns all column schemas.
func (s *ReaderSchema) Columns() []*ColumnSchema {
	result := make([]*ColumnSchema, 0, len(s.columns))
	for _, col := range s.columns {
		result = append(result, col)
	}
	return result
}

// promoteType returns the promoted type when two types need to be merged.
// Type promotion rules from design doc:
// - int64 + int64 → int64
// - int64 + float64 → float64
// - int64 + string → string
// - int64 + bool → string
// - float64 + float64 → float64
// - float64 + string → string
// - float64 + bool → string
// - string + string → string
// - string + bool → string
// - bool + bool → bool
// - bytes + * → string
func promoteType(a, b DataType) DataType {
	if a == b {
		return a
	}

	// String is the most general type
	if a == DataTypeString || b == DataTypeString {
		return DataTypeString
	}

	// Bytes promotes to string with anything else
	if a == DataTypeBytes || b == DataTypeBytes {
		return DataTypeString
	}

	// Float64 is more general than int64
	if (a == DataTypeFloat64 && b == DataTypeInt64) ||
		(a == DataTypeInt64 && b == DataTypeFloat64) {
		return DataTypeFloat64
	}

	// Bool mixed with int64 or float64 → string
	if a == DataTypeBool || b == DataTypeBool {
		return DataTypeString
	}

	// Default to string for unknown combinations
	return DataTypeString
}

// normalizeRow normalizes a row in-place according to the schema, performing type conversions
// and removing keys with null values. Columns not in schema are dropped and counted as errors.
func normalizeRow(ctx context.Context, row pipeline.Row, schema *ReaderSchema) {
	// Track keys to delete (can't delete while iterating)
	var keysToDelete []wkk.RowKey

	for key, value := range row {
		if value == nil {
			// Mark null values for deletion
			keysToDelete = append(keysToDelete, key)
			continue
		}

		col, exists := schema.columns[key]
		if !exists {
			// Column not in schema - this is a bug in schema extraction
			// Drop the column and increment error counter
			keysToDelete = append(keysToDelete, key)
			schemaViolationsCounter.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("column", wkk.RowKeyValue(key)),
			))
			continue
		}

		// Convert value to match schema type
		converted, err := convertValue(value, col.DataType)
		if err != nil {
			// If conversion fails, drop the value (downstream consumers expect typed values)
			keysToDelete = append(keysToDelete, key)
			typeConversionFailedCounter.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("column", wkk.RowKeyValue(key)),
				attribute.String("target_type", col.DataType.String()),
			))
			continue
		}

		// Update in-place
		row[key] = converted
	}

	// Remove null keys and schema violations
	for _, key := range keysToDelete {
		delete(row, key)
	}
}

// convertValue converts a value to the target data type.
func convertValue(value any, targetType DataType) (any, error) {
	switch targetType {
	case DataTypeString:
		return convertToString(value), nil

	case DataTypeInt64:
		return convertToInt64(value)

	case DataTypeFloat64:
		return convertToFloat64(value)

	case DataTypeBool:
		return convertToBool(value)

	case DataTypeBytes:
		return convertToBytes(value)

	default:
		return value, nil
	}
}

// convertToString converts any value to string.
func convertToString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case int64:
		return fmt.Sprintf("%d", v)
	case int:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case []byte:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// convertToInt64 converts a value to int64.
func convertToInt64(value any) (int64, error) {
	switch v := value.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case float64:
		return int64(v), nil
	case string:
		var i int64
		_, err := fmt.Sscanf(v, "%d", &i)
		return i, err
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

// convertToFloat64 converts a value to float64.
func convertToFloat64(value any) (float64, error) {
	switch v := value.(type) {
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case int:
		return float64(v), nil
	case string:
		var f float64
		_, err := fmt.Sscanf(v, "%f", &f)
		return f, err
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", value)
	}
}

// convertToBool converts a value to bool.
func convertToBool(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		return v == "true" || v == "1", nil
	case int64:
		return v != 0, nil
	case int:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", value)
	}
}

// convertToBytes converts a value to bytes.
func convertToBytes(value any) ([]byte, error) {
	switch v := value.(type) {
	case []byte:
		return v, nil
	case string:
		return []byte(v), nil
	default:
		return nil, fmt.Errorf("cannot convert %T to bytes", value)
	}
}
