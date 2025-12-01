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
	"errors"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// ErrRowNormalization is a sentinel error indicating row normalization failed.
// Use errors.Is(err, ErrRowNormalization) to check for this error.
// Use errors.As(err, &RowNormalizationError{}) to extract details.
var ErrRowNormalization = errors.New("row normalization failed")

// RowNormalizationError represents an error that occurred while normalizing a single row.
// These errors are row-specific and indicate data quality issues rather than systemic failures.
type RowNormalizationError struct {
	// Column is the name of the column that caused the error
	Column string
	// Reason describes what went wrong
	Reason string
	// Err is the underlying error if any
	Err error
}

func (e *RowNormalizationError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: column %q: %s: %v", ErrRowNormalization, e.Column, e.Reason, e.Err)
	}
	return fmt.Sprintf("%s: column %q: %s", ErrRowNormalization, e.Column, e.Reason)
}

func (e *RowNormalizationError) Unwrap() error {
	return e.Err
}

func (e *RowNormalizationError) Is(target error) bool {
	return target == ErrRowNormalization
}

// DataType represents the type of data in a column.
type DataType int

const (
	DataTypeUnknown DataType = iota // Unknown/uninitialized type - should not be used
	DataTypeString
	DataTypeInt64
	DataTypeFloat64
	DataTypeBool
	DataTypeBytes
	DataTypeAny // For complex types (list, struct, map) that are passed through as-is
)

func (dt DataType) String() string {
	switch dt {
	case DataTypeUnknown:
		return "unknown"
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
	case DataTypeAny:
		return "any"
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
	columns       map[wkk.RowKey]*ColumnSchema
	columnNameMap map[wkk.RowKey]wkk.RowKey // new name -> original name mapping
}

// NewReaderSchema creates a new empty schema.
func NewReaderSchema() *ReaderSchema {
	return &ReaderSchema{
		columns:       make(map[wkk.RowKey]*ColumnSchema),
		columnNameMap: make(map[wkk.RowKey]wkk.RowKey),
	}
}

// AddColumn adds or updates a column in the schema with name mapping.
// The column is stored under the new (normalized) name.
// The mapping tracks the relationship between new and original names.
// For identity mappings (where new == original), pass the same name for both parameters.
func (s *ReaderSchema) AddColumn(newName, originalName wkk.RowKey, dataType DataType, hasNonNull bool) {
	if existing, ok := s.columns[newName]; ok {
		// Promote type if needed
		existing.DataType = promoteType(existing.DataType, dataType)
		existing.HasNonNull = existing.HasNonNull || hasNonNull
	} else {
		s.columns[newName] = &ColumnSchema{
			Name:       newName,
			DataType:   dataType,
			HasNonNull: hasNonNull,
		}
	}
	// Always store the mapping from new name to original name
	s.columnNameMap[newName] = originalName
}

// GetOriginalName returns the original name for a column, or the same name if no mapping exists.
func (s *ReaderSchema) GetOriginalName(newName wkk.RowKey) wkk.RowKey {
	if originalName, exists := s.columnNameMap[newName]; exists {
		return originalName
	}
	return newName
}

// GetAllOriginalNames returns a map of all new names to original names.
func (s *ReaderSchema) GetAllOriginalNames() map[wkk.RowKey]wkk.RowKey {
	// Return a copy to prevent external mutation
	result := make(map[wkk.RowKey]wkk.RowKey, len(s.columnNameMap))
	for k, v := range s.columnNameMap {
		result[k] = v
	}
	return result
}

// GetColumnType returns the data type for a column name.
func (s *ReaderSchema) GetColumnType(name string) DataType {
	key := wkk.NewRowKey(name)
	if col, ok := s.columns[key]; ok {
		return col.DataType
	}
	return DataTypeUnknown // Return Unknown if column not found
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

	// Any mixed with anything → Any (preserve passthrough behavior)
	if a == DataTypeAny || b == DataTypeAny {
		return DataTypeAny
	}

	// Default to string for unknown combinations
	return DataTypeString
}

// normalizeRow normalizes a row in-place according to the schema, performing type conversions
// and removing keys with null values.
//
// Returns a RowNormalizationError if:
// - A column exists in the row but not in the schema (indicates schema extraction bug)
// - Type conversion fails for a column value
//
// These errors indicate data quality issues and should cause the row to be rejected.
func normalizeRow(ctx context.Context, row pipeline.Row, schema *ReaderSchema) error {
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
			// Increment error counter and return error
			columnName := wkk.RowKeyValue(key)
			schemaViolationsCounter.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("column", columnName),
			))
			return &RowNormalizationError{
				Column: columnName,
				Reason: "column not in schema",
			}
		}

		// Convert value to match schema type
		converted, err := convertValue(value, col.DataType)
		if err != nil {
			// Conversion failed - increment counter and return error
			columnName := wkk.RowKeyValue(key)
			typeConversionFailedCounter.Add(ctx, 1, otelmetric.WithAttributes(
				attribute.String("column", columnName),
				attribute.String("target_type", col.DataType.String()),
			))
			return &RowNormalizationError{
				Column: columnName,
				Reason: fmt.Sprintf("type conversion to %s failed", col.DataType.String()),
				Err:    err,
			}
		}

		// Update in-place
		row[key] = converted
	}

	// Remove null keys and schema violations
	for _, key := range keysToDelete {
		delete(row, key)
	}

	return nil
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
	case arrow.Timestamp:
		// Arrow timestamps need unit normalization to milliseconds
		return normalizeTimestampValue(int64(v)), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", value)
	}
}

// normalizeTimestampValue converts timestamps to milliseconds using heuristics.
// Detects nanoseconds (> 1e15), seconds (< 2e9), and assumes milliseconds otherwise.
// This matches the logic in csv_log_translator.go.
func normalizeTimestampValue(ts int64) int64 {
	if ts <= 0 {
		return 0
	}

	// Nanoseconds (after year 2001 in nanoseconds: 1e15)
	// Example: 1758397185000000000 (2025-11-30) > 1e15
	if ts > 1e15 {
		return ts / 1e6
	}

	// Seconds (before year 2033 in milliseconds: 2e9)
	// Example: 1758397185 (2025-11-30) < 2e9
	// Values < 2e9 are treated as seconds and converted to milliseconds
	if ts < 2e9 {
		return ts * 1000
	}

	// Assume milliseconds
	// Example: 1758397185000 (2025-11-30)
	return ts
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
	case arrow.Timestamp:
		// Arrow timestamps need unit normalization to milliseconds
		return float64(normalizeTimestampValue(int64(v))), nil
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
