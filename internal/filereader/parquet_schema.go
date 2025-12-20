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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/file"
	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// extractSchemaFromParquetFile extracts schema from parquet file metadata.
func extractSchemaFromParquetFile(pf *parquetgo.File) *ReaderSchema {
	schema := NewReaderSchema()

	// Get parquet schema from file
	parquetSchema := pf.Schema()

	// Walk through all fields in the schema
	walkParquetSchema(schema, parquetSchema, "")

	return schema
}

// walkParquetSchema recursively walks the parquet schema and adds columns.
func walkParquetSchema(schema *ReaderSchema, node parquetgo.Node, prefix string) {
	// Check if this node has fields (Group or Schema)
	if !node.Leaf() {
		fields := node.Fields()
		for _, field := range fields {
			// Each field has a name
			fieldName := field.Name()

			// Build full path
			fullPath := fieldName
			if prefix != "" {
				fullPath = prefix + "." + fieldName
			}

			// Recursively walk this field
			walkParquetSchema(schema, field, fullPath)
		}
		return
	}

	// Leaf node - this is an actual column
	// prefix contains the full path to this column
	if prefix == "" {
		return
	}

	// Convert dots to underscores (same as reader does)
	columnName := wkk.NormalizeName(prefix)
	key := wkk.NewRowKeyFromBytes([]byte(columnName))

	// Map parquet type to our DataType
	dataType := parquetTypeToDataType(node.Type())
	schema.AddColumn(key, key, dataType, true)
}

// parquetTypeToDataType converts parquet type to our DataType.
func parquetTypeToDataType(ptype parquetgo.Type) DataType {
	// Check logical type first for ByteArray types
	if logicalType := ptype.LogicalType(); logicalType != nil {
		if logicalType.UTF8 != nil {
			return DataTypeString
		}
	}

	switch ptype.Kind() {
	case parquetgo.Boolean:
		return DataTypeBool
	case parquetgo.Int32, parquetgo.Int64:
		return DataTypeInt64
	case parquetgo.Float, parquetgo.Double:
		return DataTypeFloat64
	case parquetgo.ByteArray, parquetgo.FixedLenByteArray:
		// If no logical type was detected above, treat as bytes
		return DataTypeBytes
	default:
		// Default to string for unknown types
		return DataTypeString
	}
}

// extractParquetStatistics extracts column statistics from parquet file.
// Returns map of columnName -> hasNonNull.
func extractParquetStatistics(pf *file.Reader) map[string]bool {
	columnStats := make(map[string]bool)

	if pf == nil {
		return columnStats
	}

	// Check statistics across all row groups
	for rgIdx := 0; rgIdx < pf.NumRowGroups(); rgIdx++ {
		rgMeta := pf.MetaData().RowGroup(rgIdx)

		for colIdx := 0; colIdx < rgMeta.NumColumns(); colIdx++ {
			colChunk, err := rgMeta.ColumnChunk(colIdx)
			if err != nil {
				continue
			}

			// Get column name from schema
			schemaCol := pf.MetaData().Schema.Column(colIdx)
			colName := schemaCol.Name()

			// Skip if we already determined this column has non-null values
			if columnStats[colName] {
				continue
			}

			// Get statistics for this column chunk
			if stats, err := colChunk.Statistics(); err == nil && stats != nil {
				// If column has min/max set, it has non-null values
				if stats.HasMinMax() {
					columnStats[colName] = true
				} else if stats.HasNullCount() && stats.NullCount() < rgMeta.NumRows() {
					// Null count is less than total rows, so there are non-null values
					columnStats[colName] = true
				} else if stats.HasNullCount() && stats.NullCount() == rgMeta.NumRows() {
					// All values are null in this row group
					// Only set to false if not already set (another row group might have data)
					if _, exists := columnStats[colName]; !exists {
						columnStats[colName] = false
					}
				}
			}
		}
	}

	return columnStats
}

// arrowTypeToDataType converts Arrow type to our DataType.
func arrowTypeToDataType(atype arrow.DataType) DataType {
	switch atype.ID() {
	case arrow.BOOL:
		return DataTypeBool
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64,
		arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return DataTypeInt64
	case arrow.FLOAT32, arrow.FLOAT64:
		return DataTypeFloat64
	case arrow.STRING, arrow.LARGE_STRING:
		return DataTypeString
	case arrow.BINARY, arrow.LARGE_BINARY:
		return DataTypeBytes
	case arrow.TIMESTAMP, arrow.DATE32, arrow.DATE64, arrow.TIME32, arrow.TIME64:
		return DataTypeInt64
	case arrow.NULL:
		// NULL type means all values are null - we need parquet physical type
		// Return Any for now, caller should check parquet metadata
		return DataTypeAny
	default:
		// For complex types (list, struct, map) and unknown, use DataTypeAny
		// These will be passed through as-is without conversion
		return DataTypeAny
	}
}

// buildParquetTypeMap builds a map of underscored column path to DataType from parquet metadata.
// This uses the actual parquet physical types, not Arrow's inferred types.
// INT32 is promoted to Int64 for consistency.
// The key is the column path with dots replaced by underscores (e.g., "foo.bar.baz" -> "foo_bar_baz").
func buildParquetTypeMap(pf *file.Reader) map[string]DataType {
	typeMap := make(map[string]DataType)

	if pf == nil {
		return typeMap
	}

	schema := pf.MetaData().Schema
	for i := 0; i < schema.NumColumns(); i++ {
		col := schema.Column(i)
		// Use full path with underscores as key to avoid collisions between
		// top-level columns and nested columns with the same leaf name.
		// e.g., "error" vs "pushconfig_status.Response.error" become
		// "error" vs "pushconfig_status_Response_error"
		colKey := wkk.NormalizeName(col.Path())

		// Map parquet physical type to our DataType
		physicalType := col.PhysicalType()

		switch physicalType {
		case parquet.Types.Boolean:
			typeMap[colKey] = DataTypeBool
		case parquet.Types.Int32, parquet.Types.Int64:
			// Promote INT32 to Int64
			typeMap[colKey] = DataTypeInt64
		case parquet.Types.Float, parquet.Types.Double:
			typeMap[colKey] = DataTypeFloat64
		case parquet.Types.ByteArray, parquet.Types.FixedLenByteArray:
			// Check logical type to distinguish string from bytes
			logicalType := col.LogicalType()
			if logicalType != nil && logicalType.String() == "String" {
				typeMap[colKey] = DataTypeString
			} else {
				typeMap[colKey] = DataTypeBytes
			}
		default:
			// Unknown physical type - use Any
			typeMap[colKey] = DataTypeAny
		}
	}

	return typeMap
}
