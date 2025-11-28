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
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// extractSchemaFromParquetFile extracts schema from parquet file metadata.
func extractSchemaFromParquetFile(pf *parquet.File) *ReaderSchema {
	schema := NewReaderSchema()

	// Get parquet schema from file
	parquetSchema := pf.Schema()

	// Walk through all fields in the schema
	walkParquetSchema(schema, parquetSchema, "")

	return schema
}

// walkParquetSchema recursively walks the parquet schema and adds columns.
func walkParquetSchema(schema *ReaderSchema, node parquet.Node, prefix string) {
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
	columnName := strings.ReplaceAll(prefix, ".", "_")
	key := wkk.NewRowKeyFromBytes([]byte(columnName))

	// Map parquet type to our DataType
	dataType := parquetTypeToDataType(node.Type())
	schema.AddColumn(key, dataType, true)
}

// parquetTypeToDataType converts parquet type to our DataType.
func parquetTypeToDataType(ptype parquet.Type) DataType {
	// Check logical type first for ByteArray types
	if logicalType := ptype.LogicalType(); logicalType != nil {
		if logicalType.UTF8 != nil {
			return DataTypeString
		}
	}

	switch ptype.Kind() {
	case parquet.Boolean:
		return DataTypeBool
	case parquet.Int32, parquet.Int64:
		return DataTypeInt64
	case parquet.Float, parquet.Double:
		return DataTypeFloat64
	case parquet.ByteArray, parquet.FixedLenByteArray:
		// If no logical type was detected above, treat as bytes
		return DataTypeBytes
	default:
		// Default to string for unknown types
		return DataTypeString
	}
}

// extractSchemaFromArrowSchema extracts schema from Arrow schema metadata.
func extractSchemaFromArrowSchema(arrowSchema *arrow.Schema) *ReaderSchema {
	schema := NewReaderSchema()

	// Walk through all fields in the schema
	for _, field := range arrowSchema.Fields() {
		walkArrowField(schema, field)
	}

	return schema
}

// walkArrowField recursively walks an Arrow field and adds columns.
func walkArrowField(schema *ReaderSchema, field arrow.Field) {
	columnName := field.Name
	key := wkk.NewRowKeyFromBytes([]byte(columnName))

	// Map arrow type to our DataType
	dataType := arrowTypeToDataType(field.Type)
	schema.AddColumn(key, dataType, !field.Nullable)
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
	default:
		// For complex types (list, struct, map) and unknown, use DataTypeAny
		// These will be passed through as-is without conversion
		return DataTypeAny
	}
}
