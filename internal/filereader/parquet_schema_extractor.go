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
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet/file"
	pqarrow "github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// ExtractCompleteParquetSchema performs a full two-pass scan of a parquet file to discover
// all columns, including dynamic map keys and nested structures.
//
// This function:
// 1. Reads parquet metadata to get column types (physical types from parquet schema)
// 2. Reads Arrow schema to get structure (for nested types)
// 3. Scans ALL rows in the file to discover all map keys and track actual data types
// 4. Returns a complete ReaderSchema with all flattened column paths and proper types
//
// For simple types (int32, int64, string, etc): use parquet physical type
// For maps: discover keys by scanning data, track value types
// For nested structs: recursively flatten all paths
// For arrays/lists: treat as-is (not flattened)
func ExtractCompleteParquetSchema(ctx context.Context, pf *file.Reader, fr *pqarrow.FileReader) (*ReaderSchema, error) {
	schema := NewReaderSchema()

	// Get the Arrow schema for structure
	arrowSchema, err := fr.Schema()
	if err != nil {
		return nil, fmt.Errorf("failed to get arrow schema: %w", err)
	}

	// Extract statistics to determine which top-level columns have non-null values
	columnStats := extractParquetStatistics(pf)

	// Build map of column name to parquet physical type
	// Use parquet metadata directly instead of Arrow inferred types
	parquetTypes := buildParquetTypeMap(pf)

	// Create a record reader to scan all rows
	rr, err := fr.GetRecordReader(ctx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create record reader: %w", err)
	}
	defer rr.Release()

	// First pass: add all fields from the Arrow schema with parquet physical types
	for _, field := range arrowSchema.Fields() {
		hasNonNull := columnStats[field.Name]
		// Use parquet physical type if available, otherwise infer from Arrow type
		var dataType DataType
		if pqType, ok := parquetTypes[field.Name]; ok {
			dataType = pqType
		} else {
			dataType = arrowTypeToDataType(field.Type)
		}
		discoverFieldsFromArrowType(field, field.Name, field.Name, hasNonNull, dataType, schema)
	}

	// Second pass: scan all rows to discover dynamic map keys
	for {
		rec, err := rr.Read()
		if err != nil {
			break // EOF or error, either way we're done
		}
		if rec == nil || rec.NumRows() == 0 {
			break
		}

		// Scan each row to discover map keys
		fields := rec.Schema().Fields()
		numRows := int(rec.NumRows())

		for i := 0; i < numRows; i++ {
			for j, field := range fields {
				col := rec.Column(j)
				if !col.IsNull(i) {
					// Discover and add any map keys in this value
					discoverMapKeysInValue(field.Name, field.Name, col, i, schema)
				}
			}
		}

		rec.Release()
	}

	return schema, nil
}

// discoverFieldsFromArrowType recursively discovers all static fields from an Arrow type.
// This handles nested structs but NOT maps (map keys are dynamic and discovered by scanning data).
// hasNonNull indicates whether the parent field has non-null values based on statistics.
// dataType is the known type from parquet metadata for leaf fields.
func discoverFieldsFromArrowType(field arrow.Field, dottedPath, underscoredPath string, hasNonNull bool, dataType DataType, schema *ReaderSchema) {
	switch dt := field.Type.(type) {
	case *arrow.StructType:
		// Recursively discover nested struct fields
		// Nested fields get their types from Arrow (not parquet metadata)
		for _, nestedField := range dt.Fields() {
			nestedDotted := dottedPath + "." + nestedField.Name
			nestedUnderscored := underscoredPath + "_" + nestedField.Name
			nestedType := arrowTypeToDataType(nestedField.Type)
			discoverFieldsFromArrowType(nestedField, nestedDotted, nestedUnderscored, hasNonNull, nestedType, schema)
		}

	case *arrow.MapType:
		// Maps are NOT flattened here - their keys are discovered during data scan
		// Just add the map field itself as a placeholder
		finalUnderscored := strings.ReplaceAll(underscoredPath, ".", "_")
		newKey := wkk.NewRowKey(finalUnderscored)
		origKey := wkk.NewRowKey(dottedPath)
		// We'll discover actual types during scan, for now use Any
		schema.AddColumn(newKey, origKey, DataTypeAny, hasNonNull)

	case *arrow.ListType:
		// Lists are stored as-is (not flattened)
		finalUnderscored := strings.ReplaceAll(underscoredPath, ".", "_")
		newKey := wkk.NewRowKey(finalUnderscored)
		origKey := wkk.NewRowKey(dottedPath)
		schema.AddColumn(newKey, origKey, DataTypeAny, hasNonNull)

	default:
		// Leaf field - use the dataType passed in (from parquet metadata)
		finalUnderscored := strings.ReplaceAll(underscoredPath, ".", "_")
		newKey := wkk.NewRowKey(finalUnderscored)
		origKey := wkk.NewRowKey(dottedPath)
		schema.AddColumn(newKey, origKey, dataType, hasNonNull)
	}
}

// discoverMapKeysInValue recursively scans an Arrow array value to discover map keys.
// This is called for every row to ensure we find ALL map keys in the entire file.
func discoverMapKeysInValue(dottedPath, underscoredPath string, col arrow.Array, rowIndex int, schema *ReaderSchema) {
	switch c := col.(type) {
	case *array.Struct:
		// Recursively scan nested struct fields
		dt := c.DataType().(*arrow.StructType)
		fields := dt.Fields()
		for j, field := range fields {
			nestedCol := c.Field(j)
			if !nestedCol.IsNull(rowIndex) {
				nestedDotted := dottedPath + "." + field.Name
				nestedUnderscored := underscoredPath + "_" + field.Name
				discoverMapKeysInValue(nestedDotted, nestedUnderscored, nestedCol, rowIndex, schema)
			}
		}

	case *array.Map:
		// Discover all keys in this map entry
		if !c.IsNull(rowIndex) {
			start, end := c.ValueOffsets(rowIndex)
			keys := c.Keys()
			items := c.Items()

			for j := start; j < end; j++ {
				key := convertArrowValueForSchema(keys, int(j))
				if keyStr, ok := key.(string); ok {
					// Build flattened path for this map key
					nestedDotted := dottedPath + "." + keyStr
					nestedUnderscored := underscoredPath + "_" + keyStr
					finalUnderscored := strings.ReplaceAll(nestedUnderscored, ".", "_")
					finalDotted := nestedDotted

					// Use the actual Arrow type from the items array
					// instead of inferring from the converted Go value
					dataType := arrowTypeToDataType(items.DataType())

					// Add this map key as a column
					newKey := wkk.NewRowKey(finalUnderscored)
					origKey := wkk.NewRowKey(finalDotted)
					schema.AddColumn(newKey, origKey, dataType, true)

					// Recursively discover nested maps/structs in the value
					if !items.IsNull(int(j)) {
						discoverMapKeysInValue(finalDotted, finalUnderscored, items, int(j), schema)
					}
				}
			}
		}

	case *array.List:
		// Lists are not flattened, but we should still check their elements for nested maps
		// For now, we treat lists as opaque
		// Future: could scan list elements for nested maps if needed

	default:
		// Leaf value - nothing to discover
	}
}

// convertArrowValueForSchema converts an Arrow value to Go type for schema type inference.
// This is a simplified version that doesn't need to handle all edge cases.
func convertArrowValueForSchema(col arrow.Array, i int) any {
	switch c := col.(type) {
	case *array.Boolean:
		return c.Value(i)
	case *array.Int8:
		return int64(c.Value(i))
	case *array.Int16:
		return int64(c.Value(i))
	case *array.Int32:
		return int64(c.Value(i))
	case *array.Int64:
		return c.Value(i)
	case *array.Uint8:
		return uint64(c.Value(i))
	case *array.Uint16:
		return uint64(c.Value(i))
	case *array.Uint32:
		return uint64(c.Value(i))
	case *array.Uint64:
		return c.Value(i)
	case *array.Float32:
		return float64(c.Value(i))
	case *array.Float64:
		return c.Value(i)
	case *array.String:
		return c.Value(i)
	case *array.LargeString:
		return c.Value(i)
	case *array.Binary:
		return c.Value(i)
	case *array.LargeBinary:
		return c.Value(i)
	default:
		// For unknown types, return string representation
		return fmt.Sprintf("%v", c.ValueStr(i))
	}
}
