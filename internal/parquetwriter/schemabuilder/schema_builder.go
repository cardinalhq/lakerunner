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

package schemabuilder

import (
	"fmt"
	"reflect"

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/filereader"
)

// SchemaBuilder accumulates multiple example rows (map[string]any) and produces
// a consolidated map[string]parquet.Node. It ensures that fields with the same
// key have compatible types across all added examples.
type SchemaBuilder struct {
	exemplars map[string]any // Field name -> sample value for type inference
}

// NewSchemaBuilder initializes an empty schema builder.
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{exemplars: make(map[string]any)}
}

// AddRow inspects the fields of a row (a map[string]any) and stores exemplar values
// for type inference. Does lightweight type checking without expensive ParquetNodeFromType calls.
func (b *SchemaBuilder) AddRow(row map[string]any) error {
	for name, val := range row {
		if val == nil {
			continue // Skip nil values - they don't contribute to schema
		}
		if existing, exists := b.exemplars[name]; exists {
			// Check type compatibility using reflect.TypeOf - much cheaper than ParquetNodeFromType
			if reflect.TypeOf(existing) != reflect.TypeOf(val) {
				return fmt.Errorf(
					"type mismatch for field %q: existing=%T, new=%T",
					name, existing, val,
				)
			}
		} else {
			// Store the first non-nil value we see for this field name
			b.exemplars[name] = val
		}
	}
	return nil
}

// Build creates parquet nodes from collected exemplars and returns the consolidated node map.
// This is where ParquetNodeFromType is called - only once per unique field.
// Returns an error if any field cannot be converted to a valid parquet node.
func (b *SchemaBuilder) Build() (map[string]parquet.Node, error) {
	nodes := make(map[string]parquet.Node, len(b.exemplars))
	for name, exemplar := range b.exemplars {
		node, err := ParquetNodeFromType(name, exemplar)
		if err != nil {
			return nil, fmt.Errorf("failed to build node for field %q: %w", name, err)
		}
		nodes[name] = node
	}
	return nodes, nil
}

// HasColumns returns true if the builder has seen any columns.
func (b *SchemaBuilder) HasColumns() bool {
	return len(b.exemplars) > 0
}

// GetColumnNames returns a sorted list of column names seen so far.
func (b *SchemaBuilder) GetColumnNames() []string {
	names := make([]string, 0, len(b.exemplars))
	for name := range b.exemplars {
		names = append(names, name)
	}
	return names
}

// BuildFromReaderSchema creates parquet nodes directly from a ReaderSchema.
// This allows providing schema upfront without needing to scan data.
// Filters out all-null columns (HasNonNull=false) automatically.
func BuildFromReaderSchema(schema *filereader.ReaderSchema) (map[string]parquet.Node, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema cannot be nil")
	}

	nodes := make(map[string]parquet.Node)
	for _, col := range schema.Columns() {
		// Skip all-null columns
		if !col.HasNonNull {
			continue
		}

		columnName := string(col.Name.Value())
		node, err := ParquetNodeFromReaderDataType(columnName, col.DataType)
		if err != nil {
			return nil, fmt.Errorf("failed to build node for column %q: %w", columnName, err)
		}
		nodes[columnName] = node
	}

	if len(nodes) == 0 {
		return nil, fmt.Errorf("schema has no non-null columns")
	}

	return nodes, nil
}

// ParquetNodeFromReaderDataType converts a filereader.DataType to a parquet.Node.
func ParquetNodeFromReaderDataType(name string, dataType filereader.DataType) (parquet.Node, error) {
	enc := func(n parquet.Node) parquet.Node {
		if n.Leaf() && wantDictionary(name) {
			n = parquet.Encoded(n, &parquet.RLEDictionary)
		}
		return n
	}

	switch dataType {
	case filereader.DataTypeBool:
		return parquet.Optional(enc(parquet.Leaf(parquet.BooleanType))), nil
	case filereader.DataTypeInt64:
		return parquet.Optional(enc(parquet.Int(64))), nil
	case filereader.DataTypeFloat64:
		return parquet.Optional(enc(parquet.Leaf(parquet.DoubleType))), nil
	case filereader.DataTypeString:
		return parquet.Optional(enc(parquet.String())), nil
	case filereader.DataTypeBytes:
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), nil
	case filereader.DataTypeAny:
		// For complex types, use string representation
		return parquet.Optional(enc(parquet.String())), nil
	default:
		return nil, fmt.Errorf("unsupported data type: %v", dataType)
	}
}
