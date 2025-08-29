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
