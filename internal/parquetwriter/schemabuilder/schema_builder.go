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
	"maps"

	"github.com/parquet-go/parquet-go"
)

// SchemaBuilder accumulates multiple example rows (map[string]any) and produces
// a consolidated map[string]parquet.Node. It ensures that fields with the same
// key have compatible types across all added examples.
type SchemaBuilder struct {
	nodes map[string]parquet.Node
}

// NewSchemaBuilder initializes an empty schema builder.
func NewSchemaBuilder() *SchemaBuilder {
	return &SchemaBuilder{nodes: make(map[string]parquet.Node)}
}

// AddRow inspects the fields of a row (a map[string]any) and merges their types
// into the builder. If a field name already exists but the new node's String()
// differs, AddRow returns an error.
func (b *SchemaBuilder) AddRow(row map[string]any) error {
	for name, val := range row {
		if val == nil {
			continue // Skip nil values - they don't contribute to schema
		}
		node, err := ParquetNodeFromType(name, val)
		if err != nil {
			return fmt.Errorf("failed to build node for field %q: %w", name, err)
		}
		if existing, ok := b.nodes[name]; ok {
			if !parquet.EqualNodes(existing, node) {
				return fmt.Errorf(
					"type mismatch for field %q: existing=%s, new=%s",
					name, existing.String(), node.String(),
				)
			}
		} else {
			b.nodes[name] = node
		}
	}
	return nil
}

// AddNodes merges a map of parquet nodes into the builder.
func (b *SchemaBuilder) AddNodes(nodes map[string]parquet.Node) error {
	for name, node := range nodes {
		if existing, ok := b.nodes[name]; ok {
			if !parquet.EqualNodes(existing, node) {
				return fmt.Errorf(
					"type mismatch for field %q: existing=%s, new=%s",
					name, existing.String(), node.String(),
				)
			}
		} else {
			b.nodes[name] = node
		}
	}
	return nil
}

// Build returns a copy of the consolidated node map.
func (b *SchemaBuilder) Build() map[string]parquet.Node {
	out := make(map[string]parquet.Node, len(b.nodes))
	maps.Copy(out, b.nodes) // Copy the map to avoid external mutations
	return out
}

// HasColumns returns true if the builder has seen any columns.
func (b *SchemaBuilder) HasColumns() bool {
	return len(b.nodes) > 0
}

// GetColumnNames returns a sorted list of column names seen so far.
func (b *SchemaBuilder) GetColumnNames() []string {
	names := make([]string, 0, len(b.nodes))
	for name := range b.nodes {
		names = append(names, name)
	}
	return names
}
