// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package buffet

import (
	"fmt"
	"maps"

	"github.com/parquet-go/parquet-go"
)

// NodeMapBuilder accumulates multiple example rows (map[string]any) and produces
// a consolidated map[string]parquet.Node. It ensures that fields with the same
// key have compatible types across all added examples.
type NodeMapBuilder struct {
	nodes map[string]parquet.Node
}

// NewNodeMapBuilder initializes an empty builder.
func NewNodeMapBuilder() *NodeMapBuilder {
	return &NodeMapBuilder{nodes: make(map[string]parquet.Node)}
}

// Add inspects the fields of example (a map[string]any) and merges their types
// into the builder. If a field name already exists but the new node’s String()
// differs, Add returns an error.
func (b *NodeMapBuilder) Add(example map[string]any) error {
	for name, val := range example {
		if val == nil {
			continue
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

func (b *NodeMapBuilder) AddNodes(nodes map[string]parquet.Node) error {
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

// Build returns a copy of the consolidated node map. You can pass this into
// NewAutoMapWriterWithNodeMap().
func (b *NodeMapBuilder) Build() map[string]parquet.Node {
	out := make(map[string]parquet.Node, len(b.nodes))
	maps.Copy(out, b.nodes) // Copy the map to avoid external mutations
	return out
}
