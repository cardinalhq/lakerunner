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

	"github.com/parquet-go/parquet-go"
)

func WriterOptions(tmpdir string, schema *parquet.Schema) []parquet.WriterOption {
	return []parquet.WriterOption{
		schema,
		parquet.Compression(&parquet.Zstd),
		parquet.PageBufferSize(32 * 1024),
		parquet.ColumnIndexSizeLimit(1024),
		parquet.MaxRowsPerRowGroup(80_000),
		parquet.ColumnPageBuffers(
			parquet.NewFileBufferPool(tmpdir, "buffers.*"),
		),
	}
}

var dictionaryFieldOverride = map[string]bool{
	"_cardinalhq.message": false,
	"_cardinalhq.tid":     false,
}

// wantDictionary returns true if the field should use dictionary encoding.
// The default is true, but can be overridded in the dictionaryFieldOverride map.
func wantDictionary(name string) bool {
	v, ok := dictionaryFieldOverride[name]
	if ok {
		return v
	}
	return true
}

// ParquetNodeFromType returns a parquet.Node for the given Go type.
// Not all types are supported.
func ParquetNodeFromType(name string, t any) (parquet.Node, error) {
	enc := func(n parquet.Node) parquet.Node {
		if n.Leaf() && wantDictionary(name) {
			n = parquet.Encoded(n, &parquet.RLEDictionary)
		}
		return n
	}

	switch t.(type) {
	case byte:
		return parquet.Optional(enc(parquet.Uint(8))), nil
	case []byte:
		return parquet.Optional(parquet.Leaf(parquet.ByteArrayType)), nil

	case int8:
		return parquet.Optional(enc(parquet.Int(8))), nil
	case []int8:
		return parquet.Optional(parquet.List(enc(parquet.Int(8)))), nil

	case int16:
		return parquet.Optional(enc(parquet.Int(16))), nil
	case []int16:
		return parquet.Optional(parquet.List(enc(parquet.Int(16)))), nil

	case int32:
		return parquet.Optional(enc(parquet.Int(32))), nil
	case []int32:
		return parquet.Optional(parquet.List(enc(parquet.Int(32)))), nil

	case int64:
		return parquet.Optional(enc(parquet.Int(64))), nil
	case []int64:
		return parquet.Optional(parquet.List(enc(parquet.Int(64)))), nil

	case float32:
		return parquet.Optional(enc(parquet.Leaf(parquet.FloatType))), nil
	case []float32:
		return parquet.Optional(parquet.List(enc(parquet.Leaf(parquet.FloatType)))), nil

	case float64:
		return parquet.Optional(enc(parquet.Leaf(parquet.DoubleType))), nil
	case []float64:
		return parquet.Optional(parquet.List(enc(parquet.Leaf(parquet.DoubleType)))), nil

	case string:
		return parquet.Optional(enc(parquet.String())), nil
	case []string:
		return parquet.Optional(parquet.List(enc(parquet.String()))), nil

	case bool:
		return parquet.Optional(enc(parquet.Leaf(parquet.BooleanType))), nil
	case []bool:
		return parquet.Optional(parquet.List(enc(parquet.Leaf(parquet.BooleanType)))), nil

	default:
		return nil, fmt.Errorf("unsupported type %T", t)
	}
}

func NodesFromMap(nodes map[string]parquet.Node, tags map[string]any) error {
	for k, v := range tags {
		if v == nil {
			continue
		}
		node, err := ParquetNodeFromType(k, v)
		if err != nil {
			return fmt.Errorf("parquet node from type for key %s: %w", k, err)
		}
		if on, ok := nodes[k]; ok {
			if !parquet.EqualNodes(on, node) {
				return fmt.Errorf("type mismatch for key %s: existing %T, new %T", k, on, node)
			}
			continue
		}
		nodes[k] = node
	}
	return nil
}

func ParquetSchemaFromNodemap(name string, fields map[string]parquet.Node) (*parquet.Schema, error) {
	return parquet.NewSchema(name, parquet.Group(fields)), nil
}
