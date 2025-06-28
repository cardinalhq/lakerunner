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

package filecrunch

import (
	"fmt"

	"github.com/parquet-go/parquet-go"
)

func MergeNodes(fh *FileHandle, mergedNodes map[string]parquet.Node) error {
	for k, v := range fh.Nodes {
		if newNode, ok := mergedNodes[k]; ok {
			if newNode != v {
				return fmt.Errorf("schema mismatch: %s, currentType %s, newType %s", k, v, newNode)
			}
		} else {
			mergedNodes[k] = v
		}
	}

	return nil
}

func SchemaFromNodes(nodes map[string]parquet.Node) *parquet.Schema {
	return parquet.NewSchema("merged", parquet.Group(nodes))
}
