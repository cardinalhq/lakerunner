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
