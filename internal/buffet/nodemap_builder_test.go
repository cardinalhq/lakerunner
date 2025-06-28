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
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNodeMapBuilder_Add(t *testing.T) {
	tests := []struct {
		name      string
		inputs    []map[string]any
		wantKeys  []string
		wantError bool
	}{
		{
			name: "success with compatible types",
			inputs: []map[string]any{
				{"foo": int64(123), "bar": "baz"},
				{"foo": int64(321), "bar": "qux"},
			},
			wantKeys:  []string{"foo", "bar"},
			wantError: false,
		},
		{
			name: "additive",
			inputs: []map[string]any{
				{"foo": int64(123)},
				{"bar": "baz"},
			},
			wantKeys:  []string{"foo", "bar"},
			wantError: false,
		},
		{
			name: "nils are ignored when adding",
			inputs: []map[string]any{
				{"foo": nil},
				{"bar": "baz"},
			},
			wantKeys:  []string{"bar"},
			wantError: false,
		},
		{
			name: "nils can be later added",
			inputs: []map[string]any{
				{"foo": nil},
				{"bar": "baz"},
				{"foo": int64(123)},
			},
			wantKeys:  []string{"foo", "bar"},
			wantError: false,
		},
		{
			name: "nils can come after that key is added",
			inputs: []map[string]any{
				{"foo": int64(123)},
				{"bar": "baz"},
				{"foo": nil},
			},
			wantKeys:  []string{"foo", "bar"},
			wantError: false,
		},
		{
			name: "type mismatch",
			inputs: []map[string]any{
				{"foo": int32(123)},
				{"foo": "not an int"},
			},
			wantError: true,
		},
		{
			name: "ParquetNodeFromType error",
			inputs: []map[string]any{
				{"foo": errors.New("fail")},
			},
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewNodeMapBuilder()
			var err error
			for _, input := range tt.inputs {
				err = b.Add(input)
				if err != nil {
					break
				}
			}
			if tt.wantError {
				require.Error(t, err, "expected error, got nil")
			} else {
				require.NoError(t, err, "unexpected error")
				foundKeys := make([]string, 0, len(b.nodes))
				for key := range b.nodes {
					foundKeys = append(foundKeys, key)
				}
				require.ElementsMatch(t, tt.wantKeys, foundKeys, "expected keys do not match found keys")
			}
		})
	}
}
