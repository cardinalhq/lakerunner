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
