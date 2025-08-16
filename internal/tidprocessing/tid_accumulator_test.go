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

package tidprocessing

import (
	"testing"
)

func TestTidAccumulator_Add(t *testing.T) {
	tests := []struct {
		name     string
		rows     []map[string]any
		expected int
	}{
		{
			name: "single valid tid",
			rows: []map[string]any{
				{"_cardinalhq.tid": int64(123)},
			},
			expected: 1,
		},
		{
			name: "multiple unique tids",
			rows: []map[string]any{
				{"_cardinalhq.tid": int64(1)},
				{"_cardinalhq.tid": int64(2)},
				{"_cardinalhq.tid": int64(3)},
			},
			expected: 3,
		},
		{
			name: "duplicate tids",
			rows: []map[string]any{
				{"_cardinalhq.tid": int64(1)},
				{"_cardinalhq.tid": int64(1)},
				{"_cardinalhq.tid": int64(2)},
			},
			expected: 2,
		},
		{
			name: "missing tid key",
			rows: []map[string]any{
				{"foo": 1},
				{"bar": 2},
			},
			expected: 0,
		},
		{
			name: "tid wrong type",
			rows: []map[string]any{
				{"_cardinalhq.tid": "not-an-int"},
				{"_cardinalhq.tid": 123.45},
			},
			expected: 0,
		},
		{
			name: "mixed valid and invalid tids",
			rows: []map[string]any{
				{"_cardinalhq.tid": int64(1)},
				{"_cardinalhq.tid": "bad"},
				{"foo": 2},
				{"_cardinalhq.tid": int64(2)},
				{"_cardinalhq.tid": int64(1)},
			},
			expected: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc := NewTidAccumulator()
			for _, row := range tt.rows {
				acc.Add(row)
			}
			result := acc.Finalize().(TidAccumulatorResult)
			if result.Cardinality != tt.expected {
				t.Errorf("got %d, want %d", result.Cardinality, tt.expected)
			}
		})
	}
}
