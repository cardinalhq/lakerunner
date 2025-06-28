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

package cmd

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
