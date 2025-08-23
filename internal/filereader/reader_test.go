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

package filereader

import (
	"testing"
)

func TestTimeOrderedSelector(t *testing.T) {
	selector := TimeOrderedSelector("timestamp")

	// Test with valid timestamps
	rows := []Row{
		{"timestamp": int64(300), "data": "third"},
		{"timestamp": int64(100), "data": "first"},
		{"timestamp": int64(200), "data": "second"},
	}

	selectedIdx := selector(rows)
	if selectedIdx != 1 {
		t.Errorf("Expected index 1 (timestamp 100), got %d", selectedIdx)
	}

	// Test with mixed timestamp types
	rows = []Row{
		{"timestamp": int32(300), "data": "third"},
		{"timestamp": float64(150.0), "data": "second"},
		{"timestamp": int64(100), "data": "first"},
	}

	selectedIdx = selector(rows)
	if selectedIdx != 2 {
		t.Errorf("Expected index 2 (timestamp 100), got %d", selectedIdx)
	}

	// Test with missing timestamp field
	rows = []Row{
		{"data": "no timestamp"},
		{"timestamp": int64(100), "data": "has timestamp"},
	}

	selectedIdx = selector(rows)
	if selectedIdx != 0 {
		t.Errorf("Expected index 0 (missing timestamp defaults to 0), got %d", selectedIdx)
	}

	// Test with empty slice
	selectedIdx = selector([]Row{})
	if selectedIdx != -1 {
		t.Errorf("Expected -1 for empty slice, got %d", selectedIdx)
	}
}

func TestExtractTimestamp(t *testing.T) {
	tests := []struct {
		row      Row
		field    string
		expected int64
	}{
		{Row{"ts": int64(123)}, "ts", 123},
		{Row{"ts": int32(456)}, "ts", 456},
		{Row{"ts": int(789)}, "ts", 789},
		{Row{"ts": float64(111.5)}, "ts", 111},
		{Row{"ts": float32(222.7)}, "ts", 222},
		{Row{"ts": "invalid"}, "ts", 0},
		{Row{"other": 123}, "ts", 0},
		{Row{}, "ts", 0},
	}

	for i, test := range tests {
		t.Run(string(rune(i+'A')), func(t *testing.T) {
			result := extractTimestamp(test.row, test.field)
			if result != test.expected {
				t.Errorf("extractTimestamp(%v, %q) = %d, want %d",
					test.row, test.field, result, test.expected)
			}
		})
	}
}
