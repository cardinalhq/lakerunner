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

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func TestExtractTimestamp(t *testing.T) {
	tests := []struct {
		row      Row
		field    string
		expected int64
	}{
		{Row{wkk.NewRowKey("ts"): int64(123)}, "ts", 123},
		{Row{wkk.NewRowKey("ts"): int32(456)}, "ts", 456},
		{Row{wkk.NewRowKey("ts"): int(789)}, "ts", 789},
		{Row{wkk.NewRowKey("ts"): float64(111.5)}, "ts", 111},
		{Row{wkk.NewRowKey("ts"): float32(222.7)}, "ts", 222},
		{Row{wkk.NewRowKey("ts"): "invalid"}, "ts", 0},
		{Row{wkk.NewRowKey("other"): 123}, "ts", 0},
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
