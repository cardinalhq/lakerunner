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

package rowcodec

import (
	"fmt"
)

// createBenchmarkMap creates a map with 100 items: 80 strings, 15 float64, 4 int64, 1 []byte
// This represents a typical telemetry row with mixed data types.
func createBenchmarkMap() map[string]any {
	row := make(map[string]any, 100)

	// 80 string fields (common for labels, metadata, etc.)
	for i := 0; i < 80; i++ {
		row[fmt.Sprintf("string_field_%d", i)] = fmt.Sprintf("value_string_%d_with_some_longer_content", i)
	}

	// 15 float64 fields (metrics values)
	for i := 0; i < 15; i++ {
		row[fmt.Sprintf("float_field_%d", i)] = float64(i) * 3.14159265359
	}

	// 4 int64 fields (timestamps, counters)
	for i := 0; i < 4; i++ {
		row[fmt.Sprintf("int_field_%d", i)] = int64(i * 1000000)
	}

	// 1 []byte field (binary payload)
	row["binary_data"] = []byte("this is some binary data that could represent a payload or encoded content")

	return row
}

// createSmallMap creates a small map for testing overhead
func createSmallMap() map[string]any {
	return map[string]any{
		"timestamp": int64(1234567890),
		"value":     float64(42.0),
		"label":     "test",
	}
}

// createLargeMap creates a large map for testing scalability
func createLargeMap() map[string]any {
	row := make(map[string]any, 1000)

	for i := 0; i < 1000; i++ {
		switch i % 4 {
		case 0:
			row[fmt.Sprintf("field_%d", i)] = fmt.Sprintf("string_value_%d", i)
		case 1:
			row[fmt.Sprintf("field_%d", i)] = float64(i) * 1.23
		case 2:
			row[fmt.Sprintf("field_%d", i)] = int64(i * 1000)
		case 3:
			row[fmt.Sprintf("field_%d", i)] = []byte(fmt.Sprintf("bytes_%d", i))
		}
	}

	return row
}
