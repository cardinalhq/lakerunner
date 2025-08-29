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
	"testing"
)

// createBenchmarkMap creates a map with 100 items: 80 strings, 15 float64, 4 int64, 1 []byte
func createBenchmarkMap() map[string]any {
	row := make(map[string]any, 100)

	// 80 string fields
	for i := 0; i < 80; i++ {
		row[fmt.Sprintf("string_field_%d", i)] = fmt.Sprintf("value_string_%d_with_some_longer_content", i)
	}

	// 15 float64 fields
	for i := 0; i < 15; i++ {
		row[fmt.Sprintf("float_field_%d", i)] = float64(i) * 3.14159265359
	}

	// 4 int64 fields
	for i := 0; i < 4; i++ {
		row[fmt.Sprintf("int_field_%d", i)] = int64(i * 1000000)
	}

	// 1 []byte field
	row["binary_data"] = []byte("this is some binary data that could represent a payload or encoded content")

	return row
}

func BenchmarkRowCodecEncode(b *testing.B) {
	config, err := NewConfig()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := config.Encode(testRow)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRowCodecDecode(b *testing.B) {
	config, err := NewConfig()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()
	encoded, err := config.Encode(testRow)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := config.Decode(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRowCodecRoundTrip(b *testing.B) {
	config, err := NewConfig()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		encoded, err := config.Encode(testRow)
		if err != nil {
			b.Fatal(err)
		}
		_, err = config.Decode(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRowCodecEncodeParallel tests concurrent encoding performance
func BenchmarkRowCodecEncodeParallel(b *testing.B) {
	config, err := NewConfig()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := config.Encode(testRow)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRowCodecDecodeParallel tests concurrent decoding performance
func BenchmarkRowCodecDecodeParallel(b *testing.B) {
	config, err := NewConfig()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()
	encoded, err := config.Encode(testRow)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			decoded, err := config.Decode(encoded)
			if err != nil {
				b.Fatal(err)
			}
			config.ReturnMap(decoded) // Return to pool
		}
	})
}

// BenchmarkRowCodecDecodeWithPool tests decoding with explicit pooled map reuse
func BenchmarkRowCodecDecodeWithPool(b *testing.B) {
	config, err := NewConfig()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()
	encoded, err := config.Encode(testRow)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		decoded, err := config.Decode(encoded)
		if err != nil {
			b.Fatal(err)
		}
		config.ReturnMap(decoded) // Return to pool for next iteration
	}
}

// BenchmarkRowCodecDecodeInto tests decoding into caller-provided map
func BenchmarkRowCodecDecodeInto(b *testing.B) {
	config, err := NewConfig()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()
	encoded, err := config.Encode(testRow)
	if err != nil {
		b.Fatal(err)
	}

	// Pre-allocate reusable map
	targetMap := make(map[string]any, 100)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		err := config.DecodeInto(encoded, targetMap)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRowCodecDecodeNoPool tests original decode without pooling for comparison
func BenchmarkRowCodecDecodeNoPool(b *testing.B) {
	config, err := NewConfig()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()
	encoded, err := config.Encode(testRow)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Don't return to pool to simulate old behavior
		_, err := config.Decode(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}
