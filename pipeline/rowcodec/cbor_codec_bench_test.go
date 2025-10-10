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
	"bytes"
	"testing"
)

func BenchmarkCBOR_Encode(b *testing.B) {
	codec, err := NewCBOR()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := codec.Encode(testRow)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCBOR_Decode(b *testing.B) {
	codec, err := NewCBOR()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()
	encoded, err := codec.Encode(testRow)
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		decoded := make(map[string]any)
		err = codec.Decode(encoded, decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCBOR_RoundTrip(b *testing.B) {
	codec, err := NewCBOR()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		encoded, err := codec.Encode(testRow)
		if err != nil {
			b.Fatal(err)
		}
		decoded := make(map[string]any)
		err = codec.Decode(encoded, decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCBOR_Stream_Encode(b *testing.B) {
	codec, err := NewCBOR()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()
	var buf bytes.Buffer

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		encoder := codec.NewEncoder(&buf)
		if err := encoder.Encode(testRow); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCBOR_Stream_Decode(b *testing.B) {
	codec, err := NewCBOR()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()
	var buf bytes.Buffer
	encoder := codec.NewEncoder(&buf)
	if err := encoder.Encode(testRow); err != nil {
		b.Fatal(err)
	}
	data := buf.Bytes()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader := bytes.NewReader(data)
		decoder := codec.NewDecoder(reader)
		decoded := make(map[string]any)
		if err := decoder.Decode(decoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCBOR_Small(b *testing.B) {
	codec, err := NewCBOR()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createSmallMap()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		encoded, err := codec.Encode(testRow)
		if err != nil {
			b.Fatal(err)
		}
		decoded := make(map[string]any)
		err = codec.Decode(encoded, decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCBOR_Large(b *testing.B) {
	codec, err := NewCBOR()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createLargeMap()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		encoded, err := codec.Encode(testRow)
		if err != nil {
			b.Fatal(err)
		}
		decoded := make(map[string]any)
		err = codec.Decode(encoded, decoded)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCBOR_FileSize(b *testing.B) {
	codec, err := NewCBOR()
	if err != nil {
		b.Fatal(err)
	}

	testRow := createBenchmarkMap()
	encoded, err := codec.Encode(testRow)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportMetric(float64(len(encoded)), "bytes")
	b.ResetTimer()

	// Just measure encoding size
	for i := 0; i < b.N; i++ {
		_, err = codec.Encode(testRow)
		if err != nil {
			b.Fatal(err)
		}
	}
}
