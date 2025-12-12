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

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func BenchmarkSpillCodec(b *testing.B) {
	codec := NewSpillCodec()
	row := benchmarkRow()
	dst := pipeline.Row{}
	buf := bytes.NewBuffer(make([]byte, 0, 2048))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		byteLen, err := codec.EncodeRowTo(buf, row)
		if err != nil {
			b.Fatal(err)
		}
		if int(byteLen) != buf.Len() {
			b.Fatalf("payload mismatch %d vs %d", byteLen, buf.Len())
		}
		if err := codec.DecodeRowFrom(bytes.NewReader(buf.Bytes()), dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkCBORCodec(b *testing.B) {
	codec, err := New(TypeCBOR)
	if err != nil {
		b.Fatal(err)
	}
	row := benchmarkRow()
	dst := pipeline.Row{}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := codec.EncodeRow(row)
		if err != nil {
			b.Fatal(err)
		}
		if err := codec.DecodeRow(data, dst); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkRow() pipeline.Row {
	return pipeline.Row{
		wkk.NewRowKey("byte"):     byte(7),
		wkk.NewRowKey("bytes"):    []byte{1, 2, 3, 4},
		wkk.NewRowKey("int64"):    int64(-123456789),
		wkk.NewRowKey("float64"):  float64(123.456),
		wkk.NewRowKey("strings"):  []string{"alpha", "beta", "gamma"},
		wkk.NewRowKey("bools"):    []bool{true, false, true, true},
		wkk.NewRowKey("float32s"): []float32{1.1, 2.2, 3.3, 4.4},
	}
}
