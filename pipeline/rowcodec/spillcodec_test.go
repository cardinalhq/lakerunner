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

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestSpillCodec_RoundTripAllTypes(t *testing.T) {
	codec := NewSpillCodec()

	row := pipeline.Row{
		wkk.NewRowKey("byte"):           byte(7),
		wkk.NewRowKey("bytes"):          []byte{1, 2, 3, 4},
		wkk.NewRowKey("int8"):           int8(-8),
		wkk.NewRowKey("int8s"):          []int8{-1, 2, -3},
		wkk.NewRowKey("int16"):          int16(-32000),
		wkk.NewRowKey("int16s"):         []int16{-4, 5, -6},
		wkk.NewRowKey("int32"):          int32(-123456),
		wkk.NewRowKey("int32s"):         []int32{-7, 8, -9},
		wkk.NewRowKey("int64"):          int64(-9876543210),
		wkk.NewRowKey("int64s"):         []int64{-10, 11, -12},
		wkk.NewRowKey("float32"):        float32(3.14),
		wkk.NewRowKey("float32s"):       []float32{1.5, -2.5, 3.75},
		wkk.NewRowKey("float64"):        float64(-123.456),
		wkk.NewRowKey("float64s"):       []float64{0.5, -0.25, 42.0},
		wkk.NewRowKey("string"):         "hello",
		wkk.NewRowKey("strings"):        []string{"a", "bc", ""},
		wkk.NewRowKey("bool"):           true,
		wkk.NewRowKey("bools"):          []bool{true, false, true},
		wkk.NewRowKey("empty_bytes"):    []byte{},
		wkk.NewRowKey("empty_strings"):  []string{},
		wkk.NewRowKey("empty_int64s"):   []int64{},
		wkk.NewRowKey("empty_boollist"): []bool{},
	}

	var buf bytes.Buffer
	byteLen, err := codec.EncodeRowTo(&buf, row)
	require.NoError(t, err)
	require.Equal(t, buf.Len(), int(byteLen))

	decoded := pipeline.GetPooledRow()
	defer pipeline.ReturnPooledRow(decoded)

	err = codec.DecodeRowFrom(bytes.NewReader(buf.Bytes()), decoded)
	require.NoError(t, err)

	require.Equal(t, row, decoded)
}

func TestSpillCodec_UnsupportedType(t *testing.T) {
	codec := NewSpillCodec()
	row := pipeline.Row{wkk.NewRowKey("bad"): struct{}{}}

	_, err := codec.EncodeRowTo(&bytes.Buffer{}, row)
	require.Error(t, err)
}

func TestSpillCodec_SharedDictionaryAcrossInstances(t *testing.T) {
	codec1 := NewSpillCodec()
	codec2 := NewSpillCodec()

	row := pipeline.Row{wkk.NewRowKey("shared_key"): int64(123)}

	var buf bytes.Buffer
	byteLen, err := codec1.EncodeRowTo(&buf, row)
	require.NoError(t, err)
	require.Equal(t, buf.Len(), int(byteLen))

	decoded := pipeline.Row{}
	err = codec2.DecodeRowFrom(bytes.NewReader(buf.Bytes()), decoded)
	require.NoError(t, err)
	require.Equal(t, row, decoded)
}
