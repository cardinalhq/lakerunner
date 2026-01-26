// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestSpillCodec_RoundTripAllTypes(t *testing.T) {
	codec := NewSpillCodec()

	row := pipeline.Row{
		wkk.NewRowKey("nil_value"):      nil,
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

func TestSpillCodec_EmptyRow(t *testing.T) {
	codec := NewSpillCodec()
	row := pipeline.Row{}

	var buf bytes.Buffer
	byteLen, err := codec.EncodeRowTo(&buf, row)
	require.NoError(t, err)
	require.Equal(t, int32(1), byteLen) // Just the varint for field count 0

	decoded := pipeline.Row{}
	err = codec.DecodeRowFrom(bytes.NewReader(buf.Bytes()), decoded)
	require.NoError(t, err)
	require.Empty(t, decoded)
}

func TestSpillCodec_UnknownKeyID(t *testing.T) {
	// Craft bytes with an invalid key ID that doesn't exist in the dictionary
	var buf bytes.Buffer
	buf.WriteByte(1) // field count = 1 (varint)
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, uint32(999999999)))
	buf.WriteByte(spillTagInt64)
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, int64(123)))

	codec := NewSpillCodec()
	decoded := pipeline.Row{}
	err := codec.DecodeRowFrom(bytes.NewReader(buf.Bytes()), decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown key id")
}

func TestSpillCodec_UnknownTypeTag(t *testing.T) {
	codec := NewSpillCodec()

	// Encode a valid row first to get a known key into the dictionary
	row := pipeline.Row{wkk.NewRowKey("test_unknown_tag"): int64(1)}
	var validBuf bytes.Buffer
	_, err := codec.EncodeRowTo(&validBuf, row)
	require.NoError(t, err)

	// Now craft bytes with a valid key ID but unknown type tag (255)
	var buf bytes.Buffer
	buf.WriteByte(1) // field count = 1
	// Get the key ID that was assigned
	keyID := codec.ensureKeyID(wkk.NewRowKey("test_unknown_tag"))
	require.NoError(t, binary.Write(&buf, binary.LittleEndian, keyID))
	buf.WriteByte(255) // invalid type tag

	decoded := pipeline.Row{}
	err = codec.DecodeRowFrom(bytes.NewReader(buf.Bytes()), decoded)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unknown type tag")
}

func TestSpillCodec_NilSlicePreservation(t *testing.T) {
	codec := NewSpillCodec()

	// Test all slice types: nil vs empty should be preserved
	row := pipeline.Row{
		wkk.NewRowKey("nil_bytes"):    []byte(nil),
		wkk.NewRowKey("empty_bytes"):  []byte{},
		wkk.NewRowKey("nil_int8s"):    []int8(nil),
		wkk.NewRowKey("empty_int8s"):  []int8{},
		wkk.NewRowKey("nil_int16s"):   []int16(nil),
		wkk.NewRowKey("empty_int16s"): []int16{},
		wkk.NewRowKey("nil_int32s"):   []int32(nil),
		wkk.NewRowKey("empty_int32s"): []int32{},
		wkk.NewRowKey("nil_int64s"):   []int64(nil),
		wkk.NewRowKey("empty_int64s"): []int64{},
		wkk.NewRowKey("nil_f32s"):     []float32(nil),
		wkk.NewRowKey("empty_f32s"):   []float32{},
		wkk.NewRowKey("nil_f64s"):     []float64(nil),
		wkk.NewRowKey("empty_f64s"):   []float64{},
		wkk.NewRowKey("nil_strings"):  []string(nil),
		wkk.NewRowKey("empty_strs"):   []string{},
		wkk.NewRowKey("nil_bools"):    []bool(nil),
		wkk.NewRowKey("empty_bools"):  []bool{},
	}

	var buf bytes.Buffer
	_, err := codec.EncodeRowTo(&buf, row)
	require.NoError(t, err)

	decoded := pipeline.Row{}
	err = codec.DecodeRowFrom(bytes.NewReader(buf.Bytes()), decoded)
	require.NoError(t, err)

	// Verify nil slices are nil
	require.Nil(t, decoded[wkk.NewRowKey("nil_bytes")])
	require.Nil(t, decoded[wkk.NewRowKey("nil_int8s")])
	require.Nil(t, decoded[wkk.NewRowKey("nil_int16s")])
	require.Nil(t, decoded[wkk.NewRowKey("nil_int32s")])
	require.Nil(t, decoded[wkk.NewRowKey("nil_int64s")])
	require.Nil(t, decoded[wkk.NewRowKey("nil_f32s")])
	require.Nil(t, decoded[wkk.NewRowKey("nil_f64s")])
	require.Nil(t, decoded[wkk.NewRowKey("nil_strings")])
	require.Nil(t, decoded[wkk.NewRowKey("nil_bools")])

	// Verify empty slices are non-nil but empty
	require.NotNil(t, decoded[wkk.NewRowKey("empty_bytes")])
	require.Empty(t, decoded[wkk.NewRowKey("empty_bytes")])
	require.NotNil(t, decoded[wkk.NewRowKey("empty_int8s")])
	require.Empty(t, decoded[wkk.NewRowKey("empty_int8s")])
	require.NotNil(t, decoded[wkk.NewRowKey("empty_int16s")])
	require.Empty(t, decoded[wkk.NewRowKey("empty_int16s")])
	require.NotNil(t, decoded[wkk.NewRowKey("empty_int32s")])
	require.Empty(t, decoded[wkk.NewRowKey("empty_int32s")])
	require.NotNil(t, decoded[wkk.NewRowKey("empty_int64s")])
	require.Empty(t, decoded[wkk.NewRowKey("empty_int64s")])
	require.NotNil(t, decoded[wkk.NewRowKey("empty_f32s")])
	require.Empty(t, decoded[wkk.NewRowKey("empty_f32s")])
	require.NotNil(t, decoded[wkk.NewRowKey("empty_f64s")])
	require.Empty(t, decoded[wkk.NewRowKey("empty_f64s")])
	require.NotNil(t, decoded[wkk.NewRowKey("empty_strs")])
	require.Empty(t, decoded[wkk.NewRowKey("empty_strs")])
	require.NotNil(t, decoded[wkk.NewRowKey("empty_bools")])
	require.Empty(t, decoded[wkk.NewRowKey("empty_bools")])
}
