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

// Package rowcodec provides a custom binary encoding/decoding for Row data
// (map[string]any) optimized for memory efficiency and performance.
//
// Binary Format:
//
//	Map: [uint32: num_pairs] + pairs
//	Pair: [uint32: key_len][key_bytes][type_byte][value_data]
//
// Type bytes:
//
//	0 = nil
//	1 = bool (1 byte: 0=false, 1=true)
//	2 = int64 (8 bytes, little endian)
//	3 = float64 (8 bytes, little endian)
//	4 = string ([uint32: len][bytes])
//	5 = []byte ([uint32: len][bytes])
//	6 = int32 (4 bytes, little endian)
//	7 = float32 (4 bytes, little endian)
//
// This format avoids reflection entirely for maximum memory efficiency.
package rowcodec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

const (
	typeNil          = byte(0)
	typeBool         = byte(1)
	typeInt64        = byte(2)
	typeFloat64      = byte(3)
	typeString       = byte(4)
	typeBytes        = byte(5)
	typeInt32        = byte(6)
	typeFloat32      = byte(7)
	typeInt64Slice   = byte(8)
	typeFloat64Slice = byte(9)
	typeStringSlice  = byte(10)
)

// mapPool provides reusable map[string]any instances to reduce allocations
var mapPool = sync.Pool{
	New: func() any {
		return make(map[string]any, 32) // Start with reasonable capacity
	},
}

// bytePool provides reusable byte slices to reduce allocations during decoding
var bytePool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 256) // Start with 256 byte capacity
		return &buf
	},
}

// getPooledMap gets a map from the pool and clears it
func getPooledMap() map[string]any {
	m := mapPool.Get().(map[string]any)
	// Clear the map for reuse
	for k := range m {
		delete(m, k)
	}
	return m
}

// returnPooledMap returns a map to the pool
func returnPooledMap(m map[string]any) {
	if len(m) > 256 { // Don't pool overly large maps
		return
	}
	mapPool.Put(m)
}

// ReturnMap returns a decoded map back to the pool for reuse.
// Call this when you're done with a map returned by Decode().
func (c *Config) ReturnMap(m map[string]any) {
	returnPooledMap(m)
}

// getPooledBytes gets a byte slice from the pool with at least the requested capacity
func getPooledBytes(minCap int) []byte {
	bufPtr := bytePool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < minCap {
		// Return insufficient buffer and create new one
		bytePool.Put(bufPtr)
		return make([]byte, minCap)
	}
	return buf[:minCap] // Slice to exact length needed
}

// returnPooledBytes returns a byte slice to the pool
func returnPooledBytes(buf []byte) {
	if cap(buf) > 4096 { // Don't pool overly large buffers
		return
	}
	resetBuf := buf[:0] // Reset length to 0 but keep capacity
	bytePool.Put(&resetBuf)
}

// Config holds encoder/decoder for the custom binary format.
type Config struct{}

// NewConfig creates a new custom binary format configuration.
func NewConfig() (*Config, error) {
	return &Config{}, nil
}

// NewEncoder creates a new binary encoder.
func (c *Config) NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

// NewDecoder creates a new binary decoder.
func (c *Config) NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

// NewRowEncoder creates a new Row encoder that preserves RowKey handles.
func (c *Config) NewRowEncoder(w io.Writer) *RowEncoder {
	return &RowEncoder{w: w}
}

// NewRowDecoder creates a new Row decoder that preserves RowKey handles.
func (c *Config) NewRowDecoder(r io.Reader) *RowDecoder {
	return &RowDecoder{r: r}
}

// Encode encodes a map[string]any to binary bytes.
func (c *Config) Encode(row map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := c.NewEncoder(&buf)
	if err := encoder.Encode(row); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes binary bytes to a map[string]any.
func (c *Config) Decode(data []byte) (map[string]any, error) {
	buf := bytes.NewReader(data)
	decoder := c.NewDecoder(buf)
	return decoder.Decode()
}

// DecodeInto decodes binary bytes into the provided map, clearing it first.
// This avoids allocating a new map and can reuse pooled maps.
func (c *Config) DecodeInto(data []byte, target map[string]any) error {
	// Clear the target map
	for k := range target {
		delete(target, k)
	}

	buf := bytes.NewReader(data)
	decoder := c.NewDecoder(buf)
	return decoder.DecodeInto(target)
}

// EncodeRow encodes a Row (map[wkk.RowKey]any) to binary bytes.
func (c *Config) EncodeRow(row pipeline.Row) ([]byte, error) {
	var buf bytes.Buffer
	encoder := c.NewRowEncoder(&buf)
	if err := encoder.EncodeRow(row); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeRow decodes binary bytes to a Row (map[wkk.RowKey]any).
func (c *Config) DecodeRow(data []byte) (pipeline.Row, error) {
	buf := bytes.NewReader(data)
	decoder := c.NewRowDecoder(buf)
	return decoder.DecodeRow()
}

// DecodeRowInto decodes binary bytes into the provided Row, clearing it first.
func (c *Config) DecodeRowInto(data []byte, target pipeline.Row) error {
	// Clear the target row
	for k := range target {
		delete(target, k)
	}

	buf := bytes.NewReader(data)
	decoder := c.NewRowDecoder(buf)
	return decoder.DecodeRowInto(target)
}

// Encoder writes binary-encoded map data to an io.Writer.
type Encoder struct {
	w io.Writer
}

// Encode writes a map[string]any in binary format.
func (e *Encoder) Encode(row map[string]any) error {
	// Write number of key-value pairs
	if err := binary.Write(e.w, binary.LittleEndian, uint32(len(row))); err != nil {
		return fmt.Errorf("write map length: %w", err)
	}

	// Write each key-value pair
	for key, value := range row {
		if err := e.writeKeyValue(key, value); err != nil {
			return fmt.Errorf("write key-value %q: %w", key, err)
		}
	}

	return nil
}

func (e *Encoder) writeKeyValue(key string, value any) error {
	// Write key length and bytes
	keyBytes := []byte(key)
	if err := binary.Write(e.w, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
		return fmt.Errorf("write key length: %w", err)
	}
	if _, err := e.w.Write(keyBytes); err != nil {
		return fmt.Errorf("write key bytes: %w", err)
	}

	// Write type byte and value data
	switch v := value.(type) {
	case nil:
		_, err := e.w.Write([]byte{typeNil})
		return err
	case bool:
		if _, err := e.w.Write([]byte{typeBool}); err != nil {
			return err
		}
		var b byte
		if v {
			b = 1
		}
		_, err := e.w.Write([]byte{b})
		return err
	case int64:
		if _, err := e.w.Write([]byte{typeInt64}); err != nil {
			return err
		}
		return binary.Write(e.w, binary.LittleEndian, v)
	case int32:
		if _, err := e.w.Write([]byte{typeInt32}); err != nil {
			return err
		}
		return binary.Write(e.w, binary.LittleEndian, v)
	case float64:
		if _, err := e.w.Write([]byte{typeFloat64}); err != nil {
			return err
		}
		return binary.Write(e.w, binary.LittleEndian, math.Float64bits(v))
	case float32:
		if _, err := e.w.Write([]byte{typeFloat32}); err != nil {
			return err
		}
		return binary.Write(e.w, binary.LittleEndian, math.Float32bits(v))
	case string:
		if _, err := e.w.Write([]byte{typeString}); err != nil {
			return err
		}
		strBytes := []byte(v)
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(strBytes))); err != nil {
			return err
		}
		_, err := e.w.Write(strBytes)
		return err
	case []byte:
		if _, err := e.w.Write([]byte{typeBytes}); err != nil {
			return err
		}
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		_, err := e.w.Write(v)
		return err
	case []int64:
		if _, err := e.w.Write([]byte{typeInt64Slice}); err != nil {
			return err
		}
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		for _, item := range v {
			if err := binary.Write(e.w, binary.LittleEndian, item); err != nil {
				return err
			}
		}
		return nil
	case []float64:
		if _, err := e.w.Write([]byte{typeFloat64Slice}); err != nil {
			return err
		}
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		for _, item := range v {
			if err := binary.Write(e.w, binary.LittleEndian, math.Float64bits(item)); err != nil {
				return err
			}
		}
		return nil
	case []string:
		if _, err := e.w.Write([]byte{typeStringSlice}); err != nil {
			return err
		}
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		for _, item := range v {
			itemBytes := []byte(item)
			if err := binary.Write(e.w, binary.LittleEndian, uint32(len(itemBytes))); err != nil {
				return err
			}
			if _, err := e.w.Write(itemBytes); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported type %T for key %q", value, key)
	}
}

// Decoder reads binary-encoded map data from an io.Reader.
type Decoder struct {
	r io.Reader
}

// Decode reads a map[string]any from binary format.
func (d *Decoder) Decode() (map[string]any, error) {
	row := getPooledMap()
	err := d.DecodeInto(row)
	if err != nil {
		returnPooledMap(row)
		return nil, err
	}
	return row, nil
}

// DecodeInto reads binary data into the provided map, which should be empty or will be cleared.
func (d *Decoder) DecodeInto(target map[string]any) error {
	// Read number of key-value pairs
	var numPairs uint32
	if err := binary.Read(d.r, binary.LittleEndian, &numPairs); err != nil {
		return fmt.Errorf("read map length: %w", err)
	}

	// Read each key-value pair
	for i := uint32(0); i < numPairs; i++ {
		key, value, err := d.readKeyValue()
		if err != nil {
			return fmt.Errorf("read key-value %d: %w", i, err)
		}
		target[key] = value
	}

	return nil
}

func (d *Decoder) readKeyValue() (string, any, error) {
	// Read key length and bytes
	var keyLen uint32
	if err := binary.Read(d.r, binary.LittleEndian, &keyLen); err != nil {
		return "", nil, fmt.Errorf("read key length: %w", err)
	}

	keyBytes := getPooledBytes(int(keyLen))
	defer returnPooledBytes(keyBytes)
	if _, err := io.ReadFull(d.r, keyBytes); err != nil {
		return "", nil, fmt.Errorf("read key bytes: %w", err)
	}
	key := string(keyBytes)

	// Read type byte
	var typeByte [1]byte
	if _, err := io.ReadFull(d.r, typeByte[:]); err != nil {
		return "", nil, fmt.Errorf("read type byte: %w", err)
	}

	// Read value based on type
	switch typeByte[0] {
	case typeNil:
		return key, nil, nil
	case typeBool:
		var b [1]byte
		if _, err := io.ReadFull(d.r, b[:]); err != nil {
			return "", nil, fmt.Errorf("read bool: %w", err)
		}
		return key, b[0] != 0, nil
	case typeInt64:
		var v int64
		if err := binary.Read(d.r, binary.LittleEndian, &v); err != nil {
			return "", nil, fmt.Errorf("read int64: %w", err)
		}
		return key, v, nil
	case typeInt32:
		var v int32
		if err := binary.Read(d.r, binary.LittleEndian, &v); err != nil {
			return "", nil, fmt.Errorf("read int32: %w", err)
		}
		return key, v, nil
	case typeFloat64:
		var bits uint64
		if err := binary.Read(d.r, binary.LittleEndian, &bits); err != nil {
			return "", nil, fmt.Errorf("read float64 bits: %w", err)
		}
		return key, math.Float64frombits(bits), nil
	case typeFloat32:
		var bits uint32
		if err := binary.Read(d.r, binary.LittleEndian, &bits); err != nil {
			return "", nil, fmt.Errorf("read float32 bits: %w", err)
		}
		return key, math.Float32frombits(bits), nil
	case typeString:
		var strLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &strLen); err != nil {
			return "", nil, fmt.Errorf("read string length: %w", err)
		}
		strBytes := getPooledBytes(int(strLen))
		defer returnPooledBytes(strBytes)
		if _, err := io.ReadFull(d.r, strBytes); err != nil {
			return "", nil, fmt.Errorf("read string bytes: %w", err)
		}
		return key, string(strBytes), nil
	case typeBytes:
		var bytesLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &bytesLen); err != nil {
			return "", nil, fmt.Errorf("read bytes length: %w", err)
		}
		data := getPooledBytes(int(bytesLen))
		defer returnPooledBytes(data)
		if _, err := io.ReadFull(d.r, data); err != nil {
			return "", nil, fmt.Errorf("read bytes data: %w", err)
		}
		// Make a copy since we're returning the buffer to the pool
		result := make([]byte, bytesLen)
		copy(result, data)
		return key, result, nil
	case typeInt64Slice:
		var sliceLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &sliceLen); err != nil {
			return "", nil, fmt.Errorf("read int64 slice length: %w", err)
		}
		slice := make([]int64, sliceLen)
		for i := range slice {
			if err := binary.Read(d.r, binary.LittleEndian, &slice[i]); err != nil {
				return "", nil, fmt.Errorf("read int64 slice item %d: %w", i, err)
			}
		}
		return key, slice, nil
	case typeFloat64Slice:
		var sliceLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &sliceLen); err != nil {
			return "", nil, fmt.Errorf("read float64 slice length: %w", err)
		}
		slice := make([]float64, sliceLen)
		for i := range slice {
			var bits uint64
			if err := binary.Read(d.r, binary.LittleEndian, &bits); err != nil {
				return "", nil, fmt.Errorf("read float64 slice item %d: %w", i, err)
			}
			slice[i] = math.Float64frombits(bits)
		}
		return key, slice, nil
	case typeStringSlice:
		var sliceLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &sliceLen); err != nil {
			return "", nil, fmt.Errorf("read string slice length: %w", err)
		}
		slice := make([]string, sliceLen)
		for i := range slice {
			var strLen uint32
			if err := binary.Read(d.r, binary.LittleEndian, &strLen); err != nil {
				return "", nil, fmt.Errorf("read string slice item %d length: %w", i, err)
			}
			strBytes := getPooledBytes(int(strLen))
			if _, err := io.ReadFull(d.r, strBytes); err != nil {
				returnPooledBytes(strBytes)
				return "", nil, fmt.Errorf("read string slice item %d data: %w", i, err)
			}
			slice[i] = string(strBytes)
			returnPooledBytes(strBytes)
		}
		return key, slice, nil
	default:
		return "", nil, fmt.Errorf("unknown type byte: %d", typeByte[0])
	}
}

// RowEncoder writes binary-encoded Row data to an io.Writer, preserving RowKey handles.
type RowEncoder struct {
	w io.Writer
}

// EncodeRow writes a pipeline.Row in binary format without converting keys to strings.
func (e *RowEncoder) EncodeRow(row pipeline.Row) error {
	// Write number of key-value pairs
	if err := binary.Write(e.w, binary.LittleEndian, uint32(len(row))); err != nil {
		return fmt.Errorf("write map length: %w", err)
	}

	// Write each key-value pair
	for rowKey, value := range row {
		if err := e.writeRowKeyValue(rowKey, value); err != nil {
			return fmt.Errorf("write key-value %q: %w", rowKey.Value(), err)
		}
	}

	return nil
}

func (e *RowEncoder) writeRowKeyValue(key wkk.RowKey, value any) error {
	// Write key as string (since we need to serialize the underlying string)
	keyStr := string(key.Value())
	keyBytes := []byte(keyStr)
	if err := binary.Write(e.w, binary.LittleEndian, uint32(len(keyBytes))); err != nil {
		return fmt.Errorf("write key length: %w", err)
	}
	if _, err := e.w.Write(keyBytes); err != nil {
		return fmt.Errorf("write key bytes: %w", err)
	}

	// Write type byte and value data (reuse same logic as Encoder)
	switch v := value.(type) {
	case nil:
		_, err := e.w.Write([]byte{typeNil})
		return err
	case bool:
		if _, err := e.w.Write([]byte{typeBool}); err != nil {
			return err
		}
		var b byte
		if v {
			b = 1
		}
		_, err := e.w.Write([]byte{b})
		return err
	case int64:
		if _, err := e.w.Write([]byte{typeInt64}); err != nil {
			return err
		}
		return binary.Write(e.w, binary.LittleEndian, v)
	case int32:
		if _, err := e.w.Write([]byte{typeInt32}); err != nil {
			return err
		}
		return binary.Write(e.w, binary.LittleEndian, v)
	case float64:
		if _, err := e.w.Write([]byte{typeFloat64}); err != nil {
			return err
		}
		return binary.Write(e.w, binary.LittleEndian, math.Float64bits(v))
	case float32:
		if _, err := e.w.Write([]byte{typeFloat32}); err != nil {
			return err
		}
		return binary.Write(e.w, binary.LittleEndian, math.Float32bits(v))
	case string:
		if _, err := e.w.Write([]byte{typeString}); err != nil {
			return err
		}
		strBytes := []byte(v)
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(strBytes))); err != nil {
			return err
		}
		_, err := e.w.Write(strBytes)
		return err
	case []byte:
		if _, err := e.w.Write([]byte{typeBytes}); err != nil {
			return err
		}
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		_, err := e.w.Write(v)
		return err
	case []int64:
		if _, err := e.w.Write([]byte{typeInt64Slice}); err != nil {
			return err
		}
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		for _, item := range v {
			if err := binary.Write(e.w, binary.LittleEndian, item); err != nil {
				return err
			}
		}
		return nil
	case []float64:
		if _, err := e.w.Write([]byte{typeFloat64Slice}); err != nil {
			return err
		}
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		for _, item := range v {
			if err := binary.Write(e.w, binary.LittleEndian, math.Float64bits(item)); err != nil {
				return err
			}
		}
		return nil
	case []string:
		if _, err := e.w.Write([]byte{typeStringSlice}); err != nil {
			return err
		}
		if err := binary.Write(e.w, binary.LittleEndian, uint32(len(v))); err != nil {
			return err
		}
		for _, item := range v {
			itemBytes := []byte(item)
			if err := binary.Write(e.w, binary.LittleEndian, uint32(len(itemBytes))); err != nil {
				return err
			}
			if _, err := e.w.Write(itemBytes); err != nil {
				return err
			}
		}
		return nil
	default:
		return fmt.Errorf("unsupported type %T for key %q", value, string(key.Value()))
	}
}

// RowDecoder reads binary-encoded Row data from an io.Reader, preserving RowKey handles.
type RowDecoder struct {
	r io.Reader
}

// DecodeRow reads a pipeline.Row from binary format, preserving RowKey handles.
func (d *RowDecoder) DecodeRow() (pipeline.Row, error) {
	// Read number of key-value pairs
	var numPairs uint32
	if err := binary.Read(d.r, binary.LittleEndian, &numPairs); err != nil {
		return nil, fmt.Errorf("read map length: %w", err)
	}

	row := make(pipeline.Row, numPairs)

	// Read each key-value pair
	for i := uint32(0); i < numPairs; i++ {
		key, value, err := d.readRowKeyValue()
		if err != nil {
			return nil, fmt.Errorf("read key-value %d: %w", i, err)
		}
		row[key] = value
	}

	return row, nil
}

// DecodeRowInto reads binary data into the provided Row.
func (d *RowDecoder) DecodeRowInto(target pipeline.Row) error {
	// Read number of key-value pairs
	var numPairs uint32
	if err := binary.Read(d.r, binary.LittleEndian, &numPairs); err != nil {
		return fmt.Errorf("read map length: %w", err)
	}

	// Read each key-value pair
	for i := uint32(0); i < numPairs; i++ {
		key, value, err := d.readRowKeyValue()
		if err != nil {
			return fmt.Errorf("read key-value %d: %w", i, err)
		}
		target[key] = value
	}

	return nil
}

func (d *RowDecoder) readRowKeyValue() (wkk.RowKey, any, error) {
	// Read key length and bytes
	var keyLen uint32
	if err := binary.Read(d.r, binary.LittleEndian, &keyLen); err != nil {
		return wkk.RowKey{}, nil, fmt.Errorf("read key length: %w", err)
	}

	keyBytes := getPooledBytes(int(keyLen))
	defer returnPooledBytes(keyBytes)
	if _, err := io.ReadFull(d.r, keyBytes); err != nil {
		return wkk.RowKey{}, nil, fmt.Errorf("read key bytes: %w", err)
	}
	// Create RowKey handle from bytes - avoids string allocation for common keys
	key := wkk.NewRowKeyFromBytes(keyBytes)

	// Read type byte
	var typeByte [1]byte
	if _, err := io.ReadFull(d.r, typeByte[:]); err != nil {
		return wkk.RowKey{}, nil, fmt.Errorf("read type byte: %w", err)
	}

	// Read value based on type (reuse same logic as Decoder)
	switch typeByte[0] {
	case typeNil:
		return key, nil, nil
	case typeBool:
		var b [1]byte
		if _, err := io.ReadFull(d.r, b[:]); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read bool: %w", err)
		}
		return key, b[0] != 0, nil
	case typeInt64:
		var v int64
		if err := binary.Read(d.r, binary.LittleEndian, &v); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read int64: %w", err)
		}
		return key, v, nil
	case typeInt32:
		var v int32
		if err := binary.Read(d.r, binary.LittleEndian, &v); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read int32: %w", err)
		}
		return key, v, nil
	case typeFloat64:
		var bits uint64
		if err := binary.Read(d.r, binary.LittleEndian, &bits); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read float64 bits: %w", err)
		}
		return key, math.Float64frombits(bits), nil
	case typeFloat32:
		var bits uint32
		if err := binary.Read(d.r, binary.LittleEndian, &bits); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read float32 bits: %w", err)
		}
		return key, math.Float32frombits(bits), nil
	case typeString:
		var strLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &strLen); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read string length: %w", err)
		}
		strBytes := getPooledBytes(int(strLen))
		defer returnPooledBytes(strBytes)
		if _, err := io.ReadFull(d.r, strBytes); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read string bytes: %w", err)
		}
		return key, string(strBytes), nil
	case typeBytes:
		var bytesLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &bytesLen); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read bytes length: %w", err)
		}
		data := getPooledBytes(int(bytesLen))
		defer returnPooledBytes(data)
		if _, err := io.ReadFull(d.r, data); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read bytes data: %w", err)
		}
		// Make a copy since we're returning the buffer to the pool
		result := make([]byte, bytesLen)
		copy(result, data)
		return key, result, nil
	case typeInt64Slice:
		var sliceLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &sliceLen); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read int64 slice length: %w", err)
		}
		slice := make([]int64, sliceLen)
		for i := range slice {
			if err := binary.Read(d.r, binary.LittleEndian, &slice[i]); err != nil {
				return wkk.RowKey{}, nil, fmt.Errorf("read int64 slice item %d: %w", i, err)
			}
		}
		return key, slice, nil
	case typeFloat64Slice:
		var sliceLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &sliceLen); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read float64 slice length: %w", err)
		}
		slice := make([]float64, sliceLen)
		for i := range slice {
			var bits uint64
			if err := binary.Read(d.r, binary.LittleEndian, &bits); err != nil {
				return wkk.RowKey{}, nil, fmt.Errorf("read float64 slice item %d: %w", i, err)
			}
			slice[i] = math.Float64frombits(bits)
		}
		return key, slice, nil
	case typeStringSlice:
		var sliceLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &sliceLen); err != nil {
			return wkk.RowKey{}, nil, fmt.Errorf("read string slice length: %w", err)
		}
		slice := make([]string, sliceLen)
		for i := range slice {
			var strLen uint32
			if err := binary.Read(d.r, binary.LittleEndian, &strLen); err != nil {
				return wkk.RowKey{}, nil, fmt.Errorf("read string slice item %d length: %w", i, err)
			}
			strBytes := getPooledBytes(int(strLen))
			if _, err := io.ReadFull(d.r, strBytes); err != nil {
				returnPooledBytes(strBytes)
				return wkk.RowKey{}, nil, fmt.Errorf("read string slice item %d data: %w", i, err)
			}
			slice[i] = string(strBytes)
			returnPooledBytes(strBytes)
		}
		return key, slice, nil
	default:
		return wkk.RowKey{}, nil, fmt.Errorf("unknown type byte: %d", typeByte[0])
	}
}
