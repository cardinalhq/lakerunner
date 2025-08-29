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

// EncodeRow encodes a Row (map[wkk.RowKey]any) to binary bytes.
func (c *Config) EncodeRow(row pipeline.Row) ([]byte, error) {
	stringMap := pipeline.ToStringMap(row)
	return c.Encode(stringMap)
}

// DecodeRow decodes binary bytes to a Row (map[wkk.RowKey]any).
func (c *Config) DecodeRow(data []byte) (pipeline.Row, error) {
	stringMap, err := c.Decode(data)
	if err != nil {
		return nil, err
	}

	// Convert string keys back to RowKeys
	row := make(pipeline.Row, len(stringMap))
	for k, v := range stringMap {
		row[wkk.NewRowKey(k)] = v
	}

	return row, nil
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
	// Read number of key-value pairs
	var numPairs uint32
	if err := binary.Read(d.r, binary.LittleEndian, &numPairs); err != nil {
		return nil, fmt.Errorf("read map length: %w", err)
	}

	row := make(map[string]any, numPairs)

	// Read each key-value pair
	for i := uint32(0); i < numPairs; i++ {
		key, value, err := d.readKeyValue()
		if err != nil {
			return nil, fmt.Errorf("read key-value %d: %w", i, err)
		}
		row[key] = value
	}

	return row, nil
}

func (d *Decoder) readKeyValue() (string, any, error) {
	// Read key length and bytes
	var keyLen uint32
	if err := binary.Read(d.r, binary.LittleEndian, &keyLen); err != nil {
		return "", nil, fmt.Errorf("read key length: %w", err)
	}

	keyBytes := make([]byte, keyLen)
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
		strBytes := make([]byte, strLen)
		if _, err := io.ReadFull(d.r, strBytes); err != nil {
			return "", nil, fmt.Errorf("read string bytes: %w", err)
		}
		return key, string(strBytes), nil
	case typeBytes:
		var bytesLen uint32
		if err := binary.Read(d.r, binary.LittleEndian, &bytesLen); err != nil {
			return "", nil, fmt.Errorf("read bytes length: %w", err)
		}
		data := make([]byte, bytesLen)
		if _, err := io.ReadFull(d.r, data); err != nil {
			return "", nil, fmt.Errorf("read bytes data: %w", err)
		}
		return key, data, nil
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
			strBytes := make([]byte, strLen)
			if _, err := io.ReadFull(d.r, strBytes); err != nil {
				return "", nil, fmt.Errorf("read string slice item %d data: %w", i, err)
			}
			slice[i] = string(strBytes)
		}
		return key, slice, nil
	default:
		return "", nil, fmt.Errorf("unknown type byte: %d", typeByte[0])
	}
}
