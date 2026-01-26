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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sync"
	"unique"
	"unsafe"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// stringBufPool provides reusable byte buffers for string reading.
// Buffers are grown as needed and returned to the pool after use.
var stringBufPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 256)
		return &buf
	},
}

// SpillCodec provides a compact, low-allocation binary encoding intended for
// process-local spill files used by DiskSortingReader. The format is not stable
// across process restarts and intentionally relies on a process-level RowKey
// dictionary to avoid per-row string serialization.
//
// SpillCodec is NOT safe for concurrent use. Each goroutine should use its own
// instance. The underlying key dictionary is shared and thread-safe, but the
// scratch buffers within each codec instance are not.
type SpillCodec struct {
	// Scratch buffers reused across calls to avoid per-row allocations.
	varintBuf [binary.MaxVarintLen64]byte
	scalarBuf [9]byte
	// Embedded byteReader to avoid allocation in readUvarint.
	br byteReader
}

// Spill type tags. Only the explicitly listed primitive types are supported.
const (
	spillTagNil byte = iota + 1
	spillTagByte
	spillTagBytes
	spillTagInt8
	spillTagInt8Slice
	spillTagInt16
	spillTagInt16Slice
	spillTagInt32
	spillTagInt32Slice
	spillTagInt64
	spillTagInt64Slice
	spillTagFloat32
	spillTagFloat32Slice
	spillTagFloat64
	spillTagFloat64Slice
	spillTagString
	spillTagStringSlice
	spillTagBool
	spillTagBoolSlice

	// Nil slice tags - preserve typed nil vs empty slice distinction
	spillTagNilBytes
	spillTagNilInt8Slice
	spillTagNilInt16Slice
	spillTagNilInt32Slice
	spillTagNilInt64Slice
	spillTagNilFloat32Slice
	spillTagNilFloat64Slice
	spillTagNilStringSlice
	spillTagNilBoolSlice
)

var (
	spillKeyMu   sync.RWMutex
	spillKeyToID = make(map[wkk.RowKey]uint32)
	spillIDToKey = make([]wkk.RowKey, 0, 256)
)

// NewSpillCodec returns a SpillCodec instance.
func NewSpillCodec() *SpillCodec {
	return &SpillCodec{}
}

// EncodeRowTo writes the provided row to w using the spill format and returns
// the number of payload bytes written (excluding the length prefix). No
// per-row byte slices are created; the encoder streams directly to the writer
// using small reusable scratch buffers.
func (c *SpillCodec) EncodeRowTo(w io.Writer, row pipeline.Row) (int32, error) {
	// Field count encoded first.
	fieldCount := uint64(len(row))
	n := binary.PutUvarint(c.varintBuf[:], fieldCount)
	written, err := w.Write(c.varintBuf[:n])
	if err != nil {
		return 0, err
	}
	byteLen := int32(written)

	for key, value := range row {
		keyID := c.ensureKeyID(key)
		binary.LittleEndian.PutUint32(c.scalarBuf[:4], keyID)
		if _, err := w.Write(c.scalarBuf[:4]); err != nil {
			return 0, err
		}
		byteLen += 4

		switch v := value.(type) {
		case nil:
			count, err := c.writeNil(w)
			byteLen += count
			if err != nil {
				return 0, err
			}
		case byte:
			if err := c.writeByte(w, spillTagByte, v); err != nil {
				return 0, err
			}
			byteLen += 2
		case []byte:
			if v == nil {
				count, err := c.writeNilTag(w, spillTagNilBytes)
				byteLen += count
				if err != nil {
					return 0, err
				}
			} else {
				count, err := c.writeBytes(w, spillTagBytes, v)
				byteLen += count
				if err != nil {
					return 0, err
				}
			}
		case int8:
			if err := c.writeByte(w, spillTagInt8, byte(v)); err != nil {
				return 0, err
			}
			byteLen += 2
		case []int8:
			if v == nil {
				count, err := c.writeNilTag(w, spillTagNilInt8Slice)
				byteLen += count
				if err != nil {
					return 0, err
				}
			} else {
				count, err := c.writeInt8Slice(w, spillTagInt8Slice, v)
				byteLen += count
				if err != nil {
					return 0, err
				}
			}
		case int16:
			count, err := c.writeFixedScalar(w, spillTagInt16, uint64(v), 2)
			byteLen += count
			if err != nil {
				return 0, err
			}
		case []int16:
			if v == nil {
				count, err := c.writeNilTag(w, spillTagNilInt16Slice)
				byteLen += count
				if err != nil {
					return 0, err
				}
			} else {
				count, err := c.writeInt16Slice(w, spillTagInt16Slice, v)
				byteLen += count
				if err != nil {
					return 0, err
				}
			}
		case int32:
			count, err := c.writeFixedScalar(w, spillTagInt32, uint64(v), 4)
			byteLen += count
			if err != nil {
				return 0, err
			}
		case []int32:
			if v == nil {
				count, err := c.writeNilTag(w, spillTagNilInt32Slice)
				byteLen += count
				if err != nil {
					return 0, err
				}
			} else {
				count, err := c.writeInt32Slice(w, spillTagInt32Slice, v)
				byteLen += count
				if err != nil {
					return 0, err
				}
			}
		case int64:
			count, err := c.writeFixedScalar(w, spillTagInt64, uint64(v), 8)
			byteLen += count
			if err != nil {
				return 0, err
			}
		case []int64:
			if v == nil {
				count, err := c.writeNilTag(w, spillTagNilInt64Slice)
				byteLen += count
				if err != nil {
					return 0, err
				}
			} else {
				count, err := c.writeInt64Slice(w, spillTagInt64Slice, v)
				byteLen += count
				if err != nil {
					return 0, err
				}
			}
		case float32:
			count, err := c.writeFloat32(w, spillTagFloat32, v)
			byteLen += count
			if err != nil {
				return 0, err
			}
		case []float32:
			if v == nil {
				count, err := c.writeNilTag(w, spillTagNilFloat32Slice)
				byteLen += count
				if err != nil {
					return 0, err
				}
			} else {
				count, err := c.writeFloat32Slice(w, spillTagFloat32Slice, v)
				byteLen += count
				if err != nil {
					return 0, err
				}
			}
		case float64:
			count, err := c.writeFloat64(w, spillTagFloat64, v)
			byteLen += count
			if err != nil {
				return 0, err
			}
		case []float64:
			if v == nil {
				count, err := c.writeNilTag(w, spillTagNilFloat64Slice)
				byteLen += count
				if err != nil {
					return 0, err
				}
			} else {
				count, err := c.writeFloat64Slice(w, spillTagFloat64Slice, v)
				byteLen += count
				if err != nil {
					return 0, err
				}
			}
		case string:
			count, err := c.writeString(w, spillTagString, v)
			byteLen += count
			if err != nil {
				return 0, err
			}
		case []string:
			if v == nil {
				count, err := c.writeNilTag(w, spillTagNilStringSlice)
				byteLen += count
				if err != nil {
					return 0, err
				}
			} else {
				count, err := c.writeStringSlice(w, spillTagStringSlice, v)
				byteLen += count
				if err != nil {
					return 0, err
				}
			}
		case bool:
			if err := c.writeByte(w, spillTagBool, boolToByte(v)); err != nil {
				return 0, err
			}
			byteLen += 2
		case []bool:
			if v == nil {
				count, err := c.writeNilTag(w, spillTagNilBoolSlice)
				byteLen += count
				if err != nil {
					return 0, err
				}
			} else {
				count, err := c.writeBoolSlice(w, spillTagBoolSlice, v)
				byteLen += count
				if err != nil {
					return 0, err
				}
			}
		default:
			return 0, fmt.Errorf("unsupported type %T", v)
		}
	}

	return byteLen, nil
}

// DecodeRowFrom reads a spill-encoded row from r into dst, clearing dst first.
// The reader must be positioned at the start of the payload (after the length
// prefix). Field values are written directly into the provided Row map.
func (c *SpillCodec) DecodeRowFrom(r io.Reader, dst pipeline.Row) error {
	for k := range dst {
		delete(dst, k)
	}

	// Read field count using embedded byteReader.
	c.br.r = r
	fieldCount, err := binary.ReadUvarint(&c.br)
	if err != nil {
		return fmt.Errorf("read field count: %w", err)
	}

	var zeroKey wkk.RowKey

	for i := uint64(0); i < fieldCount; i++ {
		if _, err := io.ReadFull(r, c.scalarBuf[:4]); err != nil {
			return fmt.Errorf("read key id: %w", err)
		}
		keyID := binary.LittleEndian.Uint32(c.scalarBuf[:4])
		key := c.lookupKey(keyID)
		if key == zeroKey {
			return fmt.Errorf("unknown key id %d", keyID)
		}

		tag, err := c.br.ReadByte()
		if err != nil {
			return fmt.Errorf("read type tag: %w", err)
		}

		switch tag {
		case spillTagNil:
			dst[key] = nil
		case spillTagByte:
			if _, err := io.ReadFull(r, c.scalarBuf[:1]); err != nil {
				return err
			}
			dst[key] = c.scalarBuf[0]
		case spillTagBytes:
			data, err := c.readBytes(r)
			if err != nil {
				return err
			}
			dst[key] = data
		case spillTagInt8:
			if _, err := io.ReadFull(r, c.scalarBuf[:1]); err != nil {
				return err
			}
			dst[key] = int8(c.scalarBuf[0])
		case spillTagInt8Slice:
			v, err := c.readInt8Slice(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagInt16:
			v, err := c.readFixedInt(r, 2)
			if err != nil {
				return err
			}
			dst[key] = int16(v)
		case spillTagInt16Slice:
			v, err := c.readInt16Slice(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagInt32:
			v, err := c.readFixedInt(r, 4)
			if err != nil {
				return err
			}
			dst[key] = int32(v)
		case spillTagInt32Slice:
			v, err := c.readInt32Slice(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagInt64:
			v, err := c.readFixedInt(r, 8)
			if err != nil {
				return err
			}
			dst[key] = int64(v)
		case spillTagInt64Slice:
			v, err := c.readInt64Slice(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagFloat32:
			v, err := c.readFloat32(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagFloat32Slice:
			v, err := c.readFloat32Slice(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagFloat64:
			v, err := c.readFloat64(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagFloat64Slice:
			v, err := c.readFloat64Slice(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagString:
			v, err := c.readString(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagStringSlice:
			v, err := c.readStringSlice(r)
			if err != nil {
				return err
			}
			dst[key] = v
		case spillTagBool:
			if _, err := io.ReadFull(r, c.scalarBuf[:1]); err != nil {
				return err
			}
			dst[key] = c.scalarBuf[0] == 1
		case spillTagBoolSlice:
			v, err := c.readBoolSlice(r)
			if err != nil {
				return err
			}
			dst[key] = v

		// Nil slice tags - return typed nil slices
		case spillTagNilBytes:
			dst[key] = []byte(nil)
		case spillTagNilInt8Slice:
			dst[key] = []int8(nil)
		case spillTagNilInt16Slice:
			dst[key] = []int16(nil)
		case spillTagNilInt32Slice:
			dst[key] = []int32(nil)
		case spillTagNilInt64Slice:
			dst[key] = []int64(nil)
		case spillTagNilFloat32Slice:
			dst[key] = []float32(nil)
		case spillTagNilFloat64Slice:
			dst[key] = []float64(nil)
		case spillTagNilStringSlice:
			dst[key] = []string(nil)
		case spillTagNilBoolSlice:
			dst[key] = []bool(nil)

		default:
			return fmt.Errorf("unknown type tag %d", tag)
		}
	}

	return nil
}

func (c *SpillCodec) ensureKeyID(key wkk.RowKey) uint32 {
	spillKeyMu.RLock()
	id, ok := spillKeyToID[key]
	spillKeyMu.RUnlock()
	if ok {
		return id
	}

	spillKeyMu.Lock()
	defer spillKeyMu.Unlock()
	if id, ok := spillKeyToID[key]; ok {
		return id
	}
	if len(spillIDToKey) >= math.MaxUint32 {
		panic("spill key dictionary overflow: too many unique keys")
	}
	id = uint32(len(spillIDToKey))
	spillKeyToID[key] = id
	spillIDToKey = append(spillIDToKey, key)
	return id
}

func (c *SpillCodec) lookupKey(id uint32) wkk.RowKey {
	spillKeyMu.RLock()
	defer spillKeyMu.RUnlock()
	if int(id) >= len(spillIDToKey) {
		var zero wkk.RowKey
		return zero
	}
	return spillIDToKey[id]
}

func (c *SpillCodec) writeByte(w io.Writer, tag byte, v byte) error {
	c.scalarBuf[0] = tag
	c.scalarBuf[1] = v
	_, err := w.Write(c.scalarBuf[:2])
	return err
}

func (c *SpillCodec) writeNil(w io.Writer) (int32, error) {
	c.scalarBuf[0] = spillTagNil
	_, err := w.Write(c.scalarBuf[:1])
	return 1, err
}

func (c *SpillCodec) writeNilTag(w io.Writer, tag byte) (int32, error) {
	c.scalarBuf[0] = tag
	_, err := w.Write(c.scalarBuf[:1])
	return 1, err
}

func (c *SpillCodec) writeFixedScalar(w io.Writer, tag byte, val uint64, width int) (int32, error) {
	c.scalarBuf[0] = tag
	switch width {
	case 2:
		binary.LittleEndian.PutUint16(c.scalarBuf[1:3], uint16(val))
		_, err := w.Write(c.scalarBuf[:3])
		return 3, err
	case 4:
		binary.LittleEndian.PutUint32(c.scalarBuf[1:5], uint32(val))
		_, err := w.Write(c.scalarBuf[:5])
		return 5, err
	case 8:
		binary.LittleEndian.PutUint64(c.scalarBuf[1:9], val)
		_, err := w.Write(c.scalarBuf[:9])
		return 9, err
	default:
		return 0, fmt.Errorf("invalid scalar width %d", width)
	}
}

func (c *SpillCodec) writeBytes(w io.Writer, tag byte, data []byte) (int32, error) {
	c.scalarBuf[0] = tag
	n := binary.PutUvarint(c.varintBuf[:], uint64(len(data)))
	if _, err := w.Write(c.scalarBuf[:1]); err != nil {
		return 0, err
	}
	if _, err := w.Write(c.varintBuf[:n]); err != nil {
		return 0, err
	}
	if _, err := w.Write(data); err != nil {
		return 0, err
	}
	return int32(1 + n + len(data)), nil
}

func (c *SpillCodec) writeInt8Slice(w io.Writer, tag byte, values []int8) (int32, error) {
	return c.writeBytes(w, tag, unsafeSliceToBytes(values))
}

func (c *SpillCodec) writeInt16Slice(w io.Writer, tag byte, values []int16) (int32, error) {
	c.scalarBuf[0] = tag
	n := binary.PutUvarint(c.varintBuf[:], uint64(len(values)))
	if _, err := w.Write(c.scalarBuf[:1]); err != nil {
		return 0, err
	}
	if _, err := w.Write(c.varintBuf[:n]); err != nil {
		return 0, err
	}
	var bytesWritten = int32(1 + n)
	for _, v := range values {
		binary.LittleEndian.PutUint16(c.scalarBuf[:2], uint16(v))
		if _, err := w.Write(c.scalarBuf[:2]); err != nil {
			return bytesWritten, err
		}
		bytesWritten += 2
	}
	return bytesWritten, nil
}

func (c *SpillCodec) writeInt32Slice(w io.Writer, tag byte, values []int32) (int32, error) {
	c.scalarBuf[0] = tag
	n := binary.PutUvarint(c.varintBuf[:], uint64(len(values)))
	if _, err := w.Write(c.scalarBuf[:1]); err != nil {
		return 0, err
	}
	if _, err := w.Write(c.varintBuf[:n]); err != nil {
		return 0, err
	}
	var bytesWritten = int32(1 + n)
	for _, v := range values {
		binary.LittleEndian.PutUint32(c.scalarBuf[:4], uint32(v))
		if _, err := w.Write(c.scalarBuf[:4]); err != nil {
			return bytesWritten, err
		}
		bytesWritten += 4
	}
	return bytesWritten, nil
}

func (c *SpillCodec) writeInt64Slice(w io.Writer, tag byte, values []int64) (int32, error) {
	c.scalarBuf[0] = tag
	n := binary.PutUvarint(c.varintBuf[:], uint64(len(values)))
	if _, err := w.Write(c.scalarBuf[:1]); err != nil {
		return 0, err
	}
	if _, err := w.Write(c.varintBuf[:n]); err != nil {
		return 0, err
	}
	var bytesWritten = int32(1 + n)
	for _, v := range values {
		binary.LittleEndian.PutUint64(c.scalarBuf[:8], uint64(v))
		if _, err := w.Write(c.scalarBuf[:8]); err != nil {
			return bytesWritten, err
		}
		bytesWritten += 8
	}
	return bytesWritten, nil
}

func (c *SpillCodec) writeFloat32(w io.Writer, tag byte, v float32) (int32, error) {
	c.scalarBuf[0] = tag
	binary.LittleEndian.PutUint32(c.scalarBuf[1:5], mathFloat32bits(v))
	_, err := w.Write(c.scalarBuf[:5])
	return 5, err
}

func (c *SpillCodec) writeFloat32Slice(w io.Writer, tag byte, values []float32) (int32, error) {
	c.scalarBuf[0] = tag
	n := binary.PutUvarint(c.varintBuf[:], uint64(len(values)))
	if _, err := w.Write(c.scalarBuf[:1]); err != nil {
		return 0, err
	}
	if _, err := w.Write(c.varintBuf[:n]); err != nil {
		return 0, err
	}
	var bytesWritten = int32(1 + n)
	for _, v := range values {
		binary.LittleEndian.PutUint32(c.scalarBuf[:4], mathFloat32bits(v))
		if _, err := w.Write(c.scalarBuf[:4]); err != nil {
			return bytesWritten, err
		}
		bytesWritten += 4
	}
	return bytesWritten, nil
}

func (c *SpillCodec) writeFloat64(w io.Writer, tag byte, v float64) (int32, error) {
	c.scalarBuf[0] = tag
	binary.LittleEndian.PutUint64(c.scalarBuf[1:9], mathFloat64bits(v))
	_, err := w.Write(c.scalarBuf[:9])
	return 9, err
}

func (c *SpillCodec) writeFloat64Slice(w io.Writer, tag byte, values []float64) (int32, error) {
	c.scalarBuf[0] = tag
	n := binary.PutUvarint(c.varintBuf[:], uint64(len(values)))
	if _, err := w.Write(c.scalarBuf[:1]); err != nil {
		return 0, err
	}
	if _, err := w.Write(c.varintBuf[:n]); err != nil {
		return 0, err
	}
	var bytesWritten = int32(1 + n)
	for _, v := range values {
		binary.LittleEndian.PutUint64(c.scalarBuf[:8], mathFloat64bits(v))
		if _, err := w.Write(c.scalarBuf[:8]); err != nil {
			return bytesWritten, err
		}
		bytesWritten += 8
	}
	return bytesWritten, nil
}

func (c *SpillCodec) writeString(w io.Writer, tag byte, s string) (int32, error) {
	c.scalarBuf[0] = tag
	n := binary.PutUvarint(c.varintBuf[:], uint64(len(s)))
	if _, err := w.Write(c.scalarBuf[:1]); err != nil {
		return 0, err
	}
	if _, err := w.Write(c.varintBuf[:n]); err != nil {
		return 0, err
	}
	if _, err := w.Write([]byte(s)); err != nil {
		return 0, err
	}
	return int32(1 + n + len(s)), nil
}

func (c *SpillCodec) writeStringSlice(w io.Writer, tag byte, values []string) (int32, error) {
	c.scalarBuf[0] = tag
	n := binary.PutUvarint(c.varintBuf[:], uint64(len(values)))
	if _, err := w.Write(c.scalarBuf[:1]); err != nil {
		return 0, err
	}
	if _, err := w.Write(c.varintBuf[:n]); err != nil {
		return 0, err
	}
	var bytesWritten = int32(1 + n)
	for _, s := range values {
		strLen := binary.PutUvarint(c.varintBuf[:], uint64(len(s)))
		if _, err := w.Write(c.varintBuf[:strLen]); err != nil {
			return bytesWritten, err
		}
		if _, err := w.Write([]byte(s)); err != nil {
			return bytesWritten, err
		}
		bytesWritten += int32(strLen + len(s))
	}
	return bytesWritten, nil
}

func (c *SpillCodec) writeBoolSlice(w io.Writer, tag byte, values []bool) (int32, error) {
	c.scalarBuf[0] = tag
	n := binary.PutUvarint(c.varintBuf[:], uint64(len(values)))
	if _, err := w.Write(c.scalarBuf[:1]); err != nil {
		return 0, err
	}
	if _, err := w.Write(c.varintBuf[:n]); err != nil {
		return 0, err
	}
	var bytesWritten = int32(1 + n)
	for _, v := range values {
		b := byte(0)
		if v {
			b = 1
		}
		c.scalarBuf[0] = b
		if _, err := w.Write(c.scalarBuf[:1]); err != nil {
			return bytesWritten, err
		}
		bytesWritten++
	}
	return bytesWritten, nil
}

func (c *SpillCodec) readBytes(r io.Reader) ([]byte, error) {
	length, err := c.readUvarint(r)
	if err != nil {
		return nil, err
	}
	buf := make([]byte, length)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func (c *SpillCodec) readInt8Slice(r io.Reader) ([]int8, error) {
	data, err := c.readBytes(r)
	if err != nil {
		return nil, err
	}
	out := make([]int8, len(data))
	for i, b := range data {
		out[i] = int8(b)
	}
	return out, nil
}

func (c *SpillCodec) readInt16Slice(r io.Reader) ([]int16, error) {
	length, err := c.readUvarint(r)
	if err != nil {
		return nil, err
	}
	out := make([]int16, length)
	for i := 0; i < int(length); i++ {
		if _, err := io.ReadFull(r, c.scalarBuf[:2]); err != nil {
			return nil, err
		}
		out[i] = int16(binary.LittleEndian.Uint16(c.scalarBuf[:2]))
	}
	return out, nil
}

func (c *SpillCodec) readInt32Slice(r io.Reader) ([]int32, error) {
	length, err := c.readUvarint(r)
	if err != nil {
		return nil, err
	}
	out := make([]int32, length)
	for i := 0; i < int(length); i++ {
		if _, err := io.ReadFull(r, c.scalarBuf[:4]); err != nil {
			return nil, err
		}
		out[i] = int32(binary.LittleEndian.Uint32(c.scalarBuf[:4]))
	}
	return out, nil
}

func (c *SpillCodec) readInt64Slice(r io.Reader) ([]int64, error) {
	length, err := c.readUvarint(r)
	if err != nil {
		return nil, err
	}
	out := make([]int64, length)
	for i := 0; i < int(length); i++ {
		if _, err := io.ReadFull(r, c.scalarBuf[:8]); err != nil {
			return nil, err
		}
		out[i] = int64(binary.LittleEndian.Uint64(c.scalarBuf[:8]))
	}
	return out, nil
}

func (c *SpillCodec) readFixedInt(r io.Reader, width int) (uint64, error) {
	if _, err := io.ReadFull(r, c.scalarBuf[:width]); err != nil {
		return 0, err
	}
	switch width {
	case 2:
		return uint64(binary.LittleEndian.Uint16(c.scalarBuf[:2])), nil
	case 4:
		return uint64(binary.LittleEndian.Uint32(c.scalarBuf[:4])), nil
	case 8:
		return binary.LittleEndian.Uint64(c.scalarBuf[:8]), nil
	default:
		return 0, fmt.Errorf("invalid width %d", width)
	}
}

func (c *SpillCodec) readFloat32(r io.Reader) (float32, error) {
	if _, err := io.ReadFull(r, c.scalarBuf[:4]); err != nil {
		return 0, err
	}
	return mathFloat32fromBits(binary.LittleEndian.Uint32(c.scalarBuf[:4])), nil
}

func (c *SpillCodec) readFloat64(r io.Reader) (float64, error) {
	if _, err := io.ReadFull(r, c.scalarBuf[:8]); err != nil {
		return 0, err
	}
	return mathFloat64fromBits(binary.LittleEndian.Uint64(c.scalarBuf[:8])), nil
}

func (c *SpillCodec) readFloat32Slice(r io.Reader) ([]float32, error) {
	length, err := c.readUvarint(r)
	if err != nil {
		return nil, err
	}
	out := make([]float32, length)
	for i := 0; i < int(length); i++ {
		if _, err := io.ReadFull(r, c.scalarBuf[:4]); err != nil {
			return nil, err
		}
		out[i] = mathFloat32fromBits(binary.LittleEndian.Uint32(c.scalarBuf[:4]))
	}
	return out, nil
}

func (c *SpillCodec) readFloat64Slice(r io.Reader) ([]float64, error) {
	length, err := c.readUvarint(r)
	if err != nil {
		return nil, err
	}
	out := make([]float64, length)
	for i := 0; i < int(length); i++ {
		if _, err := io.ReadFull(r, c.scalarBuf[:8]); err != nil {
			return nil, err
		}
		out[i] = mathFloat64fromBits(binary.LittleEndian.Uint64(c.scalarBuf[:8]))
	}
	return out, nil
}

func (c *SpillCodec) readString(r io.Reader) (string, error) {
	length, err := c.readUvarint(r)
	if err != nil {
		return "", err
	}

	// Get pooled buffer, grow if needed.
	bufPtr := stringBufPool.Get().(*[]byte)
	buf := *bufPtr
	if cap(buf) < int(length) {
		buf = make([]byte, length)
	} else {
		buf = buf[:length]
	}

	if _, err := io.ReadFull(r, buf); err != nil {
		*bufPtr = buf
		stringBufPool.Put(bufPtr)
		return "", err
	}

	// Intern the string to deduplicate repeated values (metric names, label keys, etc.)
	s := unique.Make(string(buf)).Value()

	*bufPtr = buf
	stringBufPool.Put(bufPtr)
	return s, nil
}

func (c *SpillCodec) readStringSlice(r io.Reader) ([]string, error) {
	length, err := c.readUvarint(r)
	if err != nil {
		return nil, err
	}
	out := make([]string, length)
	for i := 0; i < int(length); i++ {
		s, err := c.readString(r)
		if err != nil {
			return nil, err
		}
		out[i] = s
	}
	return out, nil
}

func (c *SpillCodec) readBoolSlice(r io.Reader) ([]bool, error) {
	length, err := c.readUvarint(r)
	if err != nil {
		return nil, err
	}
	out := make([]bool, length)
	for i := 0; i < int(length); i++ {
		if _, err := io.ReadFull(r, c.scalarBuf[:1]); err != nil {
			return nil, err
		}
		out[i] = c.scalarBuf[0] == 1
	}
	return out, nil
}

func (c *SpillCodec) readUvarint(r io.Reader) (uint64, error) {
	c.br.r = r
	return binary.ReadUvarint(&c.br)
}

// byteReader wraps an io.Reader to satisfy io.ByteReader without allocations.
type byteReader struct {
	r   io.Reader
	buf [1]byte
}

func (br *byteReader) ReadByte() (byte, error) {
	if _, err := io.ReadFull(br.r, br.buf[:]); err != nil {
		return 0, err
	}
	return br.buf[0], nil
}

func boolToByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}

// We intentionally duplicate a few math helpers to avoid pulling in the math
// package for trivial wrappers and to keep allocations at bay.
func mathFloat32bits(f float32) uint32     { return *(*uint32)(unsafe.Pointer(&f)) }
func mathFloat64bits(f float64) uint64     { return *(*uint64)(unsafe.Pointer(&f)) }
func mathFloat32fromBits(b uint32) float32 { return *(*float32)(unsafe.Pointer(&b)) }
func mathFloat64fromBits(b uint64) float64 { return *(*float64)(unsafe.Pointer(&b)) }

// unsafeSliceToBytes reinterprets an []int8 slice as []byte without allocation.
// It is safe because int8 and byte share layout. This keeps encoding allocation free.
func unsafeSliceToBytes(in []int8) []byte {
	if len(in) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(unsafe.SliceData(in))), len(in))
}
