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

// gob_codec.go provides GOB encoding/decoding for Row data.
package rowcodec

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// GOBCodec holds the GOB encoder/decoder.
type GOBCodec struct{}

// NewGOBCodec creates a new GOB codec.
func NewGOBCodec() (*GOBCodec, error) {
	return &GOBCodec{}, nil
}

// Encode encodes a map[string]any to GOB bytes.
func (c *GOBCodec) Encode(row map[string]any) ([]byte, error) {
	// Promote int32 to int64 and float32 to float64 for consistency
	promoted := make(map[string]any, len(row))
	for k, v := range row {
		switch val := v.(type) {
		case int32:
			promoted[k] = int64(val)
		case float32:
			promoted[k] = float64(val)
		default:
			promoted[k] = v
		}
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(promoted); err != nil {
		return nil, fmt.Errorf("encode GOB: %w", err)
	}
	return buf.Bytes(), nil
}

// Decode decodes GOB bytes into the supplied map[string]any.
// The supplied map is cleared before decoding.
func (c *GOBCodec) Decode(data []byte, into map[string]any) error {
	// Clear the supplied map
	for k := range into {
		delete(into, k)
	}

	buf := bytes.NewReader(data)
	decoder := gob.NewDecoder(buf)

	if err := decoder.Decode(&into); err != nil {
		return fmt.Errorf("decode GOB: %w", err)
	}
	return nil
}

// EncodeRow encodes a Row (map[wkk.RowKey]any) to GOB bytes.
func (c *GOBCodec) EncodeRow(row pipeline.Row) ([]byte, error) {
	// Convert Row to map[string]any for GOB encoding with type promotion
	stringMap := make(map[string]any, len(row))
	for k, v := range row {
		// Promote int32 to int64 and float32 to float64
		switch val := v.(type) {
		case int32:
			stringMap[string(k.Value())] = int64(val)
		case float32:
			stringMap[string(k.Value())] = float64(val)
		default:
			stringMap[string(k.Value())] = v
		}
	}

	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(stringMap); err != nil {
		return nil, fmt.Errorf("encode GOB row: %w", err)
	}
	return buf.Bytes(), nil
}

// DecodeRow decodes GOB bytes into the supplied Row (map[wkk.RowKey]any).
// The supplied Row is cleared before decoding.
func (c *GOBCodec) DecodeRow(data []byte, into pipeline.Row) error {
	// Clear the supplied Row
	for k := range into {
		delete(into, k)
	}

	buf := bytes.NewReader(data)
	decoder := gob.NewDecoder(buf)

	var stringMap map[string]any
	if err := decoder.Decode(&stringMap); err != nil {
		return fmt.Errorf("decode GOB row: %w", err)
	}

	// Convert map[string]any to Row
	for k, v := range stringMap {
		into[wkk.NewRowKey(k)] = v
	}
	return nil
}

// NewEncoder creates a new GOB encoder.
func (c *GOBCodec) NewEncoder(w io.Writer) Encoder {
	return &GOBEncoder{
		encoder: gob.NewEncoder(w),
	}
}

// NewDecoder creates a new GOB decoder.
func (c *GOBCodec) NewDecoder(r io.Reader) Decoder {
	return &GOBDecoder{
		decoder: gob.NewDecoder(r),
	}
}

// GOBEncoder writes GOB-encoded map data to an io.Writer.
type GOBEncoder struct {
	encoder *gob.Encoder
}

// Encode writes a map[string]any in GOB format.
func (e *GOBEncoder) Encode(row map[string]any) error {
	// Promote int32 to int64 and float32 to float64 for consistency
	promoted := make(map[string]any, len(row))
	for k, v := range row {
		switch val := v.(type) {
		case int32:
			promoted[k] = int64(val)
		case float32:
			promoted[k] = float64(val)
		default:
			promoted[k] = v
		}
	}
	return e.encoder.Encode(promoted)
}

// GOBDecoder reads GOB-encoded map data from an io.Reader.
type GOBDecoder struct {
	decoder *gob.Decoder
}

// Decode reads into the supplied map[string]any from GOB format.
// The supplied map is cleared before decoding.
// Returns io.EOF when no more data is available.
func (d *GOBDecoder) Decode(into map[string]any) error {
	// Clear the supplied map
	for k := range into {
		delete(into, k)
	}
	if err := d.decoder.Decode(&into); err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("decode GOB: %w", err)
	}
	return nil
}
