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

// cbor_codec.go provides CBOR encoding/decoding for Row data.
package rowcodec

import (
	"fmt"
	"io"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/fxamacker/cbor/v2"
)

// CBORCodec holds the CBOR encoder/decoder configuration.
type CBORCodec struct {
	em cbor.EncMode
	dm cbor.DecMode
}

// NewCBORCodec creates a new CBOR codec.
func NewCBORCodec() (*CBORCodec, error) {
	// Configure CBOR encoding options for best performance and compatibility
	em, err := cbor.CoreDetEncOptions().EncMode()
	if err != nil {
		return nil, fmt.Errorf("create CBOR encoder: %w", err)
	}

	dm, err := cbor.DecOptions{
		DupMapKey:       cbor.DupMapKeyEnforcedAPF,      // Enforce unique keys
		IndefLength:     cbor.IndefLengthAllowed,        // Allow indefinite length
		MaxNestedLevels: 20,                             // Reasonable nesting limit
		IntDec:          cbor.IntDecConvertSignedOrFail, // Always decode to int64 for consistency
	}.DecMode()
	if err != nil {
		return nil, fmt.Errorf("create CBOR decoder: %w", err)
	}

	return &CBORCodec{
		em: em,
		dm: dm,
	}, nil
}

// Encode encodes a map[string]any to CBOR bytes.
func (c *CBORCodec) Encode(row map[string]any) ([]byte, error) {
	return c.em.Marshal(row)
}

// Decode decodes CBOR bytes into the supplied map[string]any.
// The supplied map is cleared before decoding.
func (c *CBORCodec) Decode(data []byte, into map[string]any) error {
	// Clear the supplied map
	for k := range into {
		delete(into, k)
	}
	if err := c.dm.Unmarshal(data, &into); err != nil {
		return fmt.Errorf("decode CBOR: %w", err)
	}
	return nil
}

// EncodeRow encodes a Row (map[wkk.RowKey]any) to CBOR bytes.
func (c *CBORCodec) EncodeRow(row pipeline.Row) ([]byte, error) {
	// Convert Row to map[string]any for CBOR encoding
	stringMap := make(map[string]any, len(row))
	for k, v := range row {
		stringMap[string(k.Value())] = v
	}
	return c.em.Marshal(stringMap)
}

// DecodeRow decodes CBOR bytes into the supplied Row (map[wkk.RowKey]any).
// The supplied Row is cleared before decoding.
func (c *CBORCodec) DecodeRow(data []byte, into pipeline.Row) error {
	// Clear the supplied Row
	for k := range into {
		delete(into, k)
	}

	var stringMap map[string]any
	if err := c.dm.Unmarshal(data, &stringMap); err != nil {
		return fmt.Errorf("decode CBOR row: %w", err)
	}

	// Convert map[string]any to Row
	for k, v := range stringMap {
		into[wkk.NewRowKey(k)] = v
	}
	return nil
}

// NewEncoder creates a new CBOR encoder.
func (c *CBORCodec) NewEncoder(w io.Writer) Encoder {
	return &CBOREncoder{
		encoder: c.em.NewEncoder(w),
	}
}

// NewDecoder creates a new CBOR decoder.
func (c *CBORCodec) NewDecoder(r io.Reader) Decoder {
	return &CBORDecoder{
		decoder: c.dm.NewDecoder(r),
	}
}

// CBOREncoder writes CBOR-encoded map data to an io.Writer.
type CBOREncoder struct {
	encoder *cbor.Encoder
}

// Encode writes a map[string]any in CBOR format.
func (e *CBOREncoder) Encode(row map[string]any) error {
	return e.encoder.Encode(row)
}

// CBORDecoder reads CBOR-encoded map data from an io.Reader.
type CBORDecoder struct {
	decoder *cbor.Decoder
}

// Decode reads into the supplied map[string]any from CBOR format.
// The supplied map is cleared before decoding.
// Returns io.EOF when no more data is available.
func (d *CBORDecoder) Decode(into map[string]any) error {
	// Clear the supplied map
	for k := range into {
		delete(into, k)
	}
	if err := d.decoder.Decode(&into); err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("decode CBOR: %w", err)
	}
	return nil
}
