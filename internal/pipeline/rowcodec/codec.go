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

// Package rowcodec provides encoding/decoding interfaces for Row data
// (map[string]any) with multiple implementation options.
package rowcodec

import (
	"fmt"
	"io"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
)

// Codec defines the interface for encoding/decoding row data.
type Codec interface {
	// Encode encodes a map[string]any to bytes.
	Encode(row map[string]any) ([]byte, error)

	// Decode decodes bytes into the supplied map[string]any.
	// The supplied map is cleared before decoding.
	Decode(data []byte, into map[string]any) error

	// EncodeRow encodes a Row (map[wkk.RowKey]any) to bytes.
	EncodeRow(row pipeline.Row) ([]byte, error)

	// DecodeRow decodes bytes into the supplied Row (map[wkk.RowKey]any).
	// The supplied Row is cleared before decoding.
	DecodeRow(data []byte, into pipeline.Row) error

	// NewEncoder creates a new encoder that writes to the given writer.
	NewEncoder(w io.Writer) Encoder

	// NewDecoder creates a new decoder that reads from the given reader.
	NewDecoder(r io.Reader) Decoder
}

// Encoder defines the interface for streaming encoding.
type Encoder interface {
	// Encode writes a map[string]any in the codec's format.
	Encode(row map[string]any) error
}

// Decoder defines the interface for streaming decoding.
type Decoder interface {
	// Decode reads into the supplied map[string]any from the codec's format.
	// The supplied map is cleared before decoding.
	// Returns io.EOF when no more data is available.
	Decode(into map[string]any) error
}

// Type represents the available codec types.
type Type string

const (
	// default type
	TypeDefault Type = ""
	// TypeCBOR is the CBOR codec.
	TypeCBOR Type = "cbor"
)

// NewCBOR creates a new CBOR codec.
func NewCBOR() (Codec, error) {
	return NewCBORCodec()
}

// New creates a new codec of the specified type.
// TypeDefault uses CBOR for better performance (smaller files, less memory).
func New(t Type) (Codec, error) {
	switch t {
	case TypeCBOR, TypeDefault:
		return NewCBOR()
	default:
		return nil, fmt.Errorf("unknown codec type: %s", t)
	}
}
