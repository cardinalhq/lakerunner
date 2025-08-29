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

// Package gob provides reusable gob encoding/decoding functionality optimized for
// Row data (map[string]any) with proper type preservation.
//
// Gob is Go's native binary encoding format with excellent memory characteristics
// and automatic type registration for complex Go types.
package gob

import (
	"bytes"
	"encoding/gob"
	"io"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func init() {
	// Register the concrete types we expect in record values.
	gob.Register(map[string]any{})
	gob.Register([]any{})
	gob.Register([]float64{})
	gob.Register([]string{})
	gob.Register([]int64{})
	gob.Register([]int{})
	gob.Register([]uint64{})
	gob.Register([]map[string]any{})
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(string(""))
	gob.Register(bool(false))
	gob.Register(uint64(0))
	gob.Register(int32(0))
	gob.Register(float32(0))
}

// Config holds gob encoder and decoder configurations optimized for Row data.
type Config struct{}

// NewConfig creates a new gob configuration optimized for Row data processing.
func NewConfig() (*Config, error) {
	return &Config{}, nil
}

// NewEncoder creates a new gob encoder.
func (c *Config) NewEncoder(w io.Writer) *gob.Encoder {
	return gob.NewEncoder(w)
}

// NewDecoder creates a new gob decoder.
func (c *Config) NewDecoder(r io.Reader) *gob.Decoder {
	return gob.NewDecoder(r)
}

// Encode encodes a Row to gob bytes.
func (c *Config) Encode(row map[string]any) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(row); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Decode decodes gob bytes to a Row.
func (c *Config) Decode(data []byte) (map[string]any, error) {
	var row map[string]any
	buf := bytes.NewReader(data)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&row); err != nil {
		return nil, err
	}
	return row, nil
}

// EncodeRow encodes a Row (map[wkk.RowKey]any) to gob bytes.
// Converts RowKeys to strings for gob compatibility while preserving all data.
func (c *Config) EncodeRow(row pipeline.Row) ([]byte, error) {
	stringMap := pipeline.ToStringMap(row)
	return c.Encode(stringMap)
}

// DecodeRow decodes gob bytes to a Row (map[wkk.RowKey]any).
// Converts string keys back to RowKeys after gob decoding.
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
