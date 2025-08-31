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

// Package cbor provides reusable CBOR encoding/decoding functionality optimized for
// Row data (map[string]any) with proper type preservation and conversion.
//
// CBOR Type Behavior:
//   - All integers (int32, uint32, int) convert to int64
//   - float32 converts to float64
//   - []T slices convert to []any, except pure []float64 which is restored
//   - Maps decode directly as map[string]any (configured via DefaultMapType)
//   - uint64 values > MaxInt64 cause decode errors and should be avoided
//   - string, bool, []byte, nil are preserved exactly
//   - Invalid UTF-8 strings are allowed and decoded as-is (UTF8DecodeInvalid)
package cbor

import (
	"fmt"
	"io"
	"reflect"

	"github.com/fxamacker/cbor/v2"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// Config holds CBOR encoder and decoder configurations optimized for Row data.
type Config struct {
	encMode cbor.EncMode
	decMode cbor.DecMode
}

// NewConfig creates a new CBOR configuration optimized for Row data processing.
func NewConfig() (*Config, error) {
	// Configure CBOR encoding to preserve types
	encMode, err := cbor.EncOptions{
		Sort:          cbor.SortNone,          // Don't sort map keys - preserve order
		ShortestFloat: cbor.ShortestFloatNone, // Don't convert float types
		BigIntConvert: cbor.BigIntConvertNone, // Don't convert large integers
		Time:          cbor.TimeUnixMicro,     // Encode times as Unix timestamps
		TimeTag:       cbor.EncTagNone,        // Don't add CBOR time tags
	}.EncMode()
	if err != nil {
		return nil, fmt.Errorf("failed to create CBOR encoder: %w", err)
	}

	// Configure CBOR decoding to preserve types
	decMode, err := cbor.DecOptions{
		BigIntDec:      cbor.BigIntDecodeValue,           // Preserve large integers
		IntDec:         cbor.IntDecConvertSigned,         // Convert all integers to int64 (signed)
		DefaultMapType: reflect.TypeOf(map[string]any{}), // Decode maps as map[string]any instead of map[interface{}]interface{}
		UTF8:           cbor.UTF8DecodeInvalid,           // Allow decoding CBOR Text containing invalid UTF-8 strings
	}.DecMode()
	if err != nil {
		return nil, fmt.Errorf("failed to create CBOR decoder: %w", err)
	}

	return &Config{
		encMode: encMode,
		decMode: decMode,
	}, nil
}

// NewEncoder creates a new CBOR encoder using the optimized configuration.
func (c *Config) NewEncoder(w io.Writer) *cbor.Encoder {
	return c.encMode.NewEncoder(w)
}

// NewDecoder creates a new CBOR decoder using the optimized configuration.
func (c *Config) NewDecoder(r io.Reader) *cbor.Decoder {
	return c.decMode.NewDecoder(r)
}

// Encode encodes a Row to CBOR bytes.
func (c *Config) Encode(row map[string]any) ([]byte, error) {
	return c.encMode.Marshal(row)
}

// Decode decodes CBOR bytes to a Row with type conversion applied.
func (c *Config) Decode(data []byte) (map[string]any, error) {
	var raw map[string]any
	if err := c.decMode.Unmarshal(data, &raw); err != nil {
		return nil, err
	}

	// Apply type conversion to fix CBOR limitations
	converted := make(map[string]any)
	for k, v := range raw {
		converted[k] = convertCBORTypes(v)
	}

	return converted, nil
}

// EncodeRow encodes a Row (map[wkk.RowKey]any) to CBOR bytes.
// Converts RowKeys to strings for CBOR compatibility while preserving all data.
func (c *Config) EncodeRow(row pipeline.Row) ([]byte, error) {
	stringMap := pipeline.ToStringMap(row)
	return c.Encode(stringMap)
}

// DecodeRow decodes CBOR bytes to a Row (map[wkk.RowKey]any) with type conversion applied.
// Converts string keys back to RowKeys after CBOR decoding.
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

// UnmarshalFirst decodes the first CBOR data item from bytes and returns remaining bytes.
// This avoids decoder state retention issues that can occur with streaming decoders.
func (c *Config) UnmarshalFirst(data []byte, v any) ([]byte, error) {
	return c.decMode.UnmarshalFirst(data, v)
}

// convertCBORTypes converts CBOR-decoded values back to expected types.
// CBOR naturally converts: float32->float64, any-int->int64, []T->[]any
// With DefaultMapType set, we no longer get map[interface{}]interface{}.
func convertCBORTypes(value any) any {
	switch v := value.(type) {
	case []any:
		if len(v) == 0 {
			// Return empty []any to preserve the original slice type
			return []any{}
		}

		// Check if all elements are float64 (common case for metrics)
		allFloat64 := true
		for _, elem := range v {
			if _, ok := elem.(float64); !ok {
				allFloat64 = false
				break
			}
		}

		if allFloat64 {
			// Convert to []float64 for efficiency in metrics processing
			result := make([]float64, len(v))
			for i, elem := range v {
				result[i] = elem.(float64)
			}
			return result
		}

		// For mixed types, preserve as []any but convert elements recursively
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = convertCBORTypes(elem)
		}
		return result

	case map[string]any:
		// Convert nested maps recursively (DefaultMapType ensures we get map[string]any)
		result := make(map[string]any)
		for k, v := range v {
			result[k] = convertCBORTypes(v)
		}
		return result

	default:
		return v // Return unchanged - accept CBOR's natural conversions
	}
}
