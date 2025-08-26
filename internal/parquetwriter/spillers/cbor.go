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

package spillers

import (
	"fmt"
	"os"
	"slices"

	cbor2 "github.com/fxamacker/cbor/v2"

	"github.com/cardinalhq/lakerunner/internal/cbor"
)

// CborSpiller implements the Spiller interface using CBOR encoding.
// CBOR provides better type preservation and performance compared to GOB
// for map[string]any data structures.
type CborSpiller struct {
	config *cbor.Config
}

// NewCborSpiller creates a new CBOR-based spiller.
func NewCborSpiller() (*CborSpiller, error) {
	config, err := cbor.NewConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create CBOR config: %w", err)
	}

	return &CborSpiller{
		config: config,
	}, nil
}

// WriteSpillFile writes a sorted slice of rows to a temporary CBOR file.
func (s *CborSpiller) WriteSpillFile(tmpDir string, rows []map[string]any, keyFunc func(map[string]any) any) (*SpillFile, error) {
	if len(rows) == 0 {
		return &SpillFile{RowCount: 0}, nil
	}

	// Sort rows by key function
	slices.SortFunc(rows, func(a, b map[string]any) int {
		keyA := keyFunc(a)
		keyB := keyFunc(b)

		// Handle comparison of different types - convert to string for comparison
		strA := fmt.Sprintf("%v", keyA)
		strB := fmt.Sprintf("%v", keyB)

		if strA < strB {
			return -1
		}
		if strA > strB {
			return 1
		}
		return 0
	})

	// Create temporary file
	tempFile, err := os.CreateTemp(tmpDir, "spill-*.cbor")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp file: %w", err)
	}
	defer tempFile.Close()

	// Write rows using CBOR encoder
	encoder := s.config.NewEncoder(tempFile)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			os.Remove(tempFile.Name())
			return nil, fmt.Errorf("failed to encode row: %w", err)
		}
	}

	return &SpillFile{
		Path:     tempFile.Name(),
		RowCount: int64(len(rows)),
		Metadata: nil, // No additional metadata needed
	}, nil
}

// OpenSpillFile opens a CBOR spill file for reading.
func (s *CborSpiller) OpenSpillFile(spillFile *SpillFile, keyFunc func(map[string]any) any) (SpillReader, error) {
	file, err := os.Open(spillFile.Path)
	if err != nil {
		return nil, fmt.Errorf("failed to open spill file %s: %w", spillFile.Path, err)
	}

	return &cborSpillReader{
		file:    file,
		decoder: s.config.NewDecoder(file),
		config:  s.config,
	}, nil
}

// CleanupSpillFile removes the CBOR spill file from disk.
func (s *CborSpiller) CleanupSpillFile(spillFile *SpillFile) error {
	if spillFile.Path == "" {
		return nil // No file to clean up
	}

	if err := os.Remove(spillFile.Path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove spill file %s: %w", spillFile.Path, err)
	}
	return nil
}

// cborSpillReader implements SpillReader for CBOR files.
type cborSpillReader struct {
	file    *os.File
	decoder *cbor2.Decoder
	config  *cbor.Config
}

// Next reads the next row from the CBOR spill file.
func (r *cborSpillReader) Next() (map[string]any, error) {
	var raw map[string]any
	if err := r.decoder.Decode(&raw); err != nil {
		return nil, err // io.EOF will be returned naturally at end of file
	}

	// Apply CBOR type conversion
	converted := make(map[string]any)
	for k, v := range raw {
		converted[k] = convertCBORValue(v)
	}

	return converted, nil
}

// Close closes the CBOR spill reader.
func (r *cborSpillReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// convertCBORValue applies the same type conversion logic as the cbor package.
// This is duplicated here to avoid circular imports, but could be refactored
// if the cbor package exports this function.
func convertCBORValue(value any) any {
	switch v := value.(type) {
	case []any:
		if len(v) == 0 {
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
			// Convert to []float64 for efficiency
			result := make([]float64, len(v))
			for i, elem := range v {
				result[i] = elem.(float64)
			}
			return result
		}

		// For mixed types, preserve as []any but convert elements recursively
		result := make([]any, len(v))
		for i, elem := range v {
			result[i] = convertCBORValue(elem)
		}
		return result

	case map[string]any:
		// Convert nested maps recursively (DefaultMapType ensures we get map[string]any)
		result := make(map[string]any)
		for k, v := range v {
			result[k] = convertCBORValue(v)
		}
		return result

	default:
		return v // Return unchanged - accept CBOR's natural conversions
	}
}
