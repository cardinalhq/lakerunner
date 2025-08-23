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
	"encoding/gob"
	"fmt"
	"io"
	"os"
)

func init() {
	// Register types for gob encoding/decoding
	gob.Register(map[string]any{})
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(string(""))
	gob.Register(bool(false))
	gob.Register([]byte{})
	gob.Register([]int64{})
	gob.Register([]string{})
}

// GobSpiller implements the Spiller interface using Go's gob encoding.
// This isolates all GOB handling in a single file for easy replacement.
type GobSpiller struct {
	chunkCounter int
}

// NewGobSpiller creates a new GOB-based spiller.
func NewGobSpiller() *GobSpiller {
	return &GobSpiller{
		chunkCounter: 0,
	}
}

// WriteSpillFile writes sorted rows to a temporary file using gob encoding.
func (s *GobSpiller) WriteSpillFile(tmpDir string, rows []map[string]any, keyFunc func(map[string]any) any) (*SpillFile, error) {
	// Create temporary file
	file, err := os.CreateTemp(tmpDir, fmt.Sprintf("spill-%d-*.gob", s.chunkCounter))
	if err != nil {
		return nil, fmt.Errorf("create spill file: %w", err)
	}
	defer file.Close()

	s.chunkCounter++

	// Encode all rows to the file
	encoder := gob.NewEncoder(file)
	for _, row := range rows {
		if err := encoder.Encode(row); err != nil {
			os.Remove(file.Name())
			return nil, fmt.Errorf("encode row to spill file: %w", err)
		}
	}

	return &SpillFile{
		Path:     file.Name(),
		RowCount: int64(len(rows)),
		Metadata: nil, // No extra metadata needed for GOB files
	}, nil
}

// OpenSpillFile opens a GOB spill file for reading.
func (s *GobSpiller) OpenSpillFile(spillFile *SpillFile, keyFunc func(map[string]any) any) (SpillReader, error) {
	file, err := os.Open(spillFile.Path)
	if err != nil {
		return nil, fmt.Errorf("open spill file %s: %w", spillFile.Path, err)
	}

	return &gobSpillReader{
		file:    file,
		decoder: gob.NewDecoder(file),
		keyFunc: keyFunc,
	}, nil
}

// CleanupSpillFile removes the GOB spill file from disk.
func (s *GobSpiller) CleanupSpillFile(spillFile *SpillFile) error {
	return os.Remove(spillFile.Path)
}

// gobSpillReader implements SpillReader for GOB-encoded files.
type gobSpillReader struct {
	file    *os.File
	decoder *gob.Decoder
	keyFunc func(map[string]any) any
	closed  bool
}

// Next reads the next row from the GOB spill file.
func (r *gobSpillReader) Next() (map[string]any, error) {
	if r.closed {
		return nil, io.EOF
	}

	var row map[string]any
	if err := r.decoder.Decode(&row); err != nil {
		if err == io.EOF {
			r.closed = true
		}
		return nil, err
	}

	return row, nil
}

// Close closes the GOB spill reader and cleans up resources.
func (r *gobSpillReader) Close() error {
	r.closed = true
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
