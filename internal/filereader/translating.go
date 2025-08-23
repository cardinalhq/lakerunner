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

package filereader

import (
	"errors"
	"fmt"
)

// TranslatingReader wraps another Reader and applies row transformations.
// This enables composition where any Reader can be enhanced with signal-specific
// translation logic without coupling file parsing to data transformation.
type TranslatingReader struct {
	reader     Reader
	translator RowTranslator
	closed     bool
}

// NewTranslatingReader creates a new TranslatingReader that applies the given
// translator to each row returned by the underlying reader.
//
// The TranslatingReader takes ownership of the underlying reader and will
// close it when Close() is called.
func NewTranslatingReader(reader Reader, translator RowTranslator) (*TranslatingReader, error) {
	if reader == nil {
		return nil, errors.New("reader cannot be nil")
	}
	if translator == nil {
		return nil, errors.New("translator cannot be nil")
	}

	return &TranslatingReader{
		reader:     reader,
		translator: translator,
	}, nil
}

// Read populates the provided slice with translated rows from the underlying reader.
func (tr *TranslatingReader) Read(rows []Row) (int, error) {
	if tr.closed {
		return 0, errors.New("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	// Get raw rows from underlying reader
	n, err := tr.reader.Read(rows)

	// Translate each row that was successfully read
	for i := 0; i < n; i++ {
		translatedRow, translateErr := tr.translator.TranslateRow(rows[i])
		if translateErr != nil {
			return i, fmt.Errorf("translation failed for row %d: %w", i, translateErr)
		}

		// Clear and replace the row with translated data
		resetRow(&rows[i])
		for k, v := range translatedRow {
			rows[i][k] = v
		}
	}

	return n, err // Pass through the original error (including EOF)
}

// Close closes the underlying reader and releases resources.
func (tr *TranslatingReader) Close() error {
	if tr.closed {
		return nil
	}
	tr.closed = true

	if tr.reader != nil {
		if err := tr.reader.Close(); err != nil {
			return fmt.Errorf("failed to close underlying reader: %w", err)
		}
		tr.reader = nil
	}
	tr.translator = nil

	return nil
}
