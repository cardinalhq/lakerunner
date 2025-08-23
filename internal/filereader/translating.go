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

// GetRow returns the next translated row from the underlying reader.
func (tr *TranslatingReader) GetRow() (Row, error) {
	if tr.closed {
		return nil, errors.New("reader is closed")
	}

	// Get raw row from underlying reader
	row, err := tr.reader.GetRow()
	if err != nil {
		return nil, err // Pass through EOF and other errors
	}

	// Apply translation
	translatedRow, err := tr.translator.TranslateRow(row)
	if err != nil {
		return nil, fmt.Errorf("translation failed: %w", err)
	}

	return translatedRow, nil
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
