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

// Package filereader provides a generic interface for reading rows from various file formats.
// Callers construct readers directly and compose them as needed for their specific use cases.
package filereader

// Row represents a single row of data as a map of column names to values.
type Row map[string]any

// Reader is the core interface for reading rows from any file format.
type Reader interface {
	// GetRow returns the next row of data.
	// Returns io.EOF when there are no more rows.
	// Returns error for any read failures.
	GetRow() (row Row, err error)

	// Close releases any resources held by the reader.
	Close() error
}

// RowTranslator transforms rows from one format to another.
type RowTranslator interface {
	TranslateRow(in Row) (Row, error)
}

// SelectFunc is a function that selects which row to return next from a set of candidate rows.
// It receives a slice of rows (one from each active reader) and returns the index of the
// row that should be returned next. This enables custom sorting logic for ordered reading.
type SelectFunc func(rows []Row) int

// TimeOrderedSelector returns a SelectFunc that selects rows based on ascending timestamp order.
// It expects rows to have a timestamp field with the given fieldName.
func TimeOrderedSelector(fieldName string) SelectFunc {
	return func(rows []Row) int {
		if len(rows) == 0 {
			return -1
		}

		minIdx := 0
		minTs := extractTimestamp(rows[0], fieldName)

		for i := 1; i < len(rows); i++ {
			ts := extractTimestamp(rows[i], fieldName)
			if ts < minTs {
				minIdx = i
				minTs = ts
			}
		}

		return minIdx
	}
}

// extractTimestamp extracts a timestamp from a row, handling various numeric types.
func extractTimestamp(row Row, fieldName string) int64 {
	val, exists := row[fieldName]
	if !exists {
		return 0
	}

	switch v := val.(type) {
	case int64:
		return v
	case int32:
		return int64(v)
	case int:
		return int64(v)
	case float64:
		return int64(v)
	case float32:
		return int64(v)
	default:
		return 0
	}
}
