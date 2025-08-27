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
//
// ## Row Buffer Contract
//
// Implementations MUST follow these buffer reuse rules for memory efficiency:
//
// 1. **Input Buffer Ownership**: The caller owns the Row slice and its individual Row maps.
//    Readers MUST NOT store references to these Row maps beyond the Read() call.
//
// 2. **Row Map Reuse**: Callers may reuse Row maps across Read() calls for performance.
//    Readers MUST call resetRow() or clear each Row map before populating it to prevent
//    data leakage from previous reads.
//
// 3. **Population Contract**: Readers MUST only populate Row maps with new data after
//    ensuring they are clean. Use resetRow(&rows[i]) before populating rows[i].
//
// 4. **Error Handling**: On errors, readers SHOULD leave Row maps in a clean state
//    (empty or properly reset) to prevent partial data corruption.
//
// 5. **Concurrent Safety**: Individual Reader instances are NOT thread-safe unless
//    explicitly documented. Callers MUST NOT call Read() concurrently on the same
//    Reader instance.
//
// Example correct usage:
//   rows := make([]Row, 10)
//   n, err := reader.Read(rows)
//   // rows[0:n] now contain valid data, others are undefined
//
type Reader interface {
	// Read populates the provided slice with as many rows as possible.
	// Returns the number of rows read and any error (including io.EOF when exhausted).
	// Similar to io.Reader pattern: may return n > 0 and err != nil.
	//
	// IMPLEMENTATION REQUIREMENT: Must call resetRow() on each Row before populating
	// it to ensure proper buffer reuse and prevent data corruption.
	Read(rows []Row) (n int, err error)

	// Close releases any resources held by the reader.
	Close() error

	// TotalRowsReturned returns the total number of rows that have been successfully
	// returned via Read() calls from this reader so far. This count should only include
	// rows that were actually provided to the caller via Read().
	TotalRowsReturned() int64
}

// RowTranslator transforms rows from one format to another.
type RowTranslator interface {
	// TranslateRow transforms a row in-place by modifying the provided row pointer.
	// This eliminates confusing reference semantics and makes mutations explicit.
	TranslateRow(row *Row) error
}

// OTELMetricsProvider provides access to the underlying OpenTelemetry metrics structure.
// This is used when the original OTEL structure is needed for processing (e.g., exemplars).
type OTELMetricsProvider interface {
	// GetOTELMetrics returns the underlying parsed OTEL metrics structure.
	// This allows access to exemplars and other metadata not available in the row format.
	GetOTELMetrics() (any, error)
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

// resetRow initializes or clears a Row map for reuse to prevent data corruption.
//
// This function implements the core buffer reuse pattern for memory efficiency:
// - If the row is nil, it creates a new Row map
// - If not nil, it clears all existing data while preserving the underlying map allocation
//
// ## Usage Patterns:
//
// **Reader Implementations**: MUST call this before populating each row
//   resetRow(&rows[i])
//   rows[i]["field"] = value
//
// **Callers with Buffer Reuse**: Should call this when recycling row buffers
//   resetRow(&myRowBuffer)
//   reader.Read([]Row{myRowBuffer})
//
// **Error Recovery**: Call this to ensure clean state after errors
//   if err != nil {
//       resetRow(&pendingRow)  // Clean state for next attempt
//   }
//
// ## Memory Safety:
// This function prevents data leakage between row uses by ensuring each Row
// starts clean. It preserves the underlying map allocation for performance
// while removing all key-value pairs from previous uses.
//
func resetRow(row *Row) {
	if *row == nil {
		*row = make(Row)
	} else {
		// Clear existing row while preserving map allocation
		for k := range *row {
			delete(*row, k)
		}
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
