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

import (
	"context"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// Row represents a single row of data as a map of column names to values.
type Row = pipeline.Row

// Reader is the core interface for reading rows from any file format using pipeline semantics.
// This eliminates memory ownership issues by establishing clear ownership: batches are owned
// by the reader and must not be retained beyond the next Next() call.
type Reader interface {
	// Next returns the next batch of rows, or io.EOF when exhausted.
	// The returned batch is owned by the reader and must not be retained
	// beyond the next Next() call. Use pipeline.CopyBatch() if you need to retain.
	// The context can be used for cancellation, deadlines, and updating metrics.
	Next(ctx context.Context) (*Batch, error)

	// Close releases any resources held by the reader.
	Close() error

	// TotalRowsReturned returns the total number of rows that have been successfully
	// returned via Next() calls from this reader so far.
	TotalRowsReturned() int64
}

// RowTranslator transforms rows from one format to another.
type RowTranslator interface {
	// TranslateRow transforms a row in-place by modifying the provided row pointer.
	TranslateRow(row *Row) error
}

// OTELMetricsProvider provides access to the underlying OpenTelemetry metrics structure.
// This is used when the original OTEL structure is needed for processing (e.g., exemplars).
type OTELMetricsProvider interface {
	// GetOTELMetrics returns the underlying parsed OTEL metrics structure.
	// This allows access to exemplars and other metadata not available in the row format.
	GetOTELMetrics() (*pmetric.Metrics, error)
}

// OTELLogsProvider provides access to the underlying OpenTelemetry logs structure.
// This is used when the original OTEL structure is needed for processing (e.g., exemplars).
type OTELLogsProvider interface {
	// GetOTELLogs returns the underlying parsed OTEL logs structure.
	// This allows access to the original log body and metadata not available in the row format.
	GetOTELLogs() (*plog.Logs, error)
}

// Batch represents a collection of rows with clear ownership semantics.
// The batch is owned by the reader that returns it.
type Batch = pipeline.Batch

// extractTimestamp extracts a timestamp from a row, handling various numeric types.
func extractTimestamp(row Row, fieldName string) int64 {
	// Convert string field name to RowKey for lookup
	rowKey := wkk.NewRowKey(fieldName)
	val, exists := row[rowKey]
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

// TimeOrderedSortKeyProvider creates sort keys based on a single timestamp field
type TimeOrderedSortKeyProvider struct {
	fieldName string
}

// NewTimeOrderedSortKeyProvider creates a provider that sorts by a timestamp field
func NewTimeOrderedSortKeyProvider(fieldName string) *TimeOrderedSortKeyProvider {
	return &TimeOrderedSortKeyProvider{fieldName: fieldName}
}

// TimeOrderedSortKey represents a sort key for timestamp-based ordering
type TimeOrderedSortKey struct {
	timestamp int64
	valid     bool
}

func (k *TimeOrderedSortKey) Compare(other SortKey) int {
	o, ok := other.(*TimeOrderedSortKey)
	if !ok {
		panic("TimeOrderedSortKey.Compare: other key is not TimeOrderedSortKey")
	}

	if !k.valid || !o.valid {
		if !k.valid && !o.valid {
			return 0
		}
		if !k.valid {
			return 1 // invalid keys go last
		}
		return -1
	}

	if k.timestamp < o.timestamp {
		return -1
	}
	if k.timestamp > o.timestamp {
		return 1
	}
	return 0
}

func (k *TimeOrderedSortKey) Release() {
	// No resources to release for simple int64 key
}

func (p *TimeOrderedSortKeyProvider) MakeKey(row Row) SortKey {
	timestamp := extractTimestamp(row, p.fieldName)
	return &TimeOrderedSortKey{
		timestamp: timestamp,
		valid:     timestamp != 0,
	}
}
