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
	"context"
	"fmt"
	"io"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// IngestProtoLogsReader reads rows from OpenTelemetry protobuf logs format.
//
// Implements OTELLogsProvider interface.
type IngestProtoLogsReader struct {
	closed    bool
	rowCount  int64
	batchSize int

	// Streaming iterator state for logs
	logs          *plog.Logs
	resourceIndex int
	scopeIndex    int
	logIndex      int
	orgId         string
}

var _ Reader = (*IngestProtoLogsReader)(nil)
var _ OTELLogsProvider = (*IngestProtoLogsReader)(nil)

// NewIngestProtoLogsReader creates a new IngestProtoLogsReader for the given io.Reader.
func NewIngestProtoLogsReader(reader io.Reader, opts ReaderOptions) (*IngestProtoLogsReader, error) {
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	protoReader := &IngestProtoLogsReader{
		batchSize: batchSize,
		orgId:     opts.OrgID,
	}

	logs, err := parseProtoToOtelLogs(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL logs: %w", err)
	}
	protoReader.logs = logs

	return protoReader, nil
}

// Next returns the next batch of rows from the OTEL logs.
func (r *IngestProtoLogsReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	batch := pipeline.GetBatch()

	for batch.Len() < r.batchSize {
		// Get a row from the pool to populate
		row := pipeline.GetPooledRow()
		err := r.getLogRow(ctx, row)
		if err != nil {
			// Return unused row to pool
			pipeline.ReturnPooledRow(row)
			if err == io.EOF {
				if batch.Len() == 0 {
					pipeline.ReturnBatch(batch)
					return nil, io.EOF
				}
				break
			}
			pipeline.ReturnBatch(batch)
			return nil, err
		}

		// Add the populated row to the batch (batch takes ownership)
		batch.AppendRow(row)

		rowsInCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoLogsReader"),
		))
	}

	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "IngestProtoLogsReader"),
		))
		return batch, nil
	}

	pipeline.ReturnBatch(batch)
	return nil, io.EOF
}

// getLogRow handles reading the next log row, populating the provided row.
func (r *IngestProtoLogsReader) getLogRow(ctx context.Context, row pipeline.Row) error {
	if r.logs == nil {
		return io.EOF
	}

	// Iterator pattern: advance through resources -> scopes -> logs
	for r.resourceIndex < r.logs.ResourceLogs().Len() {
		rl := r.logs.ResourceLogs().At(r.resourceIndex)

		for r.scopeIndex < rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(r.scopeIndex)

			if r.logIndex < sl.LogRecords().Len() {
				logRecord := sl.LogRecords().At(r.logIndex)
				r.buildLogRow(rl, sl, logRecord, row)
				r.logIndex++
				return nil
			}

			r.scopeIndex++
			r.logIndex = 0
		}

		r.resourceIndex++
		r.scopeIndex = 0
		r.logIndex = 0
	}

	return io.EOF
}

// buildLogRow populates the provided row from a single log record and its context.
func (r *IngestProtoLogsReader) buildLogRow(rl plog.ResourceLogs, sl plog.ScopeLogs, logRecord plog.LogRecord, row pipeline.Row) {
	rl.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		// Fast path: avoid conversion if already a string (90% of attributes)
		var value string
		if v.Type() == pcommon.ValueTypeStr {
			value = v.Str()
		} else {
			value = v.AsString()
		}
		row[prefixAttributeRowKey(name, "resource")] = value
		return true
	})

	sl.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		var value string
		if v.Type() == pcommon.ValueTypeStr {
			value = v.Str()
		} else {
			value = v.AsString()
		}
		row[prefixAttributeRowKey(name, "scope")] = value
		return true
	})

	logRecord.Attributes().Range(func(name string, v pcommon.Value) bool {
		var value string
		if v.Type() == pcommon.ValueTypeStr {
			value = v.Str()
		} else {
			value = v.AsString()
		}
		row[prefixAttributeRowKey(name, "attr")] = value
		return true
	})

	message := logRecord.Body().AsString()
	row[wkk.RowKeyCMessage] = message
	row[wkk.RowKeyCTimestamp] = logRecord.Timestamp().AsTime().UnixMilli()
	row[wkk.RowKeyCTsns] = int64(logRecord.Timestamp())
	row[wkk.RowKeyCLevel] = logRecord.SeverityText()
}

// GetOTELLogs returns the underlying parsed OTEL logs structure.
// This allows access to the original log body and metadata not available in the row format.
func (r *IngestProtoLogsReader) GetOTELLogs() (*plog.Logs, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}
	if r.logs == nil {
		return nil, fmt.Errorf("no logs data available")
	}
	return r.logs, nil
}

// Close closes the reader and releases resources.
func (r *IngestProtoLogsReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release references to parsed data
	r.logs = nil

	return nil
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next().
func (r *IngestProtoLogsReader) TotalRowsReturned() int64 {
	return r.rowCount
}

func parseProtoToOtelLogs(reader io.Reader) (*plog.Logs, error) {
	unmarshaler := &plog.ProtoUnmarshaler{}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	logs, err := unmarshaler.UnmarshalLogs(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf logs: %w", err)
	}

	return &logs, nil
}
