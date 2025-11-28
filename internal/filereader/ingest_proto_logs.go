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
	"bytes"
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

	// Schema extracted from all logs
	schema *ReaderSchema
}

var _ Reader = (*IngestProtoLogsReader)(nil)
var _ Reader = (*IngestProtoLogsReader)(nil)

// NewIngestProtoLogsReader creates a new IngestProtoLogsReader for the given io.Reader.
func NewIngestProtoLogsReader(reader io.Reader, opts ReaderOptions) (*IngestProtoLogsReader, error) {
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	logs, err := parseProtoToOtelLogs(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL logs: %w", err)
	}

	// Extract schema from all logs (two-pass approach)
	schema := extractSchemaFromOTELLogs(logs)

	protoReader := &IngestProtoLogsReader{
		batchSize: batchSize,
		orgId:     opts.OrgID,
		logs:      logs,
		schema:    schema,
	}

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

	for r.resourceIndex < r.logs.ResourceLogs().Len() {
		rl := r.logs.ResourceLogs().At(r.resourceIndex)

		for r.scopeIndex < rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(r.scopeIndex)

			if r.logIndex < sl.LogRecords().Len() {
				logRecord := sl.LogRecords().At(r.logIndex)
				r.buildLogRow(rl, sl, logRecord, row)
				if err := normalizeRow(ctx, row, r.schema); err != nil {
					return err
				}
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
// Values are extracted with their native types based on OTEL value type.
func (r *IngestProtoLogsReader) buildLogRow(rl plog.ResourceLogs, sl plog.ScopeLogs, logRecord plog.LogRecord, row pipeline.Row) {
	rl.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := otelValueToGoValue(v)
		row[prefixAttributeRowKey(name, "resource")] = value
		return true
	})

	sl.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := otelValueToGoValue(v)
		row[prefixAttributeRowKey(name, "scope")] = value
		return true
	})

	logRecord.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := otelValueToGoValue(v)
		row[prefixAttributeRowKey(name, "attr")] = value
		return true
	})

	message := logRecord.Body().AsString()
	row[wkk.RowKeyCMessage] = message
	row[wkk.RowKeyCTimestamp] = logRecord.Timestamp().AsTime().UnixMilli()
	row[wkk.RowKeyCTsns] = int64(logRecord.Timestamp())
	row[wkk.RowKeyCLevel] = logRecord.SeverityText()
}

// otelValueToGoValue converts an OTEL pcommon.Value to a Go value with the correct type.
func otelValueToGoValue(v pcommon.Value) any {
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return v.Str()
	case pcommon.ValueTypeInt:
		return v.Int()
	case pcommon.ValueTypeDouble:
		return v.Double()
	case pcommon.ValueTypeBool:
		return v.Bool()
	case pcommon.ValueTypeBytes:
		return v.Bytes().AsRaw()
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		// Convert complex types to JSON string
		return v.AsString()
	case pcommon.ValueTypeEmpty:
		return ""
	default:
		return v.AsString()
	}
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

// GetSchema returns the schema extracted from the OTEL logs.
func (r *IngestProtoLogsReader) GetSchema() *ReaderSchema {
	return r.schema
}

func parseProtoToOtelLogs(reader io.Reader) (*plog.Logs, error) {
	unmarshaler := &plog.ProtoUnmarshaler{}

	// Use bytes.Buffer with pre-allocated capacity to avoid exponential growth
	// Typical gzip compression ratio for protobuf is 5-10x
	// Start with reasonable capacity to handle small-medium files without reallocation
	var buf bytes.Buffer
	buf.Grow(128 * 1024) // Pre-allocate 128KB

	_, err := io.Copy(&buf, reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	logs, err := unmarshaler.UnmarshalLogs(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf logs: %w", err)
	}

	return &logs, nil
}
