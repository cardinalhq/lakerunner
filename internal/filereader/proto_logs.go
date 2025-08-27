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
	"fmt"
	"io"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// ProtoLogsReader reads rows from OpenTelemetry protobuf logs format.
// Returns raw OTEL log data without signal-specific transformations.
type ProtoLogsReader struct {
	closed    bool
	rowCount  int64
	batchSize int

	// Streaming iterator state for logs
	logs          *plog.Logs
	resourceIndex int
	scopeIndex    int
	logIndex      int
}

// NewProtoLogsReader creates a new ProtoLogsReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewProtoLogsReader(reader io.Reader, batchSize int) (*ProtoLogsReader, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	protoReader := &ProtoLogsReader{
		batchSize: batchSize,
	}

	logs, err := parseProtoToOtelLogs(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL logs: %w", err)
	}
	protoReader.logs = logs

	return protoReader, nil
}

// Next returns the next batch of rows from the OTEL logs.
func (r *ProtoLogsReader) Next() (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	batch := pipeline.GetBatch()

	for batch.Len() < r.batchSize {
		row, err := r.getLogRow()
		if err != nil {
			if err == io.EOF {
				if batch.Len() == 0 {
					return nil, io.EOF
				}
				break
			}
			return nil, err
		}

		batchRow := batch.AddRow()
		for k, v := range row {
			batchRow[k] = v
		}
	}

	// Update row count with successfully read rows
	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		return batch, nil
	}

	return nil, io.EOF
}

// getLogRow handles reading the next log row.
func (r *ProtoLogsReader) getLogRow() (Row, error) {
	if r.logs == nil {
		return nil, io.EOF
	}

	// Iterator pattern: advance through resources -> scopes -> logs
	for r.resourceIndex < r.logs.ResourceLogs().Len() {
		rl := r.logs.ResourceLogs().At(r.resourceIndex)

		for r.scopeIndex < rl.ScopeLogs().Len() {
			sl := rl.ScopeLogs().At(r.scopeIndex)

			if r.logIndex < sl.LogRecords().Len() {
				logRecord := sl.LogRecords().At(r.logIndex)

				// Build row for this log record
				row := r.buildLogRow(rl, sl, logRecord)

				// Advance to next log record
				r.logIndex++

				return r.processRow(row)
			}

			// Move to next scope, reset log index
			r.scopeIndex++
			r.logIndex = 0
		}

		// Move to next resource, reset scope and log indices
		r.resourceIndex++
		r.scopeIndex = 0
		r.logIndex = 0
	}

	return nil, io.EOF
}

// buildLogRow creates a row from a single log record and its context.
func (r *ProtoLogsReader) buildLogRow(rl plog.ResourceLogs, sl plog.ScopeLogs, logRecord plog.LogRecord) map[string]any {
	ret := map[string]any{}

	// Add resource attributes with prefix
	rl.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "resource")] = value
		return true
	})

	// Add scope attributes with prefix
	sl.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "scope")] = value
		return true
	})

	// Add log attributes with prefix
	logRecord.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "log")] = value
		return true
	})

	// Add basic log fields
	ret["_cardinalhq.message"] = logRecord.Body().AsString()
	ret["_cardinalhq.timestamp"] = logRecord.Timestamp().AsTime().UnixMilli()
	ret["observed_timestamp"] = logRecord.ObservedTimestamp().AsTime().UnixMilli()
	ret["_cardinalhq.level"] = logRecord.SeverityText()
	ret["severity_number"] = int64(logRecord.SeverityNumber())

	return ret
}

// processRow applies any processing to a row.
func (r *ProtoLogsReader) processRow(row map[string]any) (Row, error) {
	return Row(row), nil
}

// Close closes the reader and releases resources.
func (r *ProtoLogsReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release references to parsed data
	r.logs = nil

	return nil
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next().
func (r *ProtoLogsReader) TotalRowsReturned() int64 {
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
