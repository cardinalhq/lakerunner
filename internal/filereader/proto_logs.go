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

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// ProtoLogsReader reads rows from OpenTelemetry protobuf logs format.
// Returns raw OTEL log data without signal-specific transformations.
type ProtoLogsReader struct {
	closed   bool
	rowCount int64

	// Streaming iterator state for logs
	logs          *plog.Logs
	resourceIndex int
	scopeIndex    int
	logIndex      int
}

// NewProtoLogsReader creates a new ProtoLogsReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewProtoLogsReader(reader io.Reader) (*ProtoLogsReader, error) {
	protoReader := &ProtoLogsReader{}

	logs, err := parseProtoToOtelLogs(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL logs: %w", err)
	}
	protoReader.logs = logs

	return protoReader, nil
}

// Read populates the provided slice with as many rows as possible.
func (r *ProtoLogsReader) Read(rows []Row) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	n := 0
	for n < len(rows) {
		row, err := r.getLogRow()
		if err != nil {
			return n, err
		}

		resetRow(&rows[n])

		// Copy data to Row
		for k, v := range row {
			rows[n][k] = v
		}

		n++
	}

	// Update row count with successfully read rows
	if n > 0 {
		r.rowCount += int64(n)
	}

	return n, nil
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
		ret["resource."+name] = v.AsString()
		return true
	})

	// Add scope attributes with prefix
	sl.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		ret["scope."+name] = v.AsString()
		return true
	})

	// Add log attributes with prefix
	logRecord.Attributes().Range(func(name string, v pcommon.Value) bool {
		ret["log."+name] = v.AsString()
		return true
	})

	// Add basic log fields
	ret["body"] = logRecord.Body().AsString()
	ret["timestamp"] = logRecord.Timestamp().AsTime().UnixMilli()
	ret["observed_timestamp"] = logRecord.ObservedTimestamp().AsTime().UnixMilli()
	ret["severity_text"] = logRecord.SeverityText()
	ret["severity_number"] = int32(logRecord.SeverityNumber())

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

// RowCount returns the total number of rows that have been successfully read.
func (r *ProtoLogsReader) RowCount() int64 {
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
