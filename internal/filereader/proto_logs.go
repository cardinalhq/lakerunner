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
	closed bool

	// Streaming state for logs
	logs                 *plog.Logs
	currentResourceIndex int
	logQueue             []map[string]any
	queueIndex           int
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

	return n, nil
}

// getLogRow handles reading the next log row.
func (r *ProtoLogsReader) getLogRow() (Row, error) {
	if r.logs == nil {
		return nil, io.EOF
	}

	// If we have rows in the queue, return the next one
	if r.queueIndex < len(r.logQueue) {
		row := r.logQueue[r.queueIndex]
		r.queueIndex++
		return r.processRow(row)
	}

	// Process next resource if available
	if r.currentResourceIndex >= r.logs.ResourceLogs().Len() {
		return nil, io.EOF
	}

	resourceLog := r.logs.ResourceLogs().At(r.currentResourceIndex)

	// Create a single resource log for processing
	singleResourceLog := plog.NewLogs()
	newResourceLog := singleResourceLog.ResourceLogs().AppendEmpty()
	resourceLog.CopyTo(newResourceLog)

	// Convert to raw rows without signal-specific transformation
	rows := r.rawLogsFromOtel(&singleResourceLog)

	r.logQueue = rows
	r.queueIndex = 0
	r.currentResourceIndex++

	// Recursively call to return the first row from the new queue
	return r.getLogRow()
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
	r.logQueue = nil

	return nil
}

// rawLogsFromOtel converts OTEL logs to raw rows without any transformations
func (r *ProtoLogsReader) rawLogsFromOtel(ol *plog.Logs) []map[string]any {
	var rets []map[string]any

	for i := 0; i < ol.ResourceLogs().Len(); i++ {
		rl := ol.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			ill := rl.ScopeLogs().At(j)
			for k := 0; k < ill.LogRecords().Len(); k++ {
				log := ill.LogRecords().At(k)
				ret := map[string]any{}

				// Add resource attributes with prefix
				rl.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
					ret["resource."+name] = v.AsString()
					return true
				})

				// Add scope attributes with prefix
				ill.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
					ret["scope."+name] = v.AsString()
					return true
				})

				// Add log attributes with prefix
				log.Attributes().Range(func(name string, v pcommon.Value) bool {
					ret["log."+name] = v.AsString()
					return true
				})

				// Add basic log fields
				ret["body"] = log.Body().AsString()
				ret["timestamp"] = log.Timestamp().AsTime().UnixMilli()
				ret["observed_timestamp"] = log.ObservedTimestamp().AsTime().UnixMilli()
				ret["severity_text"] = log.SeverityText()
				ret["severity_number"] = int32(log.SeverityNumber())

				rets = append(rets, ret)
			}
		}
	}

	return rets
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
