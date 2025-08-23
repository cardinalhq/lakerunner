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
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// ProtoTracesReader reads rows from OpenTelemetry protobuf traces format.
// Returns raw OTEL trace data without signal-specific transformations.
type ProtoTracesReader struct {
	closed bool

	// Streaming state for traces
	traces             *ptrace.Traces
	traceResourceIndex int
	traceQueue         []map[string]any
	traceQueueIndex    int
}

// NewProtoTracesReader creates a new ProtoTracesReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewProtoTracesReader(reader io.Reader) (*ProtoTracesReader, error) {
	protoReader := &ProtoTracesReader{}

	traces, err := parseProtoToOtelTraces(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL traces: %w", err)
	}
	protoReader.traces = traces

	return protoReader, nil
}

// Read populates the provided slice with as many rows as possible.
func (r *ProtoTracesReader) Read(rows []Row) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	n := 0
	for n < len(rows) {
		row, err := r.getTraceRow()
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

// getTraceRow handles reading the next trace row.
func (r *ProtoTracesReader) getTraceRow() (Row, error) {
	if r.traces == nil {
		return nil, io.EOF
	}

	// If we have rows in the queue, return the next one
	if r.traceQueueIndex < len(r.traceQueue) {
		row := r.traceQueue[r.traceQueueIndex]
		r.traceQueueIndex++
		return r.processRow(row)
	}

	// Process next resource if available
	if r.traceResourceIndex >= r.traces.ResourceSpans().Len() {
		return nil, io.EOF
	}

	resourceSpan := r.traces.ResourceSpans().At(r.traceResourceIndex)

	// Create a single resource span for processing
	singleResourceSpan := ptrace.NewTraces()
	newResourceSpan := singleResourceSpan.ResourceSpans().AppendEmpty()
	resourceSpan.CopyTo(newResourceSpan)

	// Convert to raw rows
	rows := r.rawTracesFromOtel(&singleResourceSpan)

	r.traceQueue = rows
	r.traceQueueIndex = 0
	r.traceResourceIndex++

	// Recursively call to return the first row from the new queue
	return r.getTraceRow()
}

// processRow applies any processing to a row.
func (r *ProtoTracesReader) processRow(row map[string]any) (Row, error) {
	return Row(row), nil
}

// Close closes the reader and releases resources.
func (r *ProtoTracesReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release references to parsed data
	r.traces = nil
	r.traceQueue = nil

	return nil
}

// rawTracesFromOtel converts OTEL traces to raw rows without signal-specific transformations
func (r *ProtoTracesReader) rawTracesFromOtel(ot *ptrace.Traces) []map[string]any {
	var rets []map[string]any

	for i := 0; i < ot.ResourceSpans().Len(); i++ {
		rs := ot.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				ret := map[string]any{}

				// Add resource attributes with prefix
				rs.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
					ret["resource."+name] = v.AsString()
					return true
				})

				// Add scope attributes with prefix
				ss.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
					ret["scope."+name] = v.AsString()
					return true
				})

				// Add span attributes with prefix
				span.Attributes().Range(func(name string, v pcommon.Value) bool {
					ret["span."+name] = v.AsString()
					return true
				})

				// Basic span fields - raw data only
				ret["trace_id"] = span.TraceID().String()
				ret["span_id"] = span.SpanID().String()
				ret["parent_span_id"] = span.ParentSpanID().String()
				ret["name"] = span.Name()
				ret["kind"] = span.Kind().String()
				ret["status_code"] = span.Status().Code().String()
				ret["status_message"] = span.Status().Message()
				ret["start_timestamp"] = span.StartTimestamp().AsTime().UnixMilli()
				ret["end_timestamp"] = span.EndTimestamp().AsTime().UnixMilli()

				rets = append(rets, ret)
			}
		}
	}

	return rets
}

func parseProtoToOtelTraces(reader io.Reader) (*ptrace.Traces, error) {
	unmarshaler := &ptrace.ProtoUnmarshaler{}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	traces, err := unmarshaler.UnmarshalTraces(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf traces: %w", err)
	}

	return &traces, nil
}
