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
	closed    bool
	rowCount  int64
	batchSize int

	// Streaming iterator state for traces
	traces        *ptrace.Traces
	resourceIndex int
	scopeIndex    int
	spanIndex     int
}

// NewProtoTracesReader creates a new ProtoTracesReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewProtoTracesReader(reader io.Reader, batchSize int) (*ProtoTracesReader, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	protoReader := &ProtoTracesReader{
		batchSize: batchSize,
	}

	traces, err := parseProtoToOtelTraces(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL traces: %w", err)
	}
	protoReader.traces = traces

	return protoReader, nil
}

// Next returns the next batch of rows from the OTEL traces.
func (r *ProtoTracesReader) Next() (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	batch := &Batch{
		Rows: make([]Row, 0, r.batchSize),
	}

	for len(batch.Rows) < r.batchSize {
		row, err := r.getTraceRow()
		if err != nil {
			if err == io.EOF {
				if len(batch.Rows) == 0 {
					return nil, io.EOF
				}
				break
			}
			return nil, err
		}

		batch.Rows = append(batch.Rows, row)
	}

	// Update row count with successfully read rows
	if len(batch.Rows) > 0 {
		r.rowCount += int64(len(batch.Rows))
		return batch, nil
	}

	return nil, io.EOF
}

// getTraceRow handles reading the next trace row.
func (r *ProtoTracesReader) getTraceRow() (Row, error) {
	if r.traces == nil {
		return nil, io.EOF
	}

	// Iterator pattern: advance through resources -> scopes -> spans
	for r.resourceIndex < r.traces.ResourceSpans().Len() {
		rs := r.traces.ResourceSpans().At(r.resourceIndex)

		for r.scopeIndex < rs.ScopeSpans().Len() {
			ss := rs.ScopeSpans().At(r.scopeIndex)

			if r.spanIndex < ss.Spans().Len() {
				span := ss.Spans().At(r.spanIndex)

				// Build row for this span
				row := r.buildSpanRow(rs, ss, span)

				// Advance to next span
				r.spanIndex++

				return r.processRow(row)
			}

			// Move to next scope, reset span index
			r.scopeIndex++
			r.spanIndex = 0
		}

		// Move to next resource, reset scope and span indices
		r.resourceIndex++
		r.scopeIndex = 0
		r.spanIndex = 0
	}

	return nil, io.EOF
}

// buildSpanRow creates a row from a single span and its context.
func (r *ProtoTracesReader) buildSpanRow(rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span) map[string]any {
	ret := map[string]any{}

	// Add resource attributes with prefix
	rs.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "resource")] = value
		return true
	})

	// Add scope attributes with prefix
	ss.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "scope")] = value
		return true
	})

	// Add span attributes with prefix
	span.Attributes().Range(func(name string, v pcommon.Value) bool {
		value := v.AsString()
		ret[prefixAttribute(name, "span")] = value
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

	return ret
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

	return nil
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Read().
func (r *ProtoTracesReader) TotalRowsReturned() int64 {
	return r.rowCount
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
