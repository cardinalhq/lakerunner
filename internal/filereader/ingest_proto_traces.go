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
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// IngestProtoTracesReader reads rows from OpenTelemetry protobuf traces format.
// Returns raw OTEL trace data without signal-specific transformations.
type IngestProtoTracesReader struct {
	closed    bool
	rowCount  int64
	batchSize int

	// Streaming iterator state for traces
	traces        *ptrace.Traces
	resourceIndex int
	scopeIndex    int
	spanIndex     int
	orgId         string
}

// NewProtoTracesReader creates a new ProtoTracesReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewProtoTracesReader(reader io.Reader, batchSize int) (*IngestProtoTracesReader, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}

	protoReader := &IngestProtoTracesReader{
		batchSize: batchSize,
	}

	traces, err := parseProtoToOtelTraces(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL traces: %w", err)
	}
	protoReader.traces = traces

	return protoReader, nil
}

// NewIngestProtoTracesReader creates a new ProtoTracesReader for ingestion with exemplar processing.
func NewIngestProtoTracesReader(reader io.Reader, opts ReaderOptions) (*IngestProtoTracesReader, error) {
	batchSize := opts.BatchSize
	if batchSize <= 0 {
		batchSize = 1000
	}

	protoReader := &IngestProtoTracesReader{
		batchSize: batchSize,
		orgId:     opts.OrgID,
	}

	traces, err := parseProtoToOtelTraces(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL traces: %w", err)
	}
	protoReader.traces = traces

	return protoReader, nil
}

// Next returns the next batch of rows from the OTEL traces.
func (r *IngestProtoTracesReader) Next(ctx context.Context) (*Batch, error) {
	if r.closed {
		return nil, fmt.Errorf("reader is closed")
	}

	batch := pipeline.GetBatch()

	for batch.Len() < r.batchSize {
		sourceRow, err := r.getTraceRow(ctx)
		if err != nil {
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

		// Track trace spans read from proto
		rowsInCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("reader", "ProtoTracesReader"),
		))

		// Copy to batch's reusable Row map
		row := batch.AddRow()
		for k, v := range sourceRow {
			row[k] = v
		}
	}

	// Update row count with successfully read rows
	if batch.Len() > 0 {
		r.rowCount += int64(batch.Len())
		// Track rows output to downstream
		rowsOutCounter.Add(ctx, int64(batch.Len()), otelmetric.WithAttributes(
			attribute.String("reader", "ProtoTracesReader"),
		))
		return batch, nil
	}

	pipeline.ReturnBatch(batch)
	return nil, io.EOF
}

// getTraceRow handles reading the next trace row.
func (r *IngestProtoTracesReader) getTraceRow(ctx context.Context) (pipeline.Row, error) {
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
				row := r.buildSpanRow(ctx, rs, ss, span)

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
func (r *IngestProtoTracesReader) buildSpanRow(ctx context.Context, rs ptrace.ResourceSpans, ss ptrace.ScopeSpans, span ptrace.Span) map[string]any {
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
	ret["_cardinalhq_span_trace_id"] = span.TraceID().String()
	ret["_cardinalhq_span_id"] = span.SpanID().String()
	ret["_cardinalhq_parent_span_id"] = span.ParentSpanID().String()
	ret["_cardinalhq_name"] = span.Name()
	ret["_cardinalhq_kind"] = span.Kind().String()
	ret["_cardinalhq_status_code"] = span.Status().Code().String()
	ret["_cardinalhq_status_message"] = span.Status().Message()

	// Handle start timestamp with fallback
	if span.StartTimestamp() != 0 {
		ret["_cardinalhq_timestamp"] = span.StartTimestamp().AsTime().UnixMilli()
		ret["_cardinalhq_tsns"] = int64(span.StartTimestamp())
	} else {
		// Fallback to current time when start timestamp is zero
		currentTime := time.Now()
		ret["_cardinalhq_timestamp"] = currentTime.UnixMilli()
		ret["_cardinalhq_tsns"] = currentTime.UnixNano()
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "traces"),
			attribute.String("reason", "current_fallback"),
		))
	}

	// Handle end timestamp with fallback
	if span.EndTimestamp() != 0 {
		ret["_cardinalhq_end_timestamp"] = span.EndTimestamp().AsTime().UnixMilli()
		// Calculate duration using actual timestamps
		if span.StartTimestamp() != 0 {
			ret["_cardinalhq_span_duration"] = span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Milliseconds()
		} else {
			// If start timestamp was fallback, use 0 duration
			ret["_cardinalhq_span_duration"] = int64(0)
		}
	} else {
		// Fallback to current time for end timestamp
		currentTime := time.Now()
		ret["_cardinalhq_end_timestamp"] = currentTime.UnixMilli()
		timestampFallbackCounter.Add(ctx, 1, otelmetric.WithAttributes(
			attribute.String("signal_type", "traces"),
			attribute.String("reason", "current_fallback"),
		))
		// Calculate duration if we have a valid start timestamp
		if span.StartTimestamp() != 0 {
			ret["_cardinalhq_span_duration"] = currentTime.Sub(span.StartTimestamp().AsTime()).Milliseconds()
		} else {
			ret["_cardinalhq_span_duration"] = int64(0)
		}
	}

	return ret
}

// processRow applies any processing to a row.
func (r *IngestProtoTracesReader) processRow(row map[string]any) (pipeline.Row, error) {
	result := make(pipeline.Row)
	for k, v := range row {
		result[wkk.NewRowKey(k)] = v
	}
	return result, nil
}

// Close closes the reader and releases resources.
func (r *IngestProtoTracesReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release references to parsed data
	r.traces = nil

	return nil
}

// TotalRowsReturned returns the total number of rows that have been successfully returned via Next().
func (r *IngestProtoTracesReader) TotalRowsReturned() int64 {
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
