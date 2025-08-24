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
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// ProtoMetricsReader reads rows from OpenTelemetry protobuf metrics format.
type ProtoMetricsReader struct {
	closed   bool
	rowCount int64

	// Streaming iterator state for metrics
	metrics       *pmetric.Metrics
	resourceIndex int
	scopeIndex    int
	metricIndex   int
}

// NewProtoMetricsReader creates a new ProtoMetricsReader for the given io.Reader.
// The caller is responsible for closing the underlying reader.
func NewProtoMetricsReader(reader io.Reader) (*ProtoMetricsReader, error) {
	protoReader := &ProtoMetricsReader{}

	metrics, err := parseProtoToOtelMetrics(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL metrics: %w", err)
	}
	protoReader.metrics = metrics

	return protoReader, nil
}

// Read populates the provided slice with as many rows as possible.
func (r *ProtoMetricsReader) Read(rows []Row) (int, error) {
	if r.closed {
		return 0, fmt.Errorf("reader is closed")
	}

	if len(rows) == 0 {
		return 0, nil
	}

	n := 0
	for n < len(rows) {
		row, err := r.getMetricRow()
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

// getMetricRow handles reading the next metric row using streaming iteration.
func (r *ProtoMetricsReader) getMetricRow() (Row, error) {
	if r.metrics == nil {
		return nil, io.EOF
	}

	// Iterator pattern: advance through resources -> scopes -> metrics
	for r.resourceIndex < r.metrics.ResourceMetrics().Len() {
		rm := r.metrics.ResourceMetrics().At(r.resourceIndex)

		for r.scopeIndex < rm.ScopeMetrics().Len() {
			sm := rm.ScopeMetrics().At(r.scopeIndex)

			if r.metricIndex < sm.Metrics().Len() {
				metric := sm.Metrics().At(r.metricIndex)

				// Build row for this metric
				row := r.buildMetricRow(rm, sm, metric)

				// Advance to next metric
				r.metricIndex++

				return r.processRow(row)
			}

			// Move to next scope, reset metric index
			r.scopeIndex++
			r.metricIndex = 0
		}

		// Move to next resource, reset scope and metric indices
		r.resourceIndex++
		r.scopeIndex = 0
		r.metricIndex = 0
	}

	return nil, io.EOF
}

// buildMetricRow creates a row from a single metric and its context.
func (r *ProtoMetricsReader) buildMetricRow(rm pmetric.ResourceMetrics, sm pmetric.ScopeMetrics, metric pmetric.Metric) map[string]any {
	ret := map[string]any{}

	// Add resource attributes with prefix
	rm.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
		ret["resource."+name] = v.AsString()
		return true
	})

	// Add scope attributes with prefix
	sm.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
		ret["scope."+name] = v.AsString()
		return true
	})

	// Basic metric fields - raw data only
	ret["name"] = metric.Name()
	ret["description"] = metric.Description()
	ret["unit"] = metric.Unit()
	ret["type"] = metric.Type().String()

	return ret
}

// processRow applies any processing to a row.
func (r *ProtoMetricsReader) processRow(row map[string]any) (Row, error) {
	return Row(row), nil
}

// Close closes the reader and releases resources.
func (r *ProtoMetricsReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Release references to parsed data
	r.metrics = nil

	return nil
}

// RowCount returns the total number of rows that have been successfully read.
func (r *ProtoMetricsReader) RowCount() int64 {
	return r.rowCount
}

func parseProtoToOtelMetrics(reader io.Reader) (*pmetric.Metrics, error) {
	unmarshaler := &pmetric.ProtoUnmarshaler{}

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read data: %w", err)
	}

	metrics, err := unmarshaler.UnmarshalMetrics(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf metrics: %w", err)
	}

	return &metrics, nil
}
