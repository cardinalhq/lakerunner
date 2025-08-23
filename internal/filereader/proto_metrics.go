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
	closed bool

	// Streaming state for metrics
	metrics             *pmetric.Metrics
	metricResourceIndex int
	metricQueue         []map[string]any
	metricQueueIndex    int
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

	return n, nil
}

// getMetricRow handles reading the next metric row.
func (r *ProtoMetricsReader) getMetricRow() (Row, error) {
	if r.metrics == nil {
		return nil, io.EOF
	}

	// If we have rows in the queue, return the next one
	if r.metricQueueIndex < len(r.metricQueue) {
		row := r.metricQueue[r.metricQueueIndex]
		r.metricQueueIndex++
		return r.processRow(row)
	}

	// Process next resource if available
	if r.metricResourceIndex >= r.metrics.ResourceMetrics().Len() {
		return nil, io.EOF
	}

	resourceMetric := r.metrics.ResourceMetrics().At(r.metricResourceIndex)

	// Create a single resource metric for processing
	singleResourceMetric := pmetric.NewMetrics()
	newResourceMetric := singleResourceMetric.ResourceMetrics().AppendEmpty()
	resourceMetric.CopyTo(newResourceMetric)

	// Convert to raw rows
	rows := r.rawMetricsFromOtel(&singleResourceMetric)

	r.metricQueue = rows
	r.metricQueueIndex = 0
	r.metricResourceIndex++

	// Recursively call to return the first row from the new queue
	return r.getMetricRow()
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
	r.metricQueue = nil

	return nil
}

// rawMetricsFromOtel converts OTEL metrics to raw rows without signal-specific transformations
func (r *ProtoMetricsReader) rawMetricsFromOtel(om *pmetric.Metrics) []map[string]any {
	var rets []map[string]any

	for i := 0; i < om.ResourceMetrics().Len(); i++ {
		rm := om.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			imm := rm.ScopeMetrics().At(j)
			for k := 0; k < imm.Metrics().Len(); k++ {
				metric := imm.Metrics().At(k)
				ret := map[string]any{}

				// Add resource attributes with prefix
				rm.Resource().Attributes().Range(func(name string, v pcommon.Value) bool {
					ret["resource."+name] = v.AsString()
					return true
				})

				// Add scope attributes with prefix
				imm.Scope().Attributes().Range(func(name string, v pcommon.Value) bool {
					ret["scope."+name] = v.AsString()
					return true
				})

				// Basic metric fields - raw data only
				ret["name"] = metric.Name()
				ret["description"] = metric.Description()
				ret["unit"] = metric.Unit()
				ret["type"] = metric.Type().String()

				rets = append(rets, ret)
			}
		}
	}

	return rets
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
