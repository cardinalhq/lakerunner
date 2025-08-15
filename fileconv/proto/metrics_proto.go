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

package proto

import (
	"fmt"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/cardinalhq/lakerunner/cmd/otel"
	"github.com/cardinalhq/lakerunner/fileconv"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/idgen"
)

type MetricsProtoReader struct {
	metrics *pmetric.Metrics
	// Streaming state
	currentResourceIndex int
	metricQueue          []map[string]any
	queueIndex           int
	mapper               *translate.Mapper
	tags                 map[string]string
	translator           *otel.TableTranslator
	idg                  idgen.IDGenerator
}

var _ fileconv.Reader = (*MetricsProtoReader)(nil)

func NewMetricsProtoReader(data []byte, mapper *translate.Mapper, tags map[string]string) (*MetricsProtoReader, error) {
	metrics, err := parseProtoToOtelMetricsFromBytes(data)
	if err != nil {
		return nil, fmt.Errorf("failed to parse proto to OTEL metrics: %w", err)
	}

	return NewMetricsProtoReaderFromMetrics(metrics, mapper, tags)
}

func NewMetricsProtoReaderFromMetrics(metrics *pmetric.Metrics, mapper *translate.Mapper, tags map[string]string) (*MetricsProtoReader, error) {
	translator := otel.NewTableTranslator()
	idg := idgen.NewULIDGenerator()

	return &MetricsProtoReader{
		metrics:              metrics,
		currentResourceIndex: 0,
		metricQueue:          nil,
		queueIndex:           0,
		mapper:               mapper,
		tags:                 tags,
		translator:           translator,
		idg:                  idg,
	}, nil
}

func (r *MetricsProtoReader) Close() error {
	r.metrics = nil
	return nil
}

func (r *MetricsProtoReader) GetRow() (row map[string]any, done bool, err error) {
	if r.metrics == nil {
		return nil, true, fmt.Errorf("proto metrics are not initialized")
	}

	if r.queueIndex >= len(r.metricQueue) {
		if !r.loadNextResourceMetric() {
			return nil, true, nil // No more resource metrics
		}
	}

	metric := r.metricQueue[r.queueIndex]

	for k, v := range r.tags {
		metric[k] = v
	}

	r.queueIndex++

	return metric, false, nil
}

// loadNextResourceMetric loads the next resource metric and populates the queue
func (r *MetricsProtoReader) loadNextResourceMetric() bool {
	if r.currentResourceIndex >= r.metrics.ResourceMetrics().Len() {
		return false
	}

	resourceMetric := r.metrics.ResourceMetrics().At(r.currentResourceIndex)

	singleResourceMetric := pmetric.NewMetrics()
	newResourceMetric := singleResourceMetric.ResourceMetrics().AppendEmpty()

	resourceMetric.CopyTo(newResourceMetric)

	convertedMetrics, err := r.translator.MetricsFromOtel(&singleResourceMetric, nil)
	if err != nil {
		r.currentResourceIndex++
		return r.loadNextResourceMetric()
	}

	r.metricQueue = convertedMetrics
	r.queueIndex = 0
	r.currentResourceIndex++

	return true
}

func parseProtoToOtelMetricsFromBytes(data []byte) (*pmetric.Metrics, error) {
	unmarshaler := &pmetric.ProtoUnmarshaler{}

	metrics, err := unmarshaler.UnmarshalMetrics(data)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal protobuf metrics: %w", err)
	}

	return &metrics, nil
}
