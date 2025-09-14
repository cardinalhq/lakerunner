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

package exemplars

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// createSyntheticSumMetric creates a Sum metric with unique name and tid
func createSyntheticSumMetric(metricIndex int, tid int64) (pmetric.ResourceMetrics, pmetric.ScopeMetrics, pmetric.Metric) {
	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()

	// Add resource attributes
	resource := rm.Resource()
	resource.Attributes().PutStr("service.name", "test-service")
	resource.Attributes().PutStr("service.instance.id", "test-instance-1")
	resource.Attributes().PutStr("k8s.cluster.name", "test-cluster")
	resource.Attributes().PutStr("k8s.namespace.name", "test-namespace")
	resource.Attributes().PutStr("_cardinalhq.customer_id", "test-customer-123")
	resource.Attributes().PutStr("_cardinalhq.collector_id", "test-collector")

	sm := rm.ScopeMetrics().AppendEmpty()
	scope := sm.Scope()
	scope.SetName("test-scope")
	scope.SetVersion("v1.0.0")

	metric := sm.Metrics().AppendEmpty()
	metricName := fmt.Sprintf("synthetic_metric_%d", metricIndex)
	metric.SetName(metricName)
	metric.SetDescription(fmt.Sprintf("Synthetic test metric %d", metricIndex))
	metric.SetUnit("count")

	// Create Sum metric
	sum := metric.SetEmptySum()
	sum.SetIsMonotonic(true)
	sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)

	// Add a data point with _cardinalhq.tid attribute
	dp := sum.DataPoints().AppendEmpty()
	dp.SetIntValue(int64(metricIndex * 100))
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	dp.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Now().Add(-1 * time.Minute)))

	// Set the tid attribute on the data point
	dp.Attributes().PutInt("_cardinalhq.tid", tid)
	dp.Attributes().PutStr("instance", "test-instance")
	dp.Attributes().PutStr("job", "test-job")

	return rm, sm, metric
}

// BenchmarkProcessMetricsSynthetic benchmarks ProcessMetrics with synthetic data
func BenchmarkProcessMetricsSynthetic(b *testing.B) {
	processor := NewProcessor(DefaultConfig())
	organizationID := "test-org-123"
	ctx := context.Background()

	b.ReportAllocs()

	i := 0
	for b.Loop() {
		tid := int64(i + 1000000) // Start with a base to simulate real tids
		rm, sm, m := createSyntheticSumMetric(i, tid)

		err := processor.ProcessMetrics(ctx, organizationID, rm, sm, m)
		if err != nil {
			b.Fatalf("ProcessMetrics failed: %v", err)
		}
		i++
	}
}

// BenchmarkProcessMetricsWithFlush benchmarks ProcessMetrics with periodic flushing
func BenchmarkProcessMetricsWithFlush(b *testing.B) {
	config := DefaultConfig()
	config.Metrics.ReportInterval = 100 * time.Millisecond // Flush frequently for benchmark
	config.Metrics.CacheSize = 1000

	processor := NewProcessor(config)
	organizationID := "test-org-123"
	ctx := context.Background()

	// Set up callback to track exemplars
	var exemplarCount int
	processor.SetMetricsCallback(func(ctx context.Context, orgID string, exemplars []*ExemplarData) error {
		exemplarCount += len(exemplars)
		return nil
	})

	b.ReportAllocs()

	i := 0
	for b.Loop() {
		tid := int64(i + 2000000)
		rm, sm, m := createSyntheticSumMetric(i, tid)

		err := processor.ProcessMetrics(ctx, organizationID, rm, sm, m)
		if err != nil {
			b.Fatalf("ProcessMetrics failed: %v", err)
		}
		i++
	}

	// Clean up
	_ = processor.Close()

	b.Logf("Processed %d exemplars", exemplarCount)
}

// BenchmarkProcessMetricsMemoryUsage measures memory usage patterns
func BenchmarkProcessMetricsMemoryUsage(b *testing.B) {
	processor := NewProcessor(DefaultConfig())
	organizationID := "test-org-123"
	ctx := context.Background()

	// Create a variety of metrics with different characteristics
	const numUniqueMetrics = 1000
	metrics := make([]struct {
		rm pmetric.ResourceMetrics
		sm pmetric.ScopeMetrics
		m  pmetric.Metric
	}, numUniqueMetrics)

	for i := range numUniqueMetrics {
		tid := int64(i + 3000000)
		rm, sm, m := createSyntheticSumMetric(i, tid)
		metrics[i] = struct {
			rm pmetric.ResourceMetrics
			sm pmetric.ScopeMetrics
			m  pmetric.Metric
		}{rm, sm, m}
	}

	b.ReportAllocs()

	// Process metrics multiple times to simulate cache behavior
	i := 0
	for b.Loop() {
		metricIndex := i % numUniqueMetrics
		err := processor.ProcessMetrics(ctx, organizationID,
			metrics[metricIndex].rm,
			metrics[metricIndex].sm,
			metrics[metricIndex].m)
		if err != nil {
			b.Fatalf("ProcessMetrics failed: %v", err)
		}
		i++
	}

	_ = processor.Close()
}
