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
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"
)

func loadTestMetrics(t testing.TB) pmetric.Metrics {
	// Get path to testdata
	_, filename, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(filepath.Dir(filename)))
	testdataPath := filepath.Join(projectRoot, "testdata", "metrics", "otel-metrics.binpb.gz")

	// Read and decompress the test file
	file, err := os.Open(testdataPath)
	if err != nil {
		t.Fatalf("Failed to open test file: %v", err)
	}
	defer file.Close()

	gzReader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("Failed to create gzip reader: %v", err)
	}
	defer gzReader.Close()

	data, err := io.ReadAll(gzReader)
	if err != nil {
		t.Fatalf("Failed to read compressed data: %v", err)
	}

	// Unmarshal protobuf data
	unmarshaler := &pmetric.ProtoUnmarshaler{}
	metrics, err := unmarshaler.UnmarshalMetrics(data)
	if err != nil {
		t.Fatalf("Failed to unmarshal metrics: %v", err)
	}

	return metrics
}

func BenchmarkConvertMetricsToMap(b *testing.B) {
	// Load test data once
	metrics := loadTestMetrics(b)
	processor := NewProcessor(DefaultConfig())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		result, err := processor.convertMetricsToMap(metrics)
		if err != nil {
			b.Fatalf("convertMetricsToMap failed: %v", err)
		}
		_ = result // Prevent optimization
	}
}

func BenchmarkProcessMetricsExemplar(b *testing.B) {
	// Load test data once
	metrics := loadTestMetrics(b)
	processor := NewProcessor(DefaultConfig())

	// Set up a no-op callback to avoid database operations
	callbackCount := 0
	processor.SetMetricsCallback(func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error {
		callbackCount += len(exemplars)
		return nil
	})

	ctx := context.Background()
	organizationID := "test-org-id"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Process each resource metric
		for j := 0; j < metrics.ResourceMetrics().Len(); j++ {
			rm := metrics.ResourceMetrics().At(j)
			for k := 0; k < rm.ScopeMetrics().Len(); k++ {
				sm := rm.ScopeMetrics().At(k)
				for l := 0; l < sm.Metrics().Len(); l++ {
					mm := sm.Metrics().At(l)
					err := processor.ProcessMetrics(ctx, organizationID, rm, sm, mm)
					if err != nil {
						b.Fatalf("ProcessMetrics failed: %v", err)
					}
				}
			}
		}
	}

	b.Logf("Processed %d exemplars total", callbackCount)
}

func TestRealMetricsProcessing(t *testing.T) {
	// Load test data
	metrics := loadTestMetrics(t)
	processor := NewProcessor(DefaultConfig())

	t.Logf("Loaded metrics with %d resource metrics", metrics.ResourceMetrics().Len())

	// Count metrics and data points
	totalMetrics := 0
	totalDataPoints := 0
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		rm := metrics.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			totalMetrics += sm.Metrics().Len()
			for k := 0; k < sm.Metrics().Len(); k++ {
				mm := sm.Metrics().At(k)
				switch mm.Type() {
				case pmetric.MetricTypeGauge:
					totalDataPoints += mm.Gauge().DataPoints().Len()
				case pmetric.MetricTypeSum:
					totalDataPoints += mm.Sum().DataPoints().Len()
				case pmetric.MetricTypeHistogram:
					totalDataPoints += mm.Histogram().DataPoints().Len()
				case pmetric.MetricTypeSummary:
					totalDataPoints += mm.Summary().DataPoints().Len()
				case pmetric.MetricTypeExponentialHistogram:
					totalDataPoints += mm.ExponentialHistogram().DataPoints().Len()
				}
			}
		}
	}

	t.Logf("Total metrics: %d, total data points: %d", totalMetrics, totalDataPoints)

	// Test conversion
	result, err := processor.convertMetricsToMap(metrics)
	if err != nil {
		t.Fatalf("convertMetricsToMap failed: %v", err)
	}

	if result == nil {
		t.Fatal("Result should not be nil")
	}

	// Verify structure
	resourceMetrics, ok := result["resourceMetrics"].([]any)
	if !ok {
		t.Fatal("Result should contain resourceMetrics as array")
	}

	t.Logf("Converted to map with %d resource metrics", len(resourceMetrics))

	// Test exemplar processing
	exemplarCount := 0
	processor.SetMetricsCallback(func(ctx context.Context, organizationID string, exemplars []*ExemplarData) error {
		exemplarCount += len(exemplars)
		t.Logf("Callback received %d exemplars", len(exemplars))
		for _, exemplar := range exemplars {
			if exemplar.Payload == nil {
				t.Error("Exemplar payload should not be nil")
			}
			if len(exemplar.Attributes) == 0 {
				t.Error("Exemplar attributes should not be empty")
			}
		}
		return nil
	})

	ctx := context.Background()
	organizationID := "test-org-id"

	// Process all metrics
	for i := 0; i < metrics.ResourceMetrics().Len(); i++ {
		rm := metrics.ResourceMetrics().At(i)
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			sm := rm.ScopeMetrics().At(j)
			for k := 0; k < sm.Metrics().Len(); k++ {
				mm := sm.Metrics().At(k)
				err := processor.ProcessMetrics(ctx, organizationID, rm, sm, mm)
				if err != nil {
					t.Fatalf("ProcessMetrics failed: %v", err)
				}
			}
		}
	}

	t.Logf("Processed %d exemplars total", exemplarCount)
}