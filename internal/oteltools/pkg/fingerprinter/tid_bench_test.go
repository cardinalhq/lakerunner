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

package fingerprinter

import (
	"testing"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func BenchmarkComputeTID(b *testing.B) {
	// Start with wkk map as that's what we have in practice
	wkkTags := map[wkk.RowKey]any{
		wkk.NewRowKey("metric_name"):           "http.requests.total",
		wkk.NewRowKey("chq_metric_type"):       "counter",
		wkk.NewRowKey("resource_service_name"): "api-gateway",
		wkk.NewRowKey("resource_environment"):  "production",
		wkk.NewRowKey("resource_region"):       "us-east-1",
		wkk.NewRowKey("metric_http_method"):    "GET",
		wkk.NewRowKey("metric_http_status"):    "200",
		wkk.NewRowKey("metric_endpoint"):       "/api/v1/users",
		wkk.NewRowKey("other_field"):           "ignored",  // Should be skipped
		wkk.NewRowKey("timestamp"):             1234567890, // Non-string, should be skipped
	}

	// Benchmark the optimized WKK version
	b.Run("Standard", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ComputeTID(wkkTags)
		}
	})

	// Benchmark with different map sizes
	b.Run("Small", func(b *testing.B) {
		smallTags := map[wkk.RowKey]any{
			wkk.NewRowKey("metric_name"):           "test.metric",
			wkk.NewRowKey("resource_service_name"): "test-service",
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ComputeTID(smallTags)
		}
	})

	b.Run("Large", func(b *testing.B) {
		largeTags := make(map[wkk.RowKey]any, 50)
		// Add many resource and metric fields
		for i := 0; i < 20; i++ {
			largeTags[wkk.NewRowKey(string("resource_field_"+string(rune('a'+i))))] = "value"
			largeTags[wkk.NewRowKey(string("metric_field_"+string(rune('a'+i))))] = "value"
		}
		// Add some fields that should be ignored
		for i := 0; i < 10; i++ {
			largeTags[wkk.NewRowKey(string("other_field_"+string(rune('a'+i))))] = "ignored"
		}
		largeTags[wkk.NewRowKey("metric_name")] = "test.metric"

		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = ComputeTID(largeTags)
		}
	})
}
