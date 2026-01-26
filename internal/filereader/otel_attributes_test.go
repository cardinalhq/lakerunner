// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"testing"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestPrefixAttribute(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		prefix   string
		expected string
	}{
		{
			name:     "empty name returns empty string",
			input:    "",
			prefix:   "foo",
			expected: "",
		},
		{
			name:     "name starts with underscore returns name",
			input:    "_internal",
			prefix:   "foo",
			expected: "_internal",
		},
		{
			name:     "normal name returns prefixed",
			input:    "bar",
			prefix:   "foo",
			expected: "foo_bar",
		},
		{
			name:     "empty prefix",
			input:    "bar",
			prefix:   "",
			expected: "_bar",
		},
		{
			name:     "underscore not at start",
			input:    "foo_bar",
			prefix:   "baz",
			expected: "baz_foo_bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := prefixAttribute(tt.input, tt.prefix)
			if result != tt.expected {
				t.Errorf("prefixAttribute(%q, %q) = %q; want %q", tt.input, tt.prefix, result, tt.expected)
			}
		})
	}
}

func TestPrefixAttributeRowKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		prefix   string
		expected string
	}{
		{
			name:     "empty name returns empty RowKey",
			input:    "",
			prefix:   "resource",
			expected: "",
		},
		{
			name:     "underscore prefix - no prefix added",
			input:    "_internal.field",
			prefix:   "resource",
			expected: "_internal_field",
		},
		{
			name:     "normal attribute with resource prefix",
			input:    "service.name",
			prefix:   "resource",
			expected: "resource_service_name",
		},
		{
			name:     "normal attribute with scope prefix",
			input:    "scope.version",
			prefix:   "scope",
			expected: "scope_scope_version",
		},
		{
			name:     "attribute with multiple dots",
			input:    "cloud.provider.region.name",
			prefix:   "resource",
			expected: "resource_cloud_provider_region_name",
		},
		{
			name:     "attribute without dots",
			input:    "hostname",
			prefix:   "resource",
			expected: "resource_hostname",
		},
		{
			name:     "underscore prefix with dots",
			input:    "_cardinalhq.timestamp",
			prefix:   "resource",
			expected: "_cardinalhq_timestamp",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := prefixAttributeRowKey(tt.input, tt.prefix)
			expected := wkk.NewRowKey(tt.expected)
			if result != expected {
				t.Errorf("prefixAttributeRowKey(%q, %q) = %v; want %v", tt.input, tt.prefix, result, expected)
			}
		})
	}
}

// Benchmark with realistic production data patterns
func BenchmarkPrefixAttributeRowKey(b *testing.B) {
	// Common OTEL attribute patterns from production
	testCases := []struct {
		name   string
		attr   string
		prefix string
	}{
		{"short_no_dots", "hostname", "resource"},
		{"medium_one_dot", "service.name", "resource"},
		{"long_many_dots", "cloud.provider.region.name", "resource"},
		{"underscore_prefix", "_cardinalhq.timestamp", "resource"},
		{"k8s_attribute", "k8s.pod.name", "resource"},
		{"scope_attribute", "scope.version", "scope"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = prefixAttributeRowKey(tc.attr, tc.prefix)
			}
		})
	}
}

// Benchmark to measure string allocation overhead
func BenchmarkPrefixAttributeRowKey_AllocPattern(b *testing.B) {
	const attr = "service.name"
	const prefix = "resource"

	b.Run("baseline", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = prefixAttributeRowKey(attr, prefix)
		}
	})
}

// Benchmark realistic workload - 10 attributes per log
func BenchmarkPrefixAttributeRowKey_Batch(b *testing.B) {
	attributes := []struct {
		name   string
		prefix string
	}{
		{"service.name", "resource"},
		{"service.version", "resource"},
		{"host.name", "resource"},
		{"k8s.pod.name", "resource"},
		{"k8s.namespace.name", "resource"},
		{"cloud.provider", "resource"},
		{"cloud.region", "resource"},
		{"_cardinalhq.timestamp", "resource"},
		{"scope.name", "scope"},
		{"scope.version", "scope"},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, attr := range attributes {
			_ = prefixAttributeRowKey(attr.name, attr.prefix)
		}
	}

	// Report per-attribute metrics
	logsProcessed := int64(b.N)
	attrsProcessed := logsProcessed * int64(len(attributes))
	b.ReportMetric(float64(attrsProcessed)/b.Elapsed().Seconds(), "attrs/sec")
}

// Test that RowKey conversion is idempotent
func TestPrefixAttributeRowKey_Idempotent(t *testing.T) {
	inputs := []struct {
		name   string
		prefix string
	}{
		{"service.name", "resource"},
		{"_internal", "resource"},
		{"", "resource"},
	}

	for _, input := range inputs {
		result1 := prefixAttributeRowKey(input.name, input.prefix)
		result2 := prefixAttributeRowKey(input.name, input.prefix)

		// Should produce same RowKey (unique.Handle comparison)
		if result1 != result2 {
			t.Errorf("prefixAttributeRowKey not idempotent for %q: %v != %v", input.name, result1, result2)
		}
	}
}

// Test that wkk.RowKey wrapping works correctly
func TestPrefixAttributeRowKey_RowKeyWrapper(t *testing.T) {
	result := prefixAttributeRowKey("service.name", "resource")
	expected := wkk.NewRowKey("resource_service_name")

	if result != expected {
		t.Errorf("RowKey mismatch: got %v, want %v", result, expected)
	}
}
