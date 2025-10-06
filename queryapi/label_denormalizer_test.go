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

package queryapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLabelDenormalizer_SegmentMap(t *testing.T) {
	segmentMaps := map[int64]map[string]string{
		123: {
			"resource_service_name":   "resource.service.name",
			"resource_bucket_name":    "resource.bucket.name",
			"_cardinalhq_fingerprint": "_cardinalhq.fingerprint",
		},
	}

	denormalizer := NewLabelDenormalizer(segmentMaps)

	// Test with segment-specific mapping
	result := denormalizer.Denormalize(123, "resource_service_name")
	assert.Equal(t, "resource.service.name", result)

	result = denormalizer.Denormalize(123, "_cardinalhq_fingerprint")
	assert.Equal(t, "_cardinalhq.fingerprint", result)
}

func TestLabelDenormalizer_FallbackCommonLabels(t *testing.T) {
	// Empty segment maps - should use fallback
	denormalizer := NewLabelDenormalizer(nil)

	// Test common labels from fallback
	result := denormalizer.Denormalize(999, "_cardinalhq_timestamp")
	assert.Equal(t, "_cardinalhq.timestamp", result)

	result = denormalizer.Denormalize(999, "resource_service_name")
	assert.Equal(t, "resource.service.name", result)

	result = denormalizer.Denormalize(999, "resource_k8s_pod_name")
	assert.Equal(t, "resource.k8s.pod.name", result)
}

func TestLabelDenormalizer_HeuristicConversion(t *testing.T) {
	denormalizer := NewLabelDenormalizer(nil)

	// Known prefixes should be converted
	result := denormalizer.Denormalize(999, "resource_unknown_label")
	assert.Equal(t, "resource.unknown.label", result)

	result = denormalizer.Denormalize(999, "_cardinalhq_custom_field")
	assert.Equal(t, "_cardinalhq.custom.field", result)

	result = denormalizer.Denormalize(999, "log_custom_attribute")
	assert.Equal(t, "log.custom.attribute", result)

	// Unknown prefix should be kept as-is
	result = denormalizer.Denormalize(999, "unknown_prefix_label")
	assert.Equal(t, "unknown_prefix_label", result)
}

func TestLabelDenormalizer_DenormalizeMap(t *testing.T) {
	segmentMaps := map[int64]map[string]string{
		123: {
			"resource_service_name": "resource.service.name",
			"log_level":             "log.level",
		},
	}

	denormalizer := NewLabelDenormalizer(segmentMaps)

	tags := map[string]any{
		"resource_service_name":   "my-service",
		"log_level":               "error",
		"_cardinalhq_fingerprint": "12345",
		"unknown_label":           "value",
	}

	result := denormalizer.DenormalizeMap(123, tags)

	expected := map[string]any{
		"resource.service.name":   "my-service",
		"log.level":               "error",
		"_cardinalhq.fingerprint": "12345", // From fallback
		"unknown_label":           "value", // Kept as-is (unknown prefix)
	}

	assert.Equal(t, expected, result)
}

func TestLabelDenormalizer_AttrTranslation(t *testing.T) {
	// Test that attr_ prefixed fields are translated correctly for legacy API
	denormalizer := NewLabelDenormalizer(nil)

	tags := map[string]any{
		"resource_service_name": "my-service",
		"attr_log_level":        "error",
		"attr_log_source":       "test-component",
		"log_message":           "Test message",
		"attr_custom_field":     "value",
	}

	result := denormalizer.DenormalizeMap(0, tags)

	expected := map[string]any{
		"resource.service.name": "my-service",
		"log.level":             "error",          // attr_ stripped
		"log.source":            "test-component", // attr_ stripped
		"log.message":           "Test message",
		"custom_field":          "value", // attr_ stripped, no known prefix
	}

	assert.Equal(t, expected, result)
}

func TestHeuristicDenormalize(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"resource_service_name", "resource.service.name"},
		{"_cardinalhq_timestamp", "_cardinalhq.timestamp"},
		{"log_body", "log.body"},
		{"metric_value", "metric.value"},
		{"span_name", "span.name"},
		{"trace_id", "trace.id"},
		{"unknown_field", "unknown_field"}, // No known prefix
		{"no_prefix", "no_prefix"},         // No known prefix
		// attr_ prefix should be stripped for legacy API compatibility
		{"attr_log_level", "log.level"},
		{"attr_log_source", "log.source"},
		{"attr_custom_field", "custom_field"}, // attr_ stripped, no known prefix after so kept as-is
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := heuristicDenormalize(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
