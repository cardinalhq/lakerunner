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

package fingerprint

import (
	"fmt"
	"slices"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// toRow converts a map[string]any to pipeline.Row for testing
func toRow(m map[string]any) pipeline.Row {
	row := make(pipeline.Row, len(m))
	for k, v := range m {
		row[wkk.NewRowKey(k)] = v
	}
	return row
}

func TestComputeHash(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect int64
	}{
		{
			name:   "empty string",
			input:  "",
			expect: 0,
		},
		{
			name:   "single ascii",
			input:  "x",
			expect: 120,
		},
		{
			name:   "two ascii",
			input:  "hi",
			expect: 3329,
		},
		{
			name:   "three ascii",
			input:  "cat",
			expect: 98262,
		},
		{
			name:   "four ascii",
			input:  "test",
			expect: 3556498,
		},
		{
			name:   "five ascii",
			input:  "tests",
			expect: 110251553,
		},
		{
			name:   "eight ascii",
			input:  "abcdefgh",
			expect: 2758628677764,
		},
		{
			name:   "unicode ascii",
			input:  "a😀b",
			expect: 3003559594,
		},
		{
			name:   "long string",
			input:  "the quick brown fox jumps over the lazy dog",
			expect: 9189841723308291443,
		},
		{
			name:   "ending with sisteen As",
			input:  "aaaaaaaaaaaaaaaa",
			expect: 4664096450436235520,
		},
		{
			name:   "ending with 26 As",
			input:  "aaaaaaaaaaaaaaaaaaaaaaaaaa",
			expect: -3949595362870219360,
		},
		{
			name:   "chq_telemetry_type:log",
			input:  "chq_telemetry_type:log",
			expect: -7512638186058326211,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ComputeHash(tt.input)
			assert.Equal(t, tt.expect, got, "ComputeHash(%q) = %d; want %d", tt.input, got, tt.expect)
		})
	}
}

func TestToTrigrams(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: []string{},
		},
		{
			name:     "short string less than 3",
			input:    "ab",
			expected: []string{},
		},
		{
			name:  "exactly 3 chars",
			input: "abc",
			expected: []string{
				"abc",
			},
		},
		{
			name:  "4 chars",
			input: "abcd",
			expected: []string{
				"abc",
				"bcd",
			},
		},
		{
			name:  "5 chars",
			input: "abcde",
			expected: []string{
				"abc",
				"bcd",
				"cde",
			},
		},
		{
			name:  "unicode string",
			input: "a😀b",
			expected: []string{
				"a😀b",
			},
		},
		{
			name:  "repeated chars",
			input: "aaaaa",
			expected: []string{
				"aaa",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toTrigrams(tt.input)
			assert.ElementsMatch(t, tt.expected, got)
		})
	}
}

func TestToFingerprints(t *testing.T) {
	tests := []struct {
		name            string
		input           map[string]mapset.Set[string]
		expectedFingerp mapset.Set[int64]
	}{
		{
			name:            "empty input",
			input:           map[string]mapset.Set[string]{},
			expectedFingerp: mapset.NewSet[int64](),
		},
		{
			name: "single indexed dimension, single value",
			input: map[string]mapset.Set[string]{
				"metric_name": mapset.NewSet("foo"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("metric_name", "foo"),
				ComputeFingerprint("metric_name", ExistsRegex),
			),
		},
		{
			name: "single indexed dimension, multiple values",
			input: map[string]mapset.Set[string]{
				"metric_name": mapset.NewSet("foo", "bar"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("metric_name", "foo"),
				ComputeFingerprint("metric_name", "bar"),
				ComputeFingerprint("metric_name", ExistsRegex),
			),
		},
		{
			name: "single unindexed dimension",
			input: map[string]mapset.Set[string]{
				"custom": mapset.NewSet("abc"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("custom", ExistsRegex),
			),
		},
		{
			name: "indexed and unindexed dimensions",
			input: map[string]mapset.Set[string]{
				"metric_name": mapset.NewSet("foo"),
				"custom":      mapset.NewSet("abc"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("metric_name", "foo"),
				ComputeFingerprint("metric_name", ExistsRegex),
				ComputeFingerprint("custom", ExistsRegex),
			),
		},
		{
			name: "multiple values, mixed dimensions",
			input: map[string]mapset.Set[string]{
				"metric_name":   mapset.NewSet("foo", "bar"),
				"resource_file": mapset.NewSet("file1", "file2"),
				"custom":        mapset.NewSet("baz"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("metric_name", "foo"),
				ComputeFingerprint("metric_name", "bar"),
				ComputeFingerprint("metric_name", ExistsRegex),
				ComputeFingerprint("resource_file", "file1"),
				ComputeFingerprint("resource_file", "file2"),
				ComputeFingerprint("resource_file", ExistsRegex),
				ComputeFingerprint("custom", ExistsRegex),
			),
		},
		{
			name: "verify query-api generated log check",
			input: map[string]mapset.Set[string]{
				"chq_telemetry_type": mapset.NewSet("logs"),
			},
			expectedFingerp: mapset.NewSet(
				ComputeFingerprint("chq_telemetry_type", ExistsRegex),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToFingerprints(tt.input)
			// Check that all expected fingerprints are present (got may have additional ones)
			assert.True(t, tt.expectedFingerp.IsSubset(got), "expected %v to be subset of %v", tt.expectedFingerp.ToSlice(), got.ToSlice())
		})
	}
}

func TestGenerateRowFingerprints(t *testing.T) {
	// Example log row with CardinalHQ fields
	row := map[string]any{
		"log_message":                 "User login failed",
		"metric_name":                 "log_events",
		"log_level":                   "error",
		"chq_timestamp":               int64(1640995200000),
		"resource_service.name":       "auth-service",
		"resource_k8s.namespace.name": "production",
		"custom_field":                "some_value",
	}

	// Create fingerprinter
	fp := NewFieldFingerprinter("")
	fingerprints := fp.GenerateFingerprints(toRow(row))

	// Should generate multiple fingerprints for different dimensions
	assert.Greater(t, len(fingerprints), 5, "Should generate multiple fingerprints for different fields")

	// Test that it includes fingerprints for indexed dimensions
	expectedFingerprints := mapset.NewSet[int64]()

	// metric_name is IndexExact - gets exact value fingerprint
	expectedFingerprints.Add(ComputeFingerprint("metric_name", "log_events"))
	expectedFingerprints.Add(ComputeFingerprint("metric_name", ExistsRegex))

	// log_level is IndexExact - gets exact value fingerprint (no trigrams)
	expectedFingerprints.Add(ComputeFingerprint("log_level", "error"))
	expectedFingerprints.Add(ComputeFingerprint("log_level", ExistsRegex))

	// custom_field should get exists regex fingerprint only (not in IndexedDimensions)
	expectedFingerprints.Add(ComputeFingerprint("custom_field", ExistsRegex))

	// Verify some expected fingerprints are present
	for _, expectedFp := range expectedFingerprints.ToSlice() {
		assert.True(t, slices.Contains(fingerprints, expectedFp), "Should contain fingerprint %d", expectedFp)
	}
}

// TestGenerateRowFingerprints_Comprehensive is a comprehensive regression test that validates
// the EXACT set of fingerprints generated for a realistic log row with many fields.
// This test ensures that optimizations to fingerprint generation don't change the output.
func TestGenerateRowFingerprints_Comprehensive(t *testing.T) {
	// Realistic log row with ~50 fields (typical production scenario)
	row := map[string]any{
		// CardinalHQ standard fields
		"chq_timestamp":      int64(1640995200000),
		"chq_telemetry_type": "logs",
		"chq_name":           "log_events",
		"chq_value":          1.0,
		"chq_message":        "User authentication failed for user@example.com",
		"chq_fingerprint":    int64(12345),

		// Indexed dimensions (should get full processing)
		"log_level":                   "error",
		"metric_name":                 "log_events",
		"resource_customer_domain":    "example.com",
		"resource_file":               "auth.log",
		"resource_k8s_namespace_name": "production",
		"resource_service_name":       "auth-service",
		"span_trace_id":               "abc123def456",

		// Resource attributes (NOT in DimensionsToIndex - should only get exists fingerprints)
		"resource_host_name":       "server01.example.com",
		"resource_cloud_provider":  "aws",
		"resource_cloud_region":    "us-west-2",
		"resource_container_name":  "auth-container",
		"resource_deployment_name": "auth-deployment",

		// Scope attributes (NOT in DimensionsToIndex)
		"scope_name":    "io.opentelemetry.logs",
		"scope_version": "1.0.0",

		// Log attributes (NOT in DimensionsToIndex - majority of fields)
		"attr_user_id":       "user123",
		"attr_ip_address":    "192.168.1.1",
		"attr_request_id":    "req-456-789",
		"attr_session_id":    "sess-abc-def",
		"attr_method":        "POST",
		"attr_url":           "/api/login",
		"attr_status_code":   401,
		"attr_response_time": 150.5,
		"attr_user_agent":    "Mozilla/5.0",
		"attr_referer":       "https://example.com",
		"attr_environment":   "production",
		"attr_version":       "v1.2.3",
		"attr_build_number":  "12345",
		"attr_commit_sha":    "abcdef123456",

		// Additional custom fields (NOT indexed)
		"custom_field_1":  "value1",
		"custom_field_2":  "value2",
		"custom_field_3":  42,
		"custom_field_4":  true,
		"custom_field_5":  nil, // Should be skipped
		"custom_field_6":  3.14,
		"custom_field_7":  "a very long string value that should still work fine",
		"custom_field_8":  []byte("binary data"),
		"custom_field_9":  "emoji 😀 test",
		"custom_field_10": "unicode test ñ",
	}

	// Generate fingerprints with current implementation
	fp := NewFieldFingerprinter("")
	fingerprints := fp.GenerateFingerprints(toRow(row))

	// Manually compute expected fingerprints based on the algorithm
	expected := mapset.NewSet[int64]()

	// ALL fields get an exists fingerprint
	for fieldName := range row {
		if row[fieldName] != nil { // nil values are skipped
			expected.Add(ComputeFingerprint(fieldName, ExistsRegex))
		}
	}

	// Only IndexedDimensions fields get value-based fingerprints
	// chq_telemetry_type - IndexTrigramExact (both exact and trigrams)
	expected.Add(ComputeFingerprint("chq_telemetry_type", "logs"))
	for _, trigram := range toTrigrams("logs") {
		expected.Add(ComputeFingerprint("chq_telemetry_type", trigram))
	}

	// log_level - IndexExact (exact value only)
	expected.Add(ComputeFingerprint("log_level", "error"))

	// metric_name - IndexExact (exact value only)
	expected.Add(ComputeFingerprint("metric_name", "log_events"))

	// resource_file - IndexExact (exact value only)
	expected.Add(ComputeFingerprint("resource_file", "auth.log"))

	// resource_customer_domain - IndexTrigramExact (both exact and trigrams)
	expected.Add(ComputeFingerprint("resource_customer_domain", "example.com"))
	for _, trigram := range toTrigrams("example.com") {
		expected.Add(ComputeFingerprint("resource_customer_domain", trigram))
	}

	// resource_k8s_namespace_name - IndexTrigramExact (both exact and trigrams)
	expected.Add(ComputeFingerprint("resource_k8s_namespace_name", "production"))
	for _, trigram := range toTrigrams("production") {
		expected.Add(ComputeFingerprint("resource_k8s_namespace_name", trigram))
	}

	// resource_service_name - IndexTrigramExact (both exact and trigrams)
	expected.Add(ComputeFingerprint("resource_service_name", "auth-service"))
	for _, trigram := range toTrigrams("auth-service") {
		expected.Add(ComputeFingerprint("resource_service_name", trigram))
	}

	// span_trace_id - IndexTrigramExact (both exact and trigrams)
	expected.Add(ComputeFingerprint("span_trace_id", "abc123def456"))
	for _, trigram := range toTrigrams("abc123def456") {
		expected.Add(ComputeFingerprint("span_trace_id", trigram))
	}

	// Convert fingerprints slice to set for comparison
	fingerprintsSet := mapset.NewSet(fingerprints...)

	// Verify EXACT match - same count and all fingerprints present
	assert.Equal(t, expected.Cardinality(), len(fingerprints),
		"Fingerprint count mismatch - expected %d, got %d",
		expected.Cardinality(), len(fingerprints))

	assert.True(t, expected.Equal(fingerprintsSet),
		"Fingerprint sets don't match.\nExpected: %v\nGot: %v\nMissing: %v\nExtra: %v",
		expected.ToSlice(),
		fingerprints,
		expected.Difference(fingerprintsSet).ToSlice(),
		fingerprintsSet.Difference(expected).ToSlice())
}

// TestGenerateRowFingerprintsWithCache_IdenticalOutput validates that the cached version
// produces exactly the same fingerprints as the non-cached version
func TestGenerateRowFingerprintsWithCache_IdenticalOutput(t *testing.T) {
	// Test with multiple realistic rows to ensure cache works correctly across rows
	rows := []map[string]any{
		{
			"chq_timestamp":      int64(1640995200000),
			"chq_telemetry_type": "logs",
			"log_level":          "error",
			"metric_name":        "log_events",
			"resource_file":      "auth.log",
			"custom_field_1":     "value1",
			"custom_field_2":     42,
		},
		{
			"chq_timestamp":      int64(1640995201000),
			"chq_telemetry_type": "logs",
			"log_level":          "warn",
			"metric_name":        "log_events",
			"resource_file":      "auth.log",
			"custom_field_1":     "value2",
			"custom_field_3":     "new_field",
		},
		{
			"chq_timestamp":            int64(1640995202000),
			"log_level":                "info",
			"resource_service_name":    "api-service",
			"resource_customer_domain": "example.com",
			"span_trace_id":            "trace123",
			"custom_field_4":           true,
		},
	}

	// Create fingerprinter (simulating accumulator behavior with reused instance)
	fp := NewFieldFingerprinter("")

	for i, row := range rows {
		// Generate with fresh fingerprinter for each row (simulates non-cached)
		freshFp := NewFieldFingerprinter("")
		expectedFps := freshFp.GenerateFingerprints(toRow(row))

		// Generate with shared fingerprinter (simulates cached across rows)
		cachedFps := fp.GenerateFingerprints(toRow(row))

		// Convert to sets for comparison
		expectedSet := mapset.NewSet(expectedFps...)
		cachedSet := mapset.NewSet(cachedFps...)

		// They should be identical
		assert.Equal(t, len(expectedFps), len(cachedFps),
			"Row %d: fingerprint count mismatch", i)

		assert.True(t, expectedSet.Equal(cachedSet),
			"Row %d: fingerprint sets don't match.\nExpected: %v\nGot: %v\nMissing: %v\nExtra: %v",
			i,
			expectedFps,
			cachedFps,
			expectedSet.Difference(cachedSet).ToSlice(),
			cachedSet.Difference(expectedSet).ToSlice())
	}

	// Verify cache was populated
	assert.Greater(t, len(fp.existsFpCache), 0, "Exists cache should be populated")
}

// TestFieldFingerprinter_CacheEffectiveness validates that the cache
// actually reduces computation
func TestFieldFingerprinter_CacheEffectiveness(t *testing.T) {
	// Create fingerprinter
	fp := NewFieldFingerprinter("")

	// First row - should populate cache
	row1 := map[string]any{
		"chq_timestamp": int64(1640995200000),
		"log_level":     "error",
		"metric_name":   "log_events",
		"custom_field":  "value1",
	}

	_ = fp.GenerateFingerprints(toRow(row1))

	// Verify cache was populated
	assert.Equal(t, 4, len(fp.existsFpCache), "Should have 4 field names cached")

	// Second row with same fields - should use cache
	row2 := map[string]any{
		"chq_timestamp": int64(1640995201000),
		"log_level":     "warn",
		"metric_name":   "log_events",
		"custom_field":  "value2",
	}

	_ = fp.GenerateFingerprints(toRow(row2))

	// Cache size should not grow (same fields)
	assert.Equal(t, 4, len(fp.existsFpCache), "Cache size should not grow with same fields")

	// Third row with NEW field - should add to cache
	row3 := map[string]any{
		"chq_timestamp": int64(1640995202000),
		"log_level":     "info",
		"new_field":     "new_value",
	}

	_ = fp.GenerateFingerprints(toRow(row3))

	// Cache should grow by 1
	assert.Equal(t, 5, len(fp.existsFpCache), "Cache should grow with new field")
}

// TestFieldFingerprinter_FullValueCache validates that exact-value fingerprints are cached
func TestFieldFingerprinter_FullValueCache(t *testing.T) {
	fp := NewFieldFingerprinter("")

	// All indexed fields now have exact fingerprints cached
	row1 := map[string]any{
		"metric_name":   "http_requests_total", // IndexExact
		"resource_file": "/var/log/app.log",    // IndexExact
		"log_level":     "info",                // IndexTrigramExact (also caches exact)
	}

	_ = fp.GenerateFingerprints(toRow(row1))

	// Should have cached 3 exact-value fingerprints (all indexed fields)
	assert.Equal(t, 3, len(fp.fullValueFpCache), "Should cache exact-value fingerprints")

	// Same values again - should hit cache
	row2 := map[string]any{
		"metric_name":   "http_requests_total",
		"resource_file": "/var/log/app.log",
	}

	_ = fp.GenerateFingerprints(toRow(row2))

	// Cache size should not grow
	assert.Equal(t, 3, len(fp.fullValueFpCache), "Cache should not grow for same values")

	// Different values - should add to cache
	row3 := map[string]any{
		"metric_name":   "http_errors_total",
		"resource_file": "/var/log/error.log",
	}

	_ = fp.GenerateFingerprints(toRow(row3))

	// Cache should grow by 2
	assert.Equal(t, 5, len(fp.fullValueFpCache), "Cache should grow with new values")
}

// TestFieldFingerprinter_FullValueCacheBounded validates that cache doesn't grow beyond limit
func TestFieldFingerprinter_FullValueCacheBounded(t *testing.T) {
	fp := NewFieldFingerprinter("")

	// Fill cache to just under limit
	for i := range maxFullValueCacheSize - 1 {
		row := map[string]any{
			"metric_name": fmt.Sprintf("metric_%d", i),
		}
		_ = fp.GenerateFingerprints(toRow(row))
	}

	assert.Equal(t, maxFullValueCacheSize-1, len(fp.fullValueFpCache), "Should have filled cache to limit-1")

	// Add one more to reach limit
	row := map[string]any{
		"metric_name": fmt.Sprintf("metric_%d", maxFullValueCacheSize-1),
	}
	_ = fp.GenerateFingerprints(toRow(row))

	assert.Equal(t, maxFullValueCacheSize, len(fp.fullValueFpCache), "Should have reached cache limit")

	// Try to add beyond limit - should not grow
	row = map[string]any{
		"metric_name": "overflow_metric",
	}
	_ = fp.GenerateFingerprints(toRow(row))

	assert.Equal(t, maxFullValueCacheSize, len(fp.fullValueFpCache), "Cache should not grow beyond limit")
}

func TestIsIndexed(t *testing.T) {
	tests := []struct {
		field    string
		expected bool
	}{
		{"metric_name", true},
		{"log_level", true},
		{"resource_service_name", true},
		{"resource_k8s_namespace_name", true},
		{"span_trace_id", true},
		{"chq_telemetry_type", true},
		{"resource_file", true},
		{"resource_customer_domain", true},
		{"custom_field", false},
		{"not_indexed", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsIndexed(tt.field))
		})
	}
}

func TestHasExactIndex(t *testing.T) {
	tests := []struct {
		field    string
		expected bool
	}{
		// IndexExact fields
		{"log_level", true},
		{"metric_name", true},
		{"resource_file", true},
		// IndexTrigramExact fields (also have exact)
		{"resource_service_name", true},
		{"resource_k8s_namespace_name", true},
		{"span_trace_id", true},
		{"chq_telemetry_type", true},
		{"resource_customer_domain", true},
		{"custom_field", false},
		{"not_indexed", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			assert.Equal(t, tt.expected, HasExactIndex(tt.field))
		})
	}
}

func TestHasTrigramIndex(t *testing.T) {
	tests := []struct {
		field    string
		expected bool
	}{
		// IndexTrigramExact fields
		{"resource_k8s_namespace_name", true},
		{"resource_k8s_cluster_name", true},
		{"resource_service_name", true},
		{"span_trace_id", true},
		{"chq_telemetry_type", true},
		// IndexExact only fields (no trigrams)
		{"metric_name", false},
		{"resource_file", false},
		{"log_level", false},
		{"resource_customer_domain", true},
		{"custom_field", false},
		{"not_indexed", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.field, func(t *testing.T) {
			assert.Equal(t, tt.expected, HasTrigramIndex(tt.field))
		})
	}
}
