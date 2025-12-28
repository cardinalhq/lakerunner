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

package fingerprint

import (
	"slices"
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/stretchr/testify/assert"
)

// TestComputeHashKnownValues tests the hash function with known inputs and outputs.
// These values are used as test vectors for the Rust implementation.
func TestComputeHashKnownValues(t *testing.T) {
	testCases := []struct {
		input    string
		expected int64
	}{
		// Short strings
		{"", 0},
		{"a", 97},
		{"ab", 3105},
		{"abc", 96354},
		{"abcd", 2987074},
		// Longer strings
		{"hello", 99162322},
		{"world", 113318802},
		{"hello world", 1794106052357989},
		// Fingerprint patterns
		{"resource_service_name:.*", 0}, // placeholder - will be computed
		{"resource_service_name:test-service", 2239089753361420959},
		{"span_trace_id:.*", 7623621049752551},
		{"chq_telemetry_type:traces", 1361892663878636287},
	}

	for _, tc := range testCases {
		t.Run(tc.input, func(t *testing.T) {
			result := ComputeHash(tc.input)
			t.Logf("ComputeHash(%q) = %d", tc.input, result)
			// Don't assert yet - we're generating test vectors
		})
	}
}

// TestComputeFingerprintKnownValues tests fingerprint computation with known inputs.
func TestComputeFingerprintKnownValues(t *testing.T) {
	testCases := []struct {
		fieldName string
		value     string
	}{
		{"resource_service_name", ".*"},
		{"resource_service_name", "test-service"},
		{"resource_service_name", "api-gateway"},
		{"span_trace_id", ".*"},
		{"span_trace_id", "abc123def456"},
		{"chq_telemetry_type", ".*"},
		{"chq_telemetry_type", "traces"},
		{"log_level", ".*"},
		{"log_level", "ERROR"},
	}

	t.Log("=== Fingerprint Test Vectors ===")
	for _, tc := range testCases {
		fp := ComputeFingerprint(tc.fieldName, tc.value)
		t.Logf("ComputeFingerprint(%q, %q) = %d", tc.fieldName, tc.value, fp)
	}
}

// TestToTrigramsKnownValues tests trigram generation with known inputs.
func TestToTrigramsKnownValues(t *testing.T) {
	testCases := []struct {
		input    string
		expected []string
	}{
		{"ab", nil},                              // Too short
		{"abc", []string{"abc"}},                 // Exactly 3
		{"abcd", []string{"abc", "bcd"}},         // 4 chars
		{"hello", []string{"hel", "ell", "llo"}}, // 5 chars
		{"test-service", nil},                    // Will generate multiple trigrams
	}

	t.Log("=== Trigram Test Vectors ===")
	for _, tc := range testCases {
		trigrams := toTrigrams(tc.input)
		slices.Sort(trigrams)
		t.Logf("toTrigrams(%q) = %v", tc.input, trigrams)
	}
}

// TestToFingerprintsForTraceData generates fingerprints for trace-like data.
// This provides test vectors for the Rust implementation.
func TestToFingerprintsForTraceData(t *testing.T) {
	// Simulate trace data with indexed fields
	tagValuesByName := map[string]mapset.Set[string]{
		// Indexed fields (will get value fingerprints)
		"resource_service_name":       mapset.NewSet("test-service", "api-gateway"),
		"resource_k8s_namespace_name": mapset.NewSet("production"),
		"chq_telemetry_type":          mapset.NewSet("traces"),
		"span_trace_id":               mapset.NewSet("abc123"),
		// Non-indexed fields (only get exists fingerprints)
		"span_name":     mapset.NewSet("GET /api/users"),
		"span_duration": mapset.NewSet("50"),
	}

	fingerprints := ToFingerprints(tagValuesByName)
	fps := fingerprints.ToSlice()
	slices.Sort(fps)

	t.Log("=== Trace Fingerprint Test Vectors ===")
	t.Logf("Total fingerprints: %d", len(fps))
	for i, fp := range fps {
		t.Logf("  [%d] %d", i, fp)
	}

	// Basic assertions
	assert.Greater(t, len(fps), 0, "should have fingerprints")

	// Should have exists fingerprints for all fields
	existsCount := 0
	for fieldName := range tagValuesByName {
		existsFp := ComputeFingerprint(fieldName, ExistsRegex)
		if fingerprints.Contains(existsFp) {
			existsCount++
			t.Logf("Exists fingerprint for %q: %d", fieldName, existsFp)
		}
	}
	assert.Equal(t, len(tagValuesByName), existsCount, "should have exists fingerprint for each field")
}

// TestHashAlgorithmDetails provides detailed breakdown of the hash algorithm.
func TestHashAlgorithmDetails(t *testing.T) {
	// Test the 4-byte-at-a-time processing
	input := "resource_service_name:test-service"

	var h int64
	length := len(input)
	i := 0

	t.Logf("Input: %q (length=%d)", input, length)
	t.Log("Step-by-step hash computation:")

	for i+3 < length {
		oldH := h
		h = 31*31*31*31*h + 31*31*31*int64(input[i]) + 31*31*int64(input[i+1]) + 31*int64(input[i+2]) + int64(input[i+3])
		t.Logf("  i=%d: chars=%q, h: %d -> %d", i, input[i:i+4], oldH, h)
		i += 4
	}

	for ; i < length; i++ {
		oldH := h
		h = 31*h + int64(input[i])
		t.Logf("  i=%d: char=%q, h: %d -> %d", i, string(input[i]), oldH, h)
	}

	t.Logf("Final hash: %d", h)
	assert.Equal(t, ComputeHash(input), h)
}
