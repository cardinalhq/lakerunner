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

package wkk

import "testing"

func TestNormalizeName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Already normalized
		{"already_normalized", "service_name", "service_name"},
		{"lowercase_only", "servicename", "servicename"},
		{"with_numbers", "metric123", "metric123"},
		{"underscore_separated", "my_service_name", "my_service_name"},

		// Case conversion
		{"uppercase", "SERVICE_NAME", "service_name"},
		{"mixed_case", "ServiceName", "servicename"},
		{"camelCase", "serviceName", "servicename"},

		// Dot separator (OTEL style)
		{"dot_separator", "service.name", "service_name"},
		{"multiple_dots", "http.request.method", "http_request_method"},
		{"mixed_case_and_dot", "Service.Name", "service_name"},

		// Other special characters
		{"hyphen", "my-service", "my_service"},
		{"colon", "namespace:key", "namespace_key"},
		{"slash", "path/to/resource", "path_to_resource"},
		{"at_sign", "user@domain", "user_domain"},
		{"space", "my service", "my_service"},
		{"brackets", "array[0]", "array_0_"},
		{"parens", "func(arg)", "func_arg_"},

		// Complex real-world examples
		{"k8s_style", "k8s.pod.name-v2:latest", "k8s_pod_name_v2_latest"},
		{"otel_resource", "resource.service.name", "resource_service_name"},
		{"http_header", "http.request.header.content-type", "http_request_header_content_type"},

		// Edge cases
		{"empty", "", ""},
		{"single_char", "a", "a"},
		{"single_uppercase", "A", "a"},
		{"single_special", ".", "_"},
		{"all_special", "..--::", "______"},
		{"all_uppercase", "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"},
		{"all_digits", "0123456789", "0123456789"},
		{"leading_special", ".service", "_service"},
		{"trailing_special", "service.", "service_"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := NormalizeName(tt.input)
			if result != tt.expected {
				t.Errorf("NormalizeName(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNormalizeName_Caching(t *testing.T) {
	// Verify that repeated calls return the same result
	input := "test.caching.behavior"
	expected := "test_caching_behavior"

	result1 := NormalizeName(input)
	result2 := NormalizeName(input)

	if result1 != expected {
		t.Errorf("first call: got %q, want %q", result1, expected)
	}
	if result2 != expected {
		t.Errorf("second call: got %q, want %q", result2, expected)
	}
	if result1 != result2 {
		t.Errorf("caching inconsistency: first=%q, second=%q", result1, result2)
	}
}

func BenchmarkNormalizeName(b *testing.B) {
	benchmarks := []struct {
		name  string
		input string
	}{
		{"already_normalized", "service_name"},
		{"simple_dot", "service.name"},
		{"uppercase", "SERVICE_NAME"},
		{"complex", "k8s.pod.name-v2:latest"},
		{"long", "resource.attributes.kubernetes.pod.name.with.many.segments"},
	}

	for _, bm := range benchmarks {
		b.Run(bm.name+"_warm", func(b *testing.B) {
			// Warm cache - same key repeated (common case)
			NormalizeName(bm.input) // Prime the cache
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				NormalizeName(bm.input)
			}
		})
	}
}
