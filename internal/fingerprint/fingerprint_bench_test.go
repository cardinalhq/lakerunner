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
	"testing"

	mapset "github.com/deckarep/golang-set/v2"
)

// BenchmarkComputeHash benchmarks the core hash function
func BenchmarkComputeHash(b *testing.B) {
	testCases := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"short", "abc"},
		{"medium", "hello world"},
		{"long", "resource_service_name:test-service-with-a-really-long-name-here"},
		{"very_long", "this is a very long string that has many characters and will exercise the 4-byte processing loop multiple times over and over again"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = ComputeHash(tc.input)
			}
		})
	}
}

// BenchmarkComputeFingerprint benchmarks fingerprint computation
func BenchmarkComputeFingerprint(b *testing.B) {
	testCases := []struct {
		name      string
		fieldName string
		value     string
	}{
		{"exists", "resource_service_name", ".*"},
		{"short_value", "log_level", "INFO"},
		{"medium_value", "resource_service_name", "api-gateway"},
		{"long_value", "span_trace_id", "4572d7d2106a05d35bb0da1d1cf590ae"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = ComputeFingerprint(tc.fieldName, tc.value)
			}
		})
	}
}

// BenchmarkToTrigrams benchmarks trigram generation
func BenchmarkToTrigrams(b *testing.B) {
	testCases := []struct {
		name  string
		input string
	}{
		{"short", "abc"},
		{"medium", "test-service"},
		{"long", "my-very-long-service-name-with-many-characters"},
		{"uuid", "4572d7d2106a05d35bb0da1d1cf590ae"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = toTrigrams(tc.input)
			}
		})
	}
}

// BenchmarkToFingerprints benchmarks the full fingerprint generation
func BenchmarkToFingerprints(b *testing.B) {
	// Simulate typical trace data
	tagValuesByName := map[string]mapset.Set[string]{
		"resource_service_name":       mapset.NewSet("api-gateway", "frontend", "backend-service"),
		"resource_k8s_namespace_name": mapset.NewSet("production", "staging"),
		"resource_k8s_cluster_name":   mapset.NewSet("us-west-2-prod"),
		"chq_telemetry_type":          mapset.NewSet("traces"),
		"span_trace_id":               mapset.NewSet("4572d7d2106a05d35bb0da1d1cf590ae"),
		"span_name":                   mapset.NewSet("GET /api/users"),
		"span_kind":                   mapset.NewSet("SERVER"),
		"http_method":                 mapset.NewSet("GET"),
		"http_status_code":            mapset.NewSet("200"),
		"http_url":                    mapset.NewSet("https://api.example.com/users"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ToFingerprints(tagValuesByName)
	}
}

// BenchmarkToFingerprintsLarge benchmarks with more fields (realistic span)
func BenchmarkToFingerprintsLarge(b *testing.B) {
	tagValuesByName := map[string]mapset.Set[string]{
		"resource_service_name":       mapset.NewSet("api-gateway"),
		"resource_k8s_namespace_name": mapset.NewSet("production"),
		"resource_k8s_cluster_name":   mapset.NewSet("us-west-2-prod"),
		"resource_customer_domain":    mapset.NewSet("acme-corp"),
		"chq_telemetry_type":          mapset.NewSet("traces"),
		"span_trace_id":               mapset.NewSet("4572d7d2106a05d35bb0da1d1cf590ae"),
		"span_name":                   mapset.NewSet("GET /api/users/{id}"),
		"span_kind":                   mapset.NewSet("SERVER"),
		"span_status_code":            mapset.NewSet("OK"),
		"http_method":                 mapset.NewSet("GET"),
		"http_status_code":            mapset.NewSet("200"),
		"http_url":                    mapset.NewSet("https://api.example.com/users/12345"),
		"http_host":                   mapset.NewSet("api.example.com"),
		"http_scheme":                 mapset.NewSet("https"),
		"http_target":                 mapset.NewSet("/users/12345"),
		"http_user_agent":             mapset.NewSet("Mozilla/5.0 (compatible)"),
		"net_peer_name":               mapset.NewSet("10.0.0.50"),
		"net_peer_port":               mapset.NewSet("8080"),
		"db_system":                   mapset.NewSet("postgresql"),
		"db_name":                     mapset.NewSet("users_db"),
		"db_statement":                mapset.NewSet("SELECT * FROM users WHERE id = $1"),
		"db_operation":                mapset.NewSet("SELECT"),
		"messaging_system":            mapset.NewSet("kafka"),
		"messaging_destination":       mapset.NewSet("user-events"),
		"rpc_system":                  mapset.NewSet("grpc"),
		"rpc_service":                 mapset.NewSet("UserService"),
		"rpc_method":                  mapset.NewSet("GetUser"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ToFingerprints(tagValuesByName)
	}
}
