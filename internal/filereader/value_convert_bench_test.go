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

package filereader

import (
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

// Benchmark Value.AsString() vs optimized conversion
func BenchmarkValueConversion(b *testing.B) {
	// Create test values of different types
	strVal := pcommon.NewValueStr("service-name")
	intVal := pcommon.NewValueInt(12345)
	boolVal := pcommon.NewValueBool(true)
	floatVal := pcommon.NewValueDouble(3.14159)

	b.Run("string_AsString", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = strVal.AsString()
		}
	})

	b.Run("string_Str_direct", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = strVal.Str()
		}
	})

	b.Run("int_AsString", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = intVal.AsString()
		}
	})

	b.Run("bool_AsString", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = boolVal.AsString()
		}
	})

	b.Run("float_AsString", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = floatVal.AsString()
		}
	})
}

// Benchmark realistic attribute conversion patterns
func BenchmarkRealisticAttributeConversion(b *testing.B) {
	// Typical OTEL attributes from production (90% strings, 10% other types)
	attributes := []pcommon.Value{
		pcommon.NewValueStr("web-service"),         // service.name
		pcommon.NewValueStr("1.2.3"),               // service.version
		pcommon.NewValueStr("prod-host-01"),        // host.name
		pcommon.NewValueStr("web-pod-abc123"),      // k8s.pod.name
		pcommon.NewValueStr("production"),          // k8s.namespace.name
		pcommon.NewValueStr("aws"),                 // cloud.provider
		pcommon.NewValueStr("us-west-2"),           // cloud.region
		pcommon.NewValueStr("i-1234567890abcdef0"), // host.id
		pcommon.NewValueInt(8080),                  // port (int)
		pcommon.NewValueBool(true),                 // is_production (bool)
	}

	b.Run("baseline_AsString", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, v := range attributes {
				_ = v.AsString()
			}
		}
		b.ReportMetric(float64(b.N*len(attributes))/b.Elapsed().Seconds(), "attrs/sec")
	})

	b.Run("optimized_fast_path", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, v := range attributes {
				var result string
				if v.Type() == pcommon.ValueTypeStr {
					result = v.Str()
				} else {
					result = v.AsString()
				}
				_ = result
			}
		}
		b.ReportMetric(float64(b.N*len(attributes))/b.Elapsed().Seconds(), "attrs/sec")
	})
}

// Test correctness of optimized conversion
func TestValueConversionCorrectness(t *testing.T) {
	testCases := []struct {
		name     string
		value    pcommon.Value
		expected string
	}{
		{"string", pcommon.NewValueStr("hello"), "hello"},
		{"empty_string", pcommon.NewValueStr(""), ""},
		{"int", pcommon.NewValueInt(12345), "12345"},
		{"negative_int", pcommon.NewValueInt(-999), "-999"},
		{"bool_true", pcommon.NewValueBool(true), "true"},
		{"bool_false", pcommon.NewValueBool(false), "false"},
		{"float", pcommon.NewValueDouble(3.14), "3.14"},
		{"zero_int", pcommon.NewValueInt(0), "0"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Baseline: AsString()
			baseline := tc.value.AsString()

			// Optimized: fast path
			var optimized string
			if tc.value.Type() == pcommon.ValueTypeStr {
				optimized = tc.value.Str()
			} else {
				optimized = tc.value.AsString()
			}

			if baseline != tc.expected {
				t.Errorf("AsString() = %q, want %q", baseline, tc.expected)
			}

			if optimized != tc.expected {
				t.Errorf("Optimized = %q, want %q", optimized, tc.expected)
			}

			if baseline != optimized {
				t.Errorf("Results differ: AsString()=%q, Optimized=%q", baseline, optimized)
			}
		})
	}
}

// Test that string type detection works correctly
func TestValueTypeDetection(t *testing.T) {
	strVal := pcommon.NewValueStr("test")
	intVal := pcommon.NewValueInt(123)
	boolVal := pcommon.NewValueBool(true)

	if strVal.Type() != pcommon.ValueTypeStr {
		t.Errorf("String value has type %v, want %v", strVal.Type(), pcommon.ValueTypeStr)
	}

	if intVal.Type() == pcommon.ValueTypeStr {
		t.Errorf("Int value incorrectly detected as string")
	}

	if boolVal.Type() == pcommon.ValueTypeStr {
		t.Errorf("Bool value incorrectly detected as string")
	}
}
