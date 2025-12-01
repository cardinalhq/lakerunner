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

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func TestPrefixedRowKeyCache_Basic(t *testing.T) {
	cache := NewPrefixedRowKeyCache("resource")

	// First call should compute and cache
	key1 := cache.Get("service.name")
	if cache.Len() != 1 {
		t.Errorf("Expected cache length 1, got %d", cache.Len())
	}

	// Second call should return cached value
	key2 := cache.Get("service.name")
	if cache.Len() != 1 {
		t.Errorf("Expected cache length still 1, got %d", cache.Len())
	}

	// Should be the exact same RowKey (pointer equality due to unique.Make)
	if key1 != key2 {
		t.Errorf("Expected cached keys to be identical")
	}

	// Verify the key has correct prefix and dot replacement
	expectedKey := wkk.NewRowKey("resource_service_name")
	if key1 != expectedKey {
		t.Errorf("Expected key %v, got %v", expectedKey, key1)
	}
}

func TestPrefixedRowKeyCache_MultiplePrefixes(t *testing.T) {
	resourceCache := NewPrefixedRowKeyCache("resource")
	scopeCache := NewPrefixedRowKeyCache("scope")
	attrCache := NewPrefixedRowKeyCache("attr")

	name := "service.name"

	resourceKey := resourceCache.Get(name)
	scopeKey := scopeCache.Get(name)
	attrKey := attrCache.Get(name)

	// All should have length 1
	if resourceCache.Len() != 1 || scopeCache.Len() != 1 || attrCache.Len() != 1 {
		t.Errorf("Expected all caches to have length 1")
	}

	// Keys should be different due to different prefixes
	if resourceKey == scopeKey || resourceKey == attrKey || scopeKey == attrKey {
		t.Errorf("Expected keys with different prefixes to be different")
	}

	// Verify correct prefixes
	expectedResource := wkk.NewRowKey("resource_service_name")
	expectedScope := wkk.NewRowKey("scope_service_name")
	expectedAttr := wkk.NewRowKey("attr_service_name")

	if resourceKey != expectedResource {
		t.Errorf("Resource key: expected %v, got %v", expectedResource, resourceKey)
	}
	if scopeKey != expectedScope {
		t.Errorf("Scope key: expected %v, got %v", expectedScope, scopeKey)
	}
	if attrKey != expectedAttr {
		t.Errorf("Attr key: expected %v, got %v", expectedAttr, attrKey)
	}
}

func TestPrefixedRowKeyCache_MultipleNames(t *testing.T) {
	cache := NewPrefixedRowKeyCache("resource")

	names := []string{
		"service.name",
		"deployment.environment",
		"host.name",
		"service.version",
	}

	// Get all keys
	keys := make([]wkk.RowKey, len(names))
	for i, name := range names {
		keys[i] = cache.Get(name)
	}

	// Cache should have all names
	if cache.Len() != len(names) {
		t.Errorf("Expected cache length %d, got %d", len(names), cache.Len())
	}

	// All keys should be unique
	seen := make(map[wkk.RowKey]bool)
	for _, key := range keys {
		if seen[key] {
			t.Errorf("Found duplicate key: %v", key)
		}
		seen[key] = true
	}

	// Verify idempotency - getting same names again should return same keys
	for i, name := range names {
		key := cache.Get(name)
		if key != keys[i] {
			t.Errorf("Key for %s changed on second Get", name)
		}
	}

	// Cache length should not have changed
	if cache.Len() != len(names) {
		t.Errorf("Cache length changed after re-getting keys")
	}
}

func TestPrefixedRowKeyCache_Clear(t *testing.T) {
	cache := NewPrefixedRowKeyCache("resource")

	// Add some entries
	key1 := cache.Get("service.name")
	cache.Get("deployment.environment")
	cache.Get("host.name")

	if cache.Len() != 3 {
		t.Errorf("Expected cache length 3, got %d", cache.Len())
	}

	// Clear the cache
	cache.Clear()

	if cache.Len() != 0 {
		t.Errorf("Expected cache length 0 after Clear, got %d", cache.Len())
	}

	// Getting the same key after clear should work but return new instance
	key2 := cache.Get("service.name")

	// Keys should have same value but may be different instances after clear
	expectedKey := wkk.NewRowKey("resource_service_name")
	if key2 != expectedKey {
		t.Errorf("Expected key %v after clear, got %v", expectedKey, key2)
	}

	// After getting one key, cache should have length 1
	if cache.Len() != 1 {
		t.Errorf("Expected cache length 1 after re-adding, got %d", cache.Len())
	}

	// Due to unique.Make interning, keys should actually be the same
	if key1 != key2 {
		t.Logf("Note: Keys differ after clear (expected due to new cache instance)")
	}
}

func TestPrefixedRowKeyCache_SpecialCases(t *testing.T) {
	cache := NewPrefixedRowKeyCache("resource")

	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "no dots",
			input:    "simple",
			expected: "resource_simple",
		},
		{
			name:     "multiple dots",
			input:    "a.b.c.d",
			expected: "resource_a_b_c_d",
		},
		{
			name:     "underscore prefix",
			input:    "_internal",
			expected: "_internal",
		},
		{
			name:     "underscore prefix with dots",
			input:    "_internal.field",
			expected: "_internal_field",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			key := cache.Get(tc.input)
			expectedKey := wkk.NewRowKey(tc.expected)
			if key != expectedKey {
				t.Errorf("Expected %v, got %v", expectedKey, key)
			}
		})
	}
}

func TestPrefixedRowKeyCache_Performance(t *testing.T) {
	cache := NewPrefixedRowKeyCache("resource")

	// Simulate typical usage: 10 unique attributes accessed 1000 times each
	attributes := []string{
		"service.name",
		"service.version",
		"deployment.environment",
		"host.name",
		"host.id",
		"process.pid",
		"process.command",
		"os.type",
		"cloud.provider",
		"cloud.region",
	}

	// First pass: populate cache
	for _, attr := range attributes {
		cache.Get(attr)
	}

	if cache.Len() != len(attributes) {
		t.Fatalf("Expected cache length %d, got %d", len(attributes), cache.Len())
	}

	// Second pass: verify all cached (simulate 1000 log records)
	for i := 0; i < 1000; i++ {
		for _, attr := range attributes {
			_ = cache.Get(attr)
		}
	}

	// Cache should still have same length
	if cache.Len() != len(attributes) {
		t.Errorf("Cache length changed during repeated access")
	}
}

func BenchmarkPrefixedRowKeyCache_Get_Cached(b *testing.B) {
	cache := NewPrefixedRowKeyCache("resource")

	// Pre-populate cache
	attributes := []string{
		"service.name",
		"deployment.environment",
		"host.name",
	}
	for _, attr := range attributes {
		cache.Get(attr)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cache.Get(attributes[i%len(attributes)])
	}
}

func BenchmarkPrefixedRowKeyCache_Get_Uncached(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		cache := NewPrefixedRowKeyCache("resource")
		b.StartTimer()
		_ = cache.Get("service.name")
	}
}

func BenchmarkPrefixedRowKeyCache_vs_Direct(b *testing.B) {
	b.Run("with_cache", func(b *testing.B) {
		cache := NewPrefixedRowKeyCache("resource")
		name := "service.name"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = cache.Get(name)
		}
	})

	b.Run("direct_call", func(b *testing.B) {
		name := "service.name"
		prefix := "resource"
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = prefixAttributeRowKey(name, prefix)
		}
	})
}
