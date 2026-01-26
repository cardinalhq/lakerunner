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
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// PrefixedRowKeyCache caches prefixed RowKeys to avoid repeated string building
// and interning. Each unique attribute name is transformed once and reused.
//
// Thread-safety: This cache is NOT thread-safe. It's designed for single-threaded
// use within a reader that processes one file at a time.
type PrefixedRowKeyCache struct {
	prefix string
	cache  map[string]wkk.RowKey
}

// NewPrefixedRowKeyCache creates a new RowKey cache with the given prefix.
// The prefix will be prepended to all attribute names (e.g., "resource", "scope", "attr").
func NewPrefixedRowKeyCache(prefix string) *PrefixedRowKeyCache {
	return &PrefixedRowKeyCache{
		prefix: prefix,
		cache:  make(map[string]wkk.RowKey),
	}
}

// Get returns the cached RowKey for the given attribute name, or computes and caches it
// if not already present. The returned RowKey has the prefix applied and dots replaced
// with underscores (e.g., "service.name" with prefix "resource" → "resource_service_name").
func (c *PrefixedRowKeyCache) Get(name string) wkk.RowKey {
	key, found := c.cache[name]
	if !found {
		key = prefixAttributeRowKey(name, c.prefix)
		c.cache[name] = key
	}
	return key
}

// Len returns the number of cached RowKeys.
func (c *PrefixedRowKeyCache) Len() int {
	return len(c.cache)
}

// Clear removes all cached entries. Useful for resetting between files.
func (c *PrefixedRowKeyCache) Clear() {
	c.cache = make(map[string]wkk.RowKey)
}
