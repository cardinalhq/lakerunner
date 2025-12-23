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
	"strings"
	"sync"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// prefixedKeyCache caches (prefix, name) -> RowKey mappings to avoid repeated
// string allocations for the same OTEL attributes.
//
// TODO: This cache grows without bound. In practice, the number of unique
// attribute names is small and bounded by the OTEL schema, but we should
// consider adding an LRU eviction policy or size limit if this becomes
// a memory issue.
var prefixedKeyCache sync.Map // "prefix:name" -> wkk.RowKey

// prefixAttributeRowKey creates a RowKey for an OTEL attribute with the given prefix.
// Uses wkk.NormalizeName for consistent OTEL-to-underscore conversion (includes lowercasing).
// Results are cached to avoid repeated allocations for common attribute names.
func prefixAttributeRowKey(name, prefix string) wkk.RowKey {
	if name == "" {
		return wkk.NewRowKey("")
	}

	// Fast path: check cache
	cacheKey := prefix + ":" + name
	if cached, ok := prefixedKeyCache.Load(cacheKey); ok {
		return cached.(wkk.RowKey)
	}

	// Slow path: compute and cache
	// NormalizeName already lowercases and replaces non-alphanumeric chars with underscores
	normalized := wkk.NormalizeName(name)
	var result wkk.RowKey
	if name[0] == '_' {
		// Underscore-prefixed: no prefix added
		result = wkk.NewRowKey(normalized)
	} else {
		// Normal attribute: add prefix
		var b strings.Builder
		b.Grow(len(prefix) + 1 + len(normalized))
		b.WriteString(prefix)
		b.WriteByte('_')
		b.WriteString(normalized)
		result = wkk.NewRowKey(b.String())
	}

	prefixedKeyCache.Store(cacheKey, result)
	return result
}

// prefixAttribute creates a prefixed attribute name string.
// Uses wkk.NormalizeName for consistent OTEL-to-underscore conversion.
func prefixAttribute(name, prefix string) string {
	if name == "" {
		return ""
	}
	normalized := wkk.NormalizeName(name)
	if name[0] == '_' {
		return normalized
	}
	return prefix + "_" + normalized
}
