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

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func prefixAttributeRowKey(name, prefix string) wkk.RowKey {
	if name == "" {
		return wkk.NewRowKey("")
	}

	// Fast path: check if there are any dots to replace
	hasDots := strings.Contains(name, ".")

	if name[0] == '_' {
		// Underscore-prefixed: just replace dots if present
		if !hasDots {
			return wkk.NewRowKey(name)
		}
		return wkk.NewRowKey(strings.ReplaceAll(name, ".", "_"))
	}

	// Normal attribute: add prefix
	if !hasDots {
		// No dots: simple concatenation
		var b strings.Builder
		b.Grow(len(prefix) + 1 + len(name))
		b.WriteString(prefix)
		b.WriteByte('_')
		b.WriteString(name)
		return wkk.NewRowKey(b.String())
	}

	// Has dots: replace and add prefix
	var b strings.Builder
	b.Grow(len(prefix) + 1 + len(name)) // Estimate capacity
	b.WriteString(prefix)
	b.WriteByte('_')
	for i := 0; i < len(name); i++ {
		if name[i] == '.' {
			b.WriteByte('_')
		} else {
			b.WriteByte(name[i])
		}
	}
	return wkk.NewRowKey(b.String())
}

// prefixAttribute is kept for compatibility with tests
func prefixAttribute(name, prefix string) string {
	if name == "" {
		return ""
	}
	if name[0] == '_' {
		// Convert dots to underscores even for underscore-prefixed fields
		return strings.ReplaceAll(name, ".", "_")
	}
	// Use underscore separator for PromQL/LogQL compatibility
	return prefix + "_" + strings.ReplaceAll(name, ".", "_")
}
