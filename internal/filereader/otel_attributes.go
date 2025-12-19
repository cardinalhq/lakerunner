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

// prefixAttributeRowKey creates a RowKey for an OTEL attribute with the given prefix.
// Uses wkk.NormalizeName for consistent OTEL-to-underscore conversion.
func prefixAttributeRowKey(name, prefix string) wkk.RowKey {
	if name == "" {
		return wkk.NewRowKey("")
	}

	normalized := wkk.NormalizeName(name)
	if name[0] == '_' {
		// Underscore-prefixed: no prefix added
		return wkk.NewRowKey(normalized)
	}

	// Normal attribute: add prefix
	var b strings.Builder
	b.Grow(len(prefix) + 1 + len(normalized))
	b.WriteString(prefix)
	b.WriteByte('_')
	b.WriteString(normalized)
	return wkk.NewRowKey(b.String())
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
