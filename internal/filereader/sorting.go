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

import "github.com/cardinalhq/lakerunner/pipeline"

// SortKey represents a key that can be compared for sorting
type SortKey interface {
	// Compare returns:
	// -1 if this key should come before other
	//  0 if this key equals other
	//  1 if this key should come after other
	Compare(other SortKey) int

	// Release returns the key to its pool for reuse
	Release()
}

// SortKeyProvider creates sort keys from rows
type SortKeyProvider interface {
	// MakeKey creates a sort key from a row
	MakeKey(row pipeline.Row) SortKey
}
