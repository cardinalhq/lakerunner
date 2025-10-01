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

package pipeline

import (
	"maps"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// Row represents a single data row as a map of RowKey to any value.
type Row map[wkk.RowKey]any

// CopyRow creates a deep copy of a row.
func CopyRow(in Row) Row {
	return copyRow(in)
}

// copyRow creates a deep copy of a row using the lower-level maps.Copy primitive.
func copyRow(in Row) Row {
	out := make(Row, len(in))
	maps.Copy(out, in)
	return out
}

// ToStringMap converts a Row to map[string]any for compatibility with legacy interfaces.
func ToStringMap(row Row) map[string]any {
	result := make(map[string]any, len(row))
	for key, value := range row {
		result[string(key.Value())] = value
	}
	return result
}
