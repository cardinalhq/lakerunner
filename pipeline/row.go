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

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
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

// GetString retrieves a string value from the Row.
// Returns empty string if the key is not found or the value is not a string.
func (r Row) GetString(key wkk.RowKey) string {
	if val, ok := r[key]; ok {
		if str, ok := val.(string); ok {
			return str
		}
	}
	return ""
}

// GetInt64 retrieves an int64 value from the Row.
// Returns the value and true if found and convertible, or 0 and false otherwise.
func (r Row) GetInt64(key wkk.RowKey) (int64, bool) {
	if val, ok := r[key]; ok {
		switch v := val.(type) {
		case int64:
			return v, true
		case int:
			return int64(v), true
		case float64:
			return int64(v), true
		}
	}
	return 0, false
}

// GetInt32 retrieves an int32 value from the Row.
// Returns the value and true if found and convertible, or 0 and false otherwise.
func (r Row) GetInt32(key wkk.RowKey) (int32, bool) {
	if val, ok := r[key]; ok {
		switch v := val.(type) {
		case int32:
			return v, true
		case int:
			return int32(v), true
		case int64:
			return int32(v), true
		case float64:
			return int32(v), true
		}
	}
	return 0, false
}
