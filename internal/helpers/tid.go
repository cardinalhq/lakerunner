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

package helpers

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
)

func ComputeTID(metricName string, tags map[string]any) int64 {
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		if v == "" || (k[0] == '_' && k != "_cardinalhq.metric_type") {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	_, _ = h.Write([]byte(metricName))
	for _, k := range keys {
		switch v := tags[k].(type) {
		case string:
			_, _ = h.Write([]byte(k + "=" + v + "|"))
		default:
			_, _ = h.Write([]byte(k + fmt.Sprintf("=%v|", v)))
		}
	}
	return int64(h.Sum64())
}

func MatchTags(existing, new map[string]any) map[string][]any {
	mismatches := make(map[string][]any)

	for k, v := range existing {
		if nv, ok := new[k]; !ok || nv != v {
			mismatches[k] = []any{v, new[k]}
		}
	}

	for k, v := range new {
		if _, ok := existing[k]; !ok {
			mismatches[k] = []any{nil, v}
		}
	}

	return mismatches
}

func GetFloat64Value(m map[string]any, key string) (float64, bool) {
	val, ok := m[key]
	if !ok || val == nil {
		return 0, false
	}
	floatVal, ok := val.(float64)
	if !ok {
		return 0, false
	}
	return floatVal, true
}

func GetStringValue(m map[string]any, key string) (string, bool) {
	val, ok := m[key]
	if !ok || val == nil {
		return "", false
	}
	strVal, ok := val.(string)
	if !ok {
		return "", false
	}
	return strVal, true
}

func GetInt64Value(m map[string]any, key string) (int64, bool) {
	val, ok := m[key]
	if !ok || val == nil {
		return 0, false
	}
	intVal, ok := val.(int64)
	if !ok {
		return 0, false
	}
	return intVal, true
}

// GetTIDValue extracts _cardinalhq.tid from a map, handling both int64 and string types
func GetTIDValue(m map[string]any, key string) (int64, bool) {
	val, ok := m[key]
	if !ok || val == nil {
		return 0, false
	}

	// Try int64 first (expected type)
	if intVal, ok := val.(int64); ok {
		return intVal, true
	}

	// Fall back to string conversion for backwards compatibility
	if strVal, ok := val.(string); ok {
		if parsed, err := strconv.ParseInt(strVal, 10, 64); err == nil {
			return parsed, true
		}
	}

	return 0, false
}

func MakeTags(rec map[string]any) map[string]any {
	tags := make(map[string]any)
	for k, v := range rec {
		if v == nil || k == "" || fmt.Sprintf("%v", v) == "" {
			continue
		}
		if k[0] == '_' {
			continue
		}
		tags[k] = v
	}
	return tags
}

func GetFloat64SliceJSON(m map[string]any, key string) ([]float64, bool) {
	val, ok := m[key]
	if !ok || val == nil {
		return nil, false
	}
	sliceString, ok := val.(string)
	if !ok {
		return nil, false
	}
	var floatSlice []float64
	if err := json.Unmarshal([]byte(sliceString), &floatSlice); err != nil {
		return nil, false
	}
	return floatSlice, true
}
