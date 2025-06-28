// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package helpers

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sort"
)

func ComputeTID(metricName string, tags map[string]any) int64 {
	keys := make([]string, 0, len(tags))
	for k, v := range tags {
		if v == "" || k[0] == '_' {
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
