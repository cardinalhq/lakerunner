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

package translate

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
)

type TranslatedLog struct {
	// Milliseconds since Unix epoch.
	Timestamp int64
	Body      string
	// Attributes partitioned by destination.
	ResourceAttributes map[string]string
	ScopeAttributes    map[string]string
	RecordAttributes   map[string]string
	RawAttributes      map[string]any
}

func ParseLogRow(mapper *Mapper, input map[string]any) TranslatedLog {
	now := time.Now().UnixMilli()
	var ts int64 = -1
	var body string

	resAttrs := make(map[string]string)
	scopeAttrs := make(map[string]string)
	recAttrs := make(map[string]string)
	rawAttrs := make(map[string]any)

	for rawKey, rawVal := range input {
		if rawVal == nil {
			continue
		}

		lc := strings.ToLower(rawKey)

		// Timestamp?
		if ts <= 0 && slices.Contains(mapper.TimestampColumns, lc) {
			if parsed, ok := coerceToUnixMillis(rawVal, mapper.TimeFormat); ok {
				ts = parsed
			}
			continue
		}

		// Message?
		if body == "" && slices.Contains(mapper.MessageColumns, lc) {
			body = toString(rawVal)
			continue
		}

		if strings.HasPrefix(lc, "_") {
			rawAttrs[lc] = rawVal
			continue
		}

		// Decide promotion target maps for this top-level key.
		target := recAttrs
		if slices.Contains(mapper.ResourceColumns, lc) || strings.HasPrefix(lc, "resource.") {
			lc = strings.TrimPrefix(lc, "resource.")
			target = resAttrs
		} else if slices.Contains(mapper.ScopeColumns, lc) || strings.HasPrefix(lc, "scope.") {
			lc = strings.TrimPrefix(lc, "scope.")
			target = scopeAttrs
		}
		lc = strings.TrimPrefix(lc, "log.")

		// Flatten (may produce multiple keys). Base prefix is lowercased top-level key.
		flattenValue(lc, rawVal, target)
	}

	if ts < 0 {
		ts = now
	}

	return TranslatedLog{
		Timestamp:          ts,
		Body:               body,
		ResourceAttributes: resAttrs,
		ScopeAttributes:    scopeAttrs,
		RecordAttributes:   recAttrs,
		RawAttributes:      rawAttrs,
	}
}

// coerceToUnixMillis interprets various input representations and normalizes to Unix milliseconds.
func coerceToUnixMillis(v any, tformat string) (int64, bool) {
	switch t := v.(type) {
	case time.Time:
		return t.UnixMilli(), true

	case *time.Time:
		if t == nil {
			return 0, false
		}
		return t.UnixMilli(), true

	case int64:
		return normalizeEpochToMillis(t)
	case int:
		return normalizeEpochToMillis(int64(t))
	case uint64:
		return normalizeEpochToMillis(int64(t))
	case uint:
		return normalizeEpochToMillis(int64(t))
	case float64:
		return normalizeEpochToMillis(int64(t))

	case string:
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		// integer string?
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return normalizeEpochToMillis(n)
		}
		// formatted timestamp?
		if tformat != "" {
			if ts, err := time.Parse(tformat, s); err == nil {
				return ts.UnixMilli(), true
			}
		}
	}

	return 0, false
}

const (
	// same thresholds as before, in their original units:
	secondsUpper      = int64(4_000_000_000)     // ~ year 2100 in seconds
	millisecondsUpper = secondsUpper * 1_000     // in milliseconds
	microsecondsUpper = secondsUpper * 1_000_000 // in microseconds
)

// normalizeEpochToMillis guesses the input unit (s, ms, µs, ns) based on magnitude
// and returns milliseconds. Precision finer than 1 ms is truncated.
func normalizeEpochToMillis(n int64) (int64, bool) {
	if n < 0 {
		return 0, false
	}
	switch {
	case n < secondsUpper:
		// seconds → ms
		return n * 1_000, true
	case n < millisecondsUpper:
		// already in ms
		return n, true
	case n < microsecondsUpper:
		// µs → ms (drop sub-ms)
		return n / 1_000, true
	default:
		// assume ns → ms (drop sub-ms)
		return n / 1_000_000, true
	}
}

// flattenValue flattens nested structures into dotted / indexed keys.
// Rules:
//
//	map -> prefix.child
//	slice -> prefix[index]
//
// Scalar -> prefix = string(value)
func flattenValue(prefix string, v any, out map[string]string) {
	switch val := v.(type) {
	case map[string]any:
		for k, nested := range val {
			child := k
			if prefix != "" {
				child = prefix + "." + child
			}
			flattenValue(child, nested, out)
		}
	case map[any]any: // common with YAML
		for rk, nested := range val {
			k := toString(rk)
			child := k
			if prefix != "" {
				child = prefix + "." + child
			}
			flattenValue(child, nested, out)
		}
	case []any:
		for i, elem := range val {
			flattenValue(prefix+"["+strconv.Itoa(i)+"]", elem, out)
		}
	case []string:
		for i, elem := range val {
			out[prefix+"["+strconv.Itoa(i)+"]"] = elem
		}
	case []int:
		for i, elem := range val {
			out[prefix+"["+strconv.Itoa(i)+"]"] = strconv.FormatInt(int64(elem), 10)
		}
	case []int64:
		for i, elem := range val {
			out[prefix+"["+strconv.Itoa(i)+"]"] = strconv.FormatInt(elem, 10)
		}
	case []float64:
		for i, elem := range val {
			out[prefix+"["+strconv.Itoa(i)+"]"] = strconv.FormatFloat(elem, 'f', -1, 64)
		}
	default:
		out[prefix] = toString(val)
	}
}

// toString renders supported scalar types to string (attributes stored as strings).
func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case time.Time:
		return x.Format(time.RFC3339Nano)
	case *time.Time:
		if x == nil {
			return ""
		}
		return x.Format(time.RFC3339Nano)
	case int:
		return strconv.FormatInt(int64(x), 10)
	case int8:
		return strconv.FormatInt(int64(x), 10)
	case int16:
		return strconv.FormatInt(int64(x), 10)
	case int32:
		return strconv.FormatInt(int64(x), 10)
	case int64:
		return strconv.FormatInt(x, 10)
	case uint:
		return strconv.FormatUint(uint64(x), 10)
	case uint8:
		return strconv.FormatUint(uint64(x), 10)
	case uint16:
		return strconv.FormatUint(uint64(x), 10)
	case uint32:
		return strconv.FormatUint(uint64(x), 10)
	case uint64:
		return strconv.FormatUint(x, 10)
	case float32:
		return strconv.FormatFloat(float64(x), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(x, 'f', -1, 64)
	case bool:
		if x {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", x)
	}
}
