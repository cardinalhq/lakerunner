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
	// Nanoseconds since Unix epoch.
	Timestamp int64
	Body      string
	// Attributes partitioned by destination.
	ResourceAttributes map[string]string
	ScopeAttributes    map[string]string
	RecordAttributes   map[string]string
	RawAttributes      map[string]any
}

func ParseLogRow(mapper *Mapper, input map[string]any) TranslatedLog {
	now := time.Now().UnixNano()
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
			if parsed, ok := coerceToUnixNanos(rawVal, mapper.TimeFormat); ok {
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

// coerceToUnixNanos interprets various input representations and normalizes to nanoseconds.
func coerceToUnixNanos(v any, tformat string) (int64, bool) {
	switch t := v.(type) {
	case time.Time:
		return t.UnixNano(), true
	case *time.Time:
		if t == nil {
			return 0, false
		}
		return t.UnixNano(), true
	case int64:
		return normalizeEpochNumber(t)
	case int:
		return normalizeEpochNumber(int64(t))
	case uint64:
		return normalizeEpochNumber(int64(t))
	case uint:
		return normalizeEpochNumber(int64(t))
	case float64:
		return normalizeEpochNumber(int64(t))
	case string:
		// Try parse integer (allow trimming spaces)
		s := strings.TrimSpace(t)
		if s == "" {
			return 0, false
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return normalizeEpochNumber(n)
		}
		if tformat != "" {
			if ts, err := time.Parse(tformat, s); err == nil {
				return ts.UnixNano(), true
			}
		}
	}
	return 0, false
}

const (
	// Chosen to keep any multiplication within int64 limits:
	// 4,000,000,000 seconds * 1e9 = 4e18 < 9.22e18 (safe)
	secondsUpper      = int64(4_000_000_000)     // ~ year 2100
	millisecondsUpper = secondsUpper * 1_000     // 4e12
	microsecondsUpper = secondsUpper * 1_000_000 // 4e15
)

// normalizeEpochNumber guesses the unit (s, ms, µs, ns) and returns ns.
// Returns (0, false) if the input is negative or cannot be normalized.
func normalizeEpochNumber(n int64) (int64, bool) {
	if n < 0 {
		return 0, false
	}
	switch {
	case n < secondsUpper:
		return n * int64(time.Second), true // seconds -> ns
	case n < millisecondsUpper:
		return n * int64(time.Millisecond), true // ms -> ns
	case n < microsecondsUpper:
		return n * int64(time.Microsecond), true // µs -> ns
	default:
		return n, true // assume already ns
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
