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
	"encoding/json"

	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// MarshalJSON implements json.Marshaler for Row
func (r Row) MarshalJSON() ([]byte, error) {
	buf := make([]byte, 0, 512)
	buf = append(buf, '{')

	first := true
	for k, v := range r {
		if !first {
			buf = append(buf, ',')
		}
		first = false

		// Render the key as a JSON string directly
		buf = append(buf, '"')
		buf = appendEscapedString(buf, wkk.RowKeyValue(k))
		buf = append(buf, '"', ':')

		// Marshal the value - handle strings directly
		if s, ok := v.(string); ok {
			buf = append(buf, '"')
			buf = appendEscapedString(buf, s)
			buf = append(buf, '"')
		} else {
			valueBytes, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			buf = append(buf, valueBytes...)
		}
	}

	buf = append(buf, '}')
	return buf, nil
}

// appendEscapedString appends s to buf with JSON string escaping
func appendEscapedString(buf []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch c {
		case '"', '\\':
			buf = append(buf, '\\', c)
		case '\b':
			buf = append(buf, '\\', 'b')
		case '\f':
			buf = append(buf, '\\', 'f')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\t':
			buf = append(buf, '\\', 't')
		default:
			if c < 0x20 {
				buf = append(buf, '\\', 'u', '0', '0', hexDigit(c>>4), hexDigit(c&0xF))
			} else {
				buf = append(buf, c)
			}
		}
	}
	return buf
}

// hexDigit returns the hex digit for a value 0-15
func hexDigit(n byte) byte {
	if n < 10 {
		return '0' + n
	}
	return 'a' + (n - 10)
}

// UnmarshalJSON implements json.Unmarshaler for Row
func (r *Row) UnmarshalJSON(data []byte) error {
	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}

	*r = make(Row, len(m))
	for k, v := range m {
		(*r)[wkk.NewRowKey(k)] = v
	}
	return nil
}

// Marshal marshals a Row to JSON bytes.
// It converts the Row's interned RowKeys to regular strings for JSON serialization,
// without creating an intermediate map[string]any.
func (r Row) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

// Unmarshal unmarshals JSON bytes into a Row.
// It automatically interns all string keys from the JSON into RowKeys,
// enabling efficient key comparison and memory usage.
func (r *Row) Unmarshal(data []byte) error {
	return json.Unmarshal(data, r)
}
