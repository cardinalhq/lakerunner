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

import "testing"

func TestPrefixAttribute(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		prefix   string
		expected string
	}{
		{
			name:     "empty name returns empty string",
			input:    "",
			prefix:   "foo",
			expected: "",
		},
		{
			name:     "name starts with underscore returns name",
			input:    "_internal",
			prefix:   "foo",
			expected: "_internal",
		},
		{
			name:     "normal name returns prefixed",
			input:    "bar",
			prefix:   "foo",
			expected: "foo_bar",
		},
		{
			name:     "empty prefix",
			input:    "bar",
			prefix:   "",
			expected: "_bar",
		},
		{
			name:     "underscore not at start",
			input:    "foo_bar",
			prefix:   "baz",
			expected: "baz_foo_bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := prefixAttribute(tt.input, tt.prefix)
			if result != tt.expected {
				t.Errorf("prefixAttribute(%q, %q) = %q; want %q", tt.input, tt.prefix, result, tt.expected)
			}
		})
	}
}
