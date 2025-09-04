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

package ingestion

import (
	"testing"
)


func TestGetResourceFile(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"foo/Support/bar/baz.txt", "bar"},
		{"a/b/Support/c/d.txt", "c"},
		{"foo/bar/Support", ""},
		{"Support", ""},
		{"x/Support/y/Support/z/file.txt", "y"},
		{"Support/foo.txt", "foo.txt"},
		{"foo/bar/baz.txt", ""},
		{"foo/Support/", ""},
		{"foo/Support/bar", "bar"},
		{"foo/Support/bar/baz/qux.txt", "bar"},
		{"", ""},
	}

	for _, tt := range tests {
		got := getResourceFile(tt.input)
		if got != tt.expected {
			t.Errorf("getResourceFile(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}
