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
	"testing"
)

func TestGetFileType(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		// Basic cases
		{"foo.txt", "foo"},
		{"bar.log.gz", "barlog"},
		{"baz", "baz"},
		{"foo.bar.baz", "foobar"},
		// Path cases
		{"/tmp/foo.txt", "foo"},
		{"./foo.txt", "foo"},
		{"dir/subdir/file.tar.gz", "filetar"},
		// Non-letter characters
		{"file-123.log", "file"},
		{"file_abc-xyz.2024-06-01.log", "fileabcxyz"},
		{"file@name!.txt", "filename"},
		// No extension
		{"filename", "filename"},
		// Empty string
		{"", ""},
	}

	for _, tt := range tests {
		got := GetFileType(tt.input)
		if got != tt.expected {
			t.Errorf("GetFileType(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}
