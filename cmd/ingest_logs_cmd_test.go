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

package cmd

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
		got := getFileType(tt.input)
		if got != tt.expected {
			t.Errorf("getFileType(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}
