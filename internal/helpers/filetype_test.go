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

func TestExtractCustomerDomain(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Valid patterns (domain-hyphen-rest)
		{
			name:     "simple domain with hyphen",
			input:    "example.com-abc-123456_2025-01-01-120000_server",
			expected: "example.com",
		},
		{
			name:     "subdomain pattern",
			input:    "app.example.com-xyz-987654_2025-02-15-143000_controller",
			expected: "app.example.com",
		},
		{
			name:     "multi-level subdomain",
			input:    "api.v2.example.org-def-555555_2025-03-20-100000_worker",
			expected: "api.v2.example.org",
		},
		// Invalid patterns (no hyphen)
		{
			name:     "no hyphen",
			input:    "example.com_abc_123456",
			expected: "",
		},
		// Invalid patterns (no dot in prefix)
		{
			name:     "no dot before hyphen",
			input:    "example-abc-123456_2025-01-01-120000_server",
			expected: "",
		},
		{
			name:     "only single word before hyphen",
			input:    "server-abc-123456_2025-01-01-120000_instance",
			expected: "",
		},
		// Edge cases
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "just hyphen",
			input:    "-",
			expected: "",
		},
		{
			name:     "domain only without hyphen",
			input:    "example.com",
			expected: "",
		},
		{
			name:     "hyphen at start",
			input:    "-example.com",
			expected: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ExtractCustomerDomain(tt.input)
			if got != tt.expected {
				t.Errorf("ExtractCustomerDomain(%q) = %q; want %q", tt.input, got, tt.expected)
			}
		})
	}
}
