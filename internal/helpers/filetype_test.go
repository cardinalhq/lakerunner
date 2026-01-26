// Copyright (C) 2025-2026 CardinalHQ, Inc
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
		// Valid patterns with hyphen separator
		{
			name:     "simple domain with hyphen",
			input:    "example.com-abc-123456_2025-01-01-120000_server",
			expected: "example.com",
		},
		{
			name:     "subdomain pattern with hyphen",
			input:    "app.example.com-xyz-987654_2025-02-15-143000_controller",
			expected: "app.example.com",
		},
		{
			name:     "multi-level subdomain with hyphen",
			input:    "api.v2.example.org-def-555555_2025-03-20-100000_worker",
			expected: "api.v2.example.org",
		},
		// Hyphenated domains (Codex feedback case)
		{
			name:     "hyphenated domain name",
			input:    "foo-bar.example.com-abc-123",
			expected: "foo-bar.example.com",
		},
		{
			name:     "multi-hyphen domain",
			input:    "my-api-server.example.org-instance-001_2025-01-01",
			expected: "my-api-server.example.org",
		},
		{
			name:     "hyphenated subdomain",
			input:    "app-v2.test-env.example.com_xyz_123",
			expected: "app-v2.test-env.example.com",
		},
		// Valid patterns with underscore separator
		{
			name:     "domain with underscore",
			input:    "example.com_abc_123456_2025-01-01-120000_server",
			expected: "example.com",
		},
		{
			name:     "subdomain with underscore",
			input:    "app.example.com_xyz_987654",
			expected: "app.example.com",
		},
		// Domain only (no separator)
		{
			name:     "domain only without separator",
			input:    "example.com",
			expected: "example.com",
		},
		{
			name:     "subdomain only without separator",
			input:    "app.example.com",
			expected: "app.example.com",
		},
		// Multi-level TLDs
		{
			name:     "co.uk TLD with separator",
			input:    "example.co.uk-abc-123",
			expected: "example.co.uk",
		},
		{
			name:     "co.uk TLD without separator",
			input:    "example.co.uk",
			expected: "example.co.uk",
		},
		// Invalid patterns (no dot)
		{
			name:     "no dot at all with hyphen",
			input:    "example-abc-123456",
			expected: "",
		},
		{
			name:     "no dot at all with underscore",
			input:    "example_abc_123456",
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
			name:     "just underscore",
			input:    "_",
			expected: "",
		},
		{
			name:     "just a dot",
			input:    ".",
			expected: ".",
		},
		// Real-world patterns with timestamps containing dots
		{
			name:     "vitechinc.com pattern with timestamp dots",
			input:    "vitechinc.com-abu-hirw8pmdunp-1736355841.4503462_2025-12-03-045149_controller",
			expected: "vitechinc.com",
		},
		{
			name:     "hubinternational.com pattern with timestamp dots",
			input:    "hubinternational.com-abu-6fqwfyfeb55a6baaa-1760715893.4401085_2025-12-03-051227_controller",
			expected: "hubinternational.com",
		},
		{
			name:     "rubrik.com pattern with timestamp dots and region",
			input:    "rubrik.com-abu-a6m9p3g8dnw4de0de-1748363120.6530867_2025-12-03-030919_aws-ca-central-1",
			expected: "rubrik.com",
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
