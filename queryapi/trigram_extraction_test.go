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

package queryapi

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestTrigramExtraction tests what trigrams the codesearch library extracts
// from various regex patterns
func TestTrigramExtraction(t *testing.T) {
	tests := []struct {
		name            string
		pattern         string
		expectedTrigrams int
		description     string
	}{
		{
			name:            "contains_pattern_with_wildcards",
			pattern:         ".*cloudxcommand.*",
			expectedTrigrams: 11, // Codesearch extracts trigrams from the literal part!
			description:     "Pattern with wildcards - codesearch extracts trigrams from literal portion",
		},
		{
			name:            "exact_pattern_no_wildcards",
			pattern:         "cloudxcommand",
			expectedTrigrams: 11, // Should extract trigrams from the literal string
			description:     "Exact pattern without wildcards - should extract trigrams",
		},
		{
			name:            "pattern_with_trailing_wildcard_only",
			pattern:         "cloudxcommand.*",
			expectedTrigrams: 11, // Should still extract trigrams
			description:     "Pattern with only trailing wildcard - can still extract trigrams",
		},
		{
			name:            "short_string",
			pattern:         ".*ab.*",
			expectedTrigrams: 0, // Too short for trigrams
			description:     "String too short for trigrams",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use buildLabelTrigram like the actual code does
			tq, fps := buildLabelTrigram("test_field", tt.pattern)

			t.Logf("Pattern: %s", tt.pattern)
			t.Logf("Extracted trigrams: %d", len(tq.Trigram))
			t.Logf("Fingerprints: %d", len(fps))
			for i, tri := range tq.Trigram {
				t.Logf("  Trigram %d: %s", i, tri)
			}

			if tt.expectedTrigrams == 0 {
				assert.Empty(t, tq.Trigram, tt.description)
			} else {
				assert.GreaterOrEqual(t, len(tq.Trigram), tt.expectedTrigrams, tt.description)
			}
		})
	}
}

// TestBuildLabelTrigramWithContainsPattern tests what buildLabelTrigram returns
// for a typical "contains" query pattern
func TestBuildLabelTrigramWithContainsPattern(t *testing.T) {
	label := "resource_file_type"
	pattern := ".*cloudxcommand.*" // This is what we build for CONTAINS queries

	tq, fps := buildLabelTrigram(label, pattern)

	t.Logf("Pattern: %s", pattern)
	t.Logf("Trigrams extracted: %d", len(tq.Trigram))
	t.Logf("Fingerprints generated: %d", len(fps))

	for i, tri := range tq.Trigram {
		t.Logf("  Trigram %d: %s", i, tri)
	}

	// The codesearch library is smart - it extracts trigrams from the literal part!
	assert.NotEmpty(t, tq.Trigram, "Codesearch extracts trigrams from literal portion even with wildcards")
	assert.NotEmpty(t, fps, "Fingerprints generated from extracted trigrams")
	assert.Equal(t, 11, len(tq.Trigram), "Should extract 11 trigrams from 'cloudxcommand'")
}

// TestAlternativeContainsApproach shows what we should do instead
func TestAlternativeContainsApproach(t *testing.T) {
	label := "resource_file_type"

	// Instead of building .*value.*, we should just use the value itself
	// and then match it as a substring in the query evaluation
	value := "cloudxcommand"

	tq, fps := buildLabelTrigram(label, value)

	t.Logf("Pattern: %s", value)
	t.Logf("Trigrams extracted: %d", len(tq.Trigram))
	t.Logf("Fingerprints generated: %d", len(fps))

	// This should generate trigrams!
	assert.NotEmpty(t, tq.Trigram, "Should extract trigrams from literal value")
	assert.NotEmpty(t, fps, "Should generate fingerprints from trigrams")

	t.Logf("Trigrams: %v", tq.Trigram)
}

// TestShortValueTrigramExtraction tests what happens with short values like "test"
func TestShortValueTrigramExtraction(t *testing.T) {
	tests := []struct {
		value            string
		expectedMinGrams int
	}{
		{"test", 2},              // "tes" and "est"
		{"cloudxcommand", 11},    // many trigrams
		{"ab", 0},                // too short
		{"abc", 1},               // exactly 3 chars = 1 trigram
	}

	for _, tt := range tests {
		t.Run(tt.value, func(t *testing.T) {
			pattern := ".*" + tt.value + ".*"
			tq, fps := buildLabelTrigram("test_field", pattern)

			t.Logf("Value: %s, Pattern: %s", tt.value, pattern)
			t.Logf("Trigrams: %d, Fingerprints: %d", len(tq.Trigram), len(fps))
			t.Logf("Actual trigrams: %v", tq.Trigram)

			if tt.expectedMinGrams > 0 {
				assert.GreaterOrEqual(t, len(tq.Trigram), tt.expectedMinGrams,
					"Should extract at least %d trigrams from %q", tt.expectedMinGrams, tt.value)
			} else {
				assert.Empty(t, tq.Trigram, "Should not extract trigrams from %q", tt.value)
			}
		})
	}
}
