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

package logql

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildLabelFilterWhere_InOperatorRegex(t *testing.T) {
	// Test that the SQL WHERE clause is built correctly for 'in' operator with regex
	lfs := []LabelFilter{
		{
			Label: "resource_file",
			Op:    MatchRe,
			Value: `^(example\.com-1234567890\.11_2025-10-29-170129_server-04|example\.com-1234567890\.11_2025-10-29-170129_controller|example\.com-1234567890\.11_2025-10-29-170129_server-03)$`,
		},
	}

	where := buildLabelFilterWhere(lfs, nil)
	require.Len(t, where, 1)

	// The SQL should use regexp_matches with the pattern properly quoted
	expected := `regexp_matches(resource_file, '^(example\.com-1234567890\.11_2025-10-29-170129_server-04|example\.com-1234567890\.11_2025-10-29-170129_controller|example\.com-1234567890\.11_2025-10-29-170129_server-03)$')`
	assert.Equal(t, expected, where[0])

	// Verify the pattern contains escaped dots (single backslash)
	assert.Contains(t, where[0], `\.`)
	assert.NotContains(t, where[0], `\\.`)
}

func TestBuildLabelFilterWhere_EqOperator(t *testing.T) {
	// Test that the SQL WHERE clause is built correctly for 'eq' operator
	lfs := []LabelFilter{
		{
			Label: "resource_bucket_name",
			Op:    MatchEq,
			Value: "test-bucket",
		},
	}

	where := buildLabelFilterWhere(lfs, nil)
	require.Len(t, where, 1)

	expected := `resource_bucket_name = 'test-bucket'`
	assert.Equal(t, expected, where[0])
}

func TestSqlQuote_PreservesBackslashes(t *testing.T) {
	// Test that sqlQuote preserves backslashes
	tests := []struct {
		input    string
		expected string
	}{
		{`simple`, `'simple'`},
		{`with\backslash`, `'with\backslash'`},
		{`with\\double`, `'with\\double'`},
		{`with'quote`, `'with''quote'`},
		{`pattern\.with\.dots`, `'pattern\.with\.dots'`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sqlQuote(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
