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

package queryworker

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsMissingFingerprintError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "explicit chq_fingerprint not found error",
			err:      errors.New("Binder Error: column chq_fingerprint not found"),
			expected: true,
		},
		{
			name:     "chq_fingerprint with Binder Error",
			err:      errors.New("Binder Error: Referenced column \"chq_fingerprint\" not found in FROM clause"),
			expected: true,
		},
		{
			name:     "chq_fingerprint not found without Binder",
			err:      errors.New("column chq_fingerprint not found in table"),
			expected: true,
		},
		{
			name:     "context deadline exceeded - not treated as fingerprint error",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		{
			name:     "non-fingerprint column error",
			err:      errors.New("Binder Error: column other_column not found"),
			expected: false,
		},
		{
			name:     "generic error",
			err:      errors.New("some random error"),
			expected: false,
		},
		{
			name:     "context canceled error",
			err:      context.Canceled,
			expected: false,
		},
		{
			name:     "fingerprint in error but not column related",
			err:      errors.New("invalid fingerprint value provided"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := isMissingFingerprintError(tt.err)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestRemoveFingerprintNormalization(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "SQL with fingerprint normalization",
			input:    `WITH s0 AS (SELECT * FROM table), s1 AS (SELECT s0.* REPLACE(CAST("chq_fingerprint" AS VARCHAR) AS "chq_fingerprint") FROM s0) SELECT * FROM s1`,
			expected: `WITH s0 AS (SELECT * FROM table), s1 AS (SELECT s0.* FROM s0) SELECT * FROM s1`,
		},
		{
			name:     "SQL without fingerprint normalization",
			input:    `SELECT * FROM table WHERE id = 1`,
			expected: `SELECT * FROM table WHERE id = 1`,
		},
		{
			name:     "SQL with multiple REPLACE clauses (only fingerprint removed)",
			input:    `SELECT s0.* REPLACE(CAST("chq_fingerprint" AS VARCHAR) AS "chq_fingerprint"), s1.* REPLACE(CAST("chq_fingerprint" AS VARCHAR) AS "chq_fingerprint") FROM s0`,
			expected: `SELECT s0.*, s1.* FROM s0`,
		},
		{
			name:     "empty string",
			input:    ``,
			expected: ``,
		},
		{
			name:     "SQL with similar but different REPLACE",
			input:    `SELECT s0.* REPLACE(CAST("other_column" AS VARCHAR) AS "other_column") FROM s0`,
			expected: `SELECT s0.* REPLACE(CAST("other_column" AS VARCHAR) AS "other_column") FROM s0`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := removeFingerprintNormalization(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
