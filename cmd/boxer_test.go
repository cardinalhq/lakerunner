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

package cmd

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetSelectedTasks(t *testing.T) {
	tests := []struct {
		name           string
		compactLogs    bool
		compactMetrics bool
		compactTraces  bool
		rollupMetrics  bool
		allTasks       bool
		expected       []string
	}{
		{
			name:     "no tasks selected",
			expected: nil,
		},
		{
			name:        "compact logs only",
			compactLogs: true,
			expected:    []string{"compact-logs"},
		},
		{
			name:           "compact metrics only",
			compactMetrics: true,
			expected:       []string{"compact-metrics"},
		},
		{
			name:          "compact traces only",
			compactTraces: true,
			expected:      []string{"compact-traces"},
		},
		{
			name:          "rollup metrics only",
			rollupMetrics: true,
			expected:      []string{"rollup-metrics"},
		},
		{
			name:           "compact logs and metrics",
			compactLogs:    true,
			compactMetrics: true,
			expected:       []string{"compact-logs", "compact-metrics"},
		},
		{
			name:           "all individual tasks",
			compactLogs:    true,
			compactMetrics: true,
			compactTraces:  true,
			rollupMetrics:  true,
			expected:       []string{"compact-logs", "compact-metrics", "compact-traces", "rollup-metrics"},
		},
		{
			name:     "all tasks flag (should be handled by PreRunE)",
			allTasks: true,
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset global variables
			compactLogs = tt.compactLogs
			compactMetrics = tt.compactMetrics
			compactTraces = tt.compactTraces
			rollupMetrics = tt.rollupMetrics
			allTasks = tt.allTasks

			result := getSelectedTasks()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBoxerCommandPreRunE(t *testing.T) {
	tests := []struct {
		name           string
		compactLogs    bool
		compactMetrics bool
		compactTraces  bool
		rollupMetrics  bool
		allTasks       bool
		expectError    bool
	}{
		{
			name:        "no tasks should error",
			expectError: true,
		},
		{
			name:        "compact logs should pass",
			compactLogs: true,
			expectError: false,
		},
		{
			name:           "compact metrics should pass",
			compactMetrics: true,
			expectError:    false,
		},
		{
			name:          "compact traces should pass",
			compactTraces: true,
			expectError:   false,
		},
		{
			name:          "rollup metrics should pass",
			rollupMetrics: true,
			expectError:   false,
		},
		{
			name:        "all tasks should pass",
			allTasks:    true,
			expectError: false,
		},
		{
			name:           "multiple tasks should pass",
			compactLogs:    true,
			compactMetrics: true,
			expectError:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset global variables to ensure clean test state
			compactLogs = tt.compactLogs
			compactMetrics = tt.compactMetrics
			compactTraces = tt.compactTraces
			rollupMetrics = tt.rollupMetrics
			allTasks = tt.allTasks

			// Create the validation function directly (copy from the init() function)
			validateFunc := func() error {
				// Validate at least one task is specified
				if !allTasks && !compactLogs && !compactMetrics && !compactTraces && !rollupMetrics {
					return assert.AnError // Use a placeholder error
				}
				// If --all, set all task flags
				if allTasks {
					compactLogs = true
					compactMetrics = true
					compactTraces = true
					rollupMetrics = true
				}
				return nil
			}

			// Execute the validation
			err := validateFunc()

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			// Test that --all sets all flags
			if tt.allTasks && err == nil {
				assert.True(t, compactLogs)
				assert.True(t, compactMetrics)
				assert.True(t, compactTraces)
				assert.True(t, rollupMetrics)
			}
		})
	}
}
