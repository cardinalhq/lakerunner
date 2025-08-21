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
	"os"
	"testing"
)

func TestGetBatchSizeForSignal(t *testing.T) {
	tests := []struct {
		name         string
		signal       string
		envVar       string
		envValue     string
		expectedSize int
	}{
		{
			name:         "logs with custom env var",
			signal:       "logs",
			envVar:       "LAKERUNNER_LOGS_BATCH_SIZE",
			envValue:     "10",
			expectedSize: 10,
		},
		{
			name:         "metrics with custom env var",
			signal:       "metrics",
			envVar:       "LAKERUNNER_METRICS_BATCH_SIZE",
			envValue:     "5",
			expectedSize: 5,
		},
		{
			name:         "traces with custom env var",
			signal:       "traces",
			envVar:       "LAKERUNNER_TRACES_BATCH_SIZE",
			envValue:     "15",
			expectedSize: 15,
		},
		{
			name:         "logs with default",
			signal:       "logs",
			envVar:       "",
			envValue:     "",
			expectedSize: 1,
		},
		{
			name:         "unknown signal with default",
			signal:       "unknown",
			envVar:       "",
			envValue:     "",
			expectedSize: 1,
		},
		{
			name:         "invalid env value uses default",
			signal:       "logs",
			envVar:       "LAKERUNNER_LOGS_BATCH_SIZE",
			envValue:     "invalid",
			expectedSize: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up any existing env var
			if tt.envVar != "" {
				defer func() {
					os.Unsetenv(tt.envVar)
				}()
				if tt.envValue != "" {
					os.Setenv(tt.envVar, tt.envValue)
				}
			}

			result := GetBatchSizeForSignal(tt.signal)
			if result != tt.expectedSize {
				t.Errorf("Expected batch size %d, got %d", tt.expectedSize, result)
			}
		})
	}
}

func TestGetTargetSizeBytes(t *testing.T) {
	tests := []struct {
		name          string
		envValue      string
		expectedBytes int64
	}{
		{
			name:          "default value",
			envValue:      "",
			expectedBytes: 1024 * 1024,
		},
		{
			name:          "custom value",
			envValue:      "5242880",
			expectedBytes: 5242880,
		},
		{
			name:          "invalid value uses default",
			envValue:      "invalid",
			expectedBytes: 1024 * 1024,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				os.Unsetenv("LAKERUNNER_TARGET_SIZE_BYTES")
			}()

			if tt.envValue != "" {
				os.Setenv("LAKERUNNER_TARGET_SIZE_BYTES", tt.envValue)
			}

			result := GetTargetSizeBytes()
			if result != tt.expectedBytes {
				t.Errorf("Expected target size %d bytes, got %d bytes", tt.expectedBytes, result)
			}
		})
	}
}

func TestGetMaxBatchSize(t *testing.T) {
	tests := []struct {
		name         string
		envValue     string
		expectedSize int
	}{
		{
			name:         "default value",
			envValue:     "",
			expectedSize: 20,
		},
		{
			name:         "custom value",
			envValue:     "50",
			expectedSize: 50,
		},
		{
			name:         "invalid value uses default",
			envValue:     "invalid",
			expectedSize: 20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				os.Unsetenv("LAKERUNNER_MAX_BATCH_SIZE")
			}()

			if tt.envValue != "" {
				os.Setenv("LAKERUNNER_MAX_BATCH_SIZE", tt.envValue)
			}

			result := GetMaxBatchSize()
			if result != tt.expectedSize {
				t.Errorf("Expected max batch size %d, got %d", tt.expectedSize, result)
			}
		})
	}
}
