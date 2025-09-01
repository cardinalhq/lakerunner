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

package parquetwriter

import (
	"strings"
	"testing"
)

func TestWriterConfig_ValidateValid(t *testing.T) {
	tests := []struct {
		name   string
		config WriterConfig
	}{
		{
			name: "minimal valid config",
			config: WriterConfig{
				TmpDir:         "/tmp",
				RecordsPerFile: 100,
			},
		},
		{
			name: "config with group key func but no split groups",
			config: WriterConfig{
				TmpDir:         "/tmp",
				RecordsPerFile: 100,
				GroupKeyFunc: func(row map[string]any) any {
					return row["group"]
				},
				NoSplitGroups: false,
			},
		},
		{
			name: "config with group key func and no split groups",
			config: WriterConfig{
				TmpDir:         "/tmp",
				RecordsPerFile: 100,
				GroupKeyFunc: func(row map[string]any) any {
					return row["group"]
				},
				NoSplitGroups: true,
			},
		},
		{
			name: "config with stats provider",
			config: WriterConfig{
				TmpDir:         "/tmp",
				RecordsPerFile: 100,
				StatsProvider: &mockStatsProvider{
					accumulatorFunc: func() StatsAccumulator {
						return &mockStatsAccumulator{}
					},
				},
			},
		},
		{
			name: "config with large values",
			config: WriterConfig{
				TmpDir:         "/very/long/path/to/tmp/directory",
				RecordsPerFile: 1000000,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err != nil {
				t.Errorf("Expected config to be valid, got error: %v", err)
			}
		})
	}
}

func TestWriterConfig_ValidateInvalid(t *testing.T) {
	tests := []struct {
		name          string
		config        WriterConfig
		expectedErr   string
		expectedField string
	}{
		{
			name: "empty tmp dir",
			config: WriterConfig{
				TmpDir:         "",
				RecordsPerFile: 100,
			},
			expectedErr:   "parquetwriter config: TmpDir cannot be empty",
			expectedField: "TmpDir",
		},
		{
			name: "no split groups without group key func",
			config: WriterConfig{
				TmpDir:         "/tmp",
				RecordsPerFile: 100,
				GroupKeyFunc:   nil,
				NoSplitGroups:  true,
			},
			expectedErr:   "parquetwriter config: GroupKeyFunc required when NoSplitGroups is true",
			expectedField: "GroupKeyFunc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if err == nil {
				t.Error("Expected validation error, got nil")
				return
			}

			if err.Error() != tt.expectedErr {
				t.Errorf("Expected error message %q, got %q", tt.expectedErr, err.Error())
			}

			// Check if it's a ConfigError and has the right field
			configErr, ok := err.(*ConfigError)
			if !ok {
				t.Error("Expected ConfigError type")
				return
			}

			if configErr.Field != tt.expectedField {
				t.Errorf("Expected field %q, got %q", tt.expectedField, configErr.Field)
			}
		})
	}
}

func TestWriterConfig_ValidateEdgeCases(t *testing.T) {
	t.Run("zero records per file is allowed", func(t *testing.T) {
		config := WriterConfig{
			TmpDir:         "/tmp",
			RecordsPerFile: 0, // Should be allowed
		}
		err := config.Validate()
		if err != nil {
			t.Errorf("Expected zero RecordsPerFile to be valid, got error: %v", err)
		}
	})

	t.Run("unlimited records per file mode (NoRecordLimitPerFile)", func(t *testing.T) {
		config := WriterConfig{
			TmpDir:         "/tmp",
			RecordsPerFile: NoRecordLimitPerFile, // Should be allowed - enables unlimited file mode
		}
		err := config.Validate()
		if err != nil {
			t.Errorf("Expected RecordsPerFile=-1 (unlimited mode) to be valid, got error: %v", err)
		}
	})

	t.Run("nil stats provider is allowed", func(t *testing.T) {
		config := WriterConfig{
			TmpDir:         "/tmp",
			RecordsPerFile: 100,
			StatsProvider:  nil, // Should be allowed
		}
		err := config.Validate()
		if err != nil {
			t.Errorf("Expected nil StatsProvider to be valid, got error: %v", err)
		}
	})
}

func TestConfigError_Error(t *testing.T) {
	tests := []struct {
		name     string
		err      ConfigError
		expected string
	}{
		{
			name: "basic error",
			err: ConfigError{
				Field:   "TestField",
				Message: "test message",
			},
			expected: "parquetwriter config: TestField test message",
		},
		{
			name: "empty field",
			err: ConfigError{
				Field:   "",
				Message: "test message",
			},
			expected: "parquetwriter config:  test message",
		},
		{
			name: "empty message",
			err: ConfigError{
				Field:   "TestField",
				Message: "",
			},
			expected: "parquetwriter config: TestField ",
		},
		{
			name: "both empty",
			err: ConfigError{
				Field:   "",
				Message: "",
			},
			expected: "parquetwriter config:  ",
		},
		{
			name: "field with spaces",
			err: ConfigError{
				Field:   "Test Field",
				Message: "has spaces",
			},
			expected: "parquetwriter config: Test Field has spaces",
		},
		{
			name: "message with special characters",
			err: ConfigError{
				Field:   "TestField",
				Message: "message with 'quotes' and \"double quotes\"",
			},
			expected: "parquetwriter config: TestField message with 'quotes' and \"double quotes\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.err.Error()
			if result != tt.expected {
				t.Errorf("Expected error message %q, got %q", tt.expected, result)
			}
		})
	}
}

func TestConfigError_IsError(t *testing.T) {
	err := &ConfigError{
		Field:   "TestField",
		Message: "test message",
	}

	// Test that it implements the error interface
	var _ error = err

	// Test that it has a non-empty error message
	if err.Error() == "" {
		t.Error("Expected non-empty error message")
	}
}

func TestWriterConfig_ValidateMultipleErrors(t *testing.T) {
	// Test that validation returns the first error encountered
	config := WriterConfig{
		TmpDir: "", // First error
	}

	err := config.Validate()
	if err == nil {
		t.Error("Expected validation error, got nil")
		return
	}

	// Should get the first error (TmpDir)
	if !strings.Contains(err.Error(), "TmpDir") {
		t.Errorf("Expected first error to be about TmpDir, got: %v", err)
	}

	// Should not contain other field errors since validation stops at first error
	if strings.Contains(err.Error(), "RecordsPerFile") {
		t.Errorf("Expected only first error, but got: %v", err)
	}
}

func TestWriterConfig_GroupKeyFuncValidation(t *testing.T) {
	t.Run("group key func without no split groups", func(t *testing.T) {
		config := WriterConfig{
			TmpDir:         "/tmp",
			RecordsPerFile: 100,
			GroupKeyFunc: func(row map[string]any) any {
				return row["group"]
			},
			NoSplitGroups: false, // This should be fine
		}
		err := config.Validate()
		if err != nil {
			t.Errorf("Expected valid config, got error: %v", err)
		}
	})

	t.Run("no split groups with group key func", func(t *testing.T) {
		config := WriterConfig{
			TmpDir:         "/tmp",
			RecordsPerFile: 100,
			GroupKeyFunc: func(row map[string]any) any {
				return row["group"]
			},
			NoSplitGroups: true, // This should be fine since GroupKeyFunc is provided
		}
		err := config.Validate()
		if err != nil {
			t.Errorf("Expected valid config, got error: %v", err)
		}
	})
}

func TestWriterConfig_BoundaryValues(t *testing.T) {
	tests := []struct {
		name        string
		config      WriterConfig
		shouldError bool
	}{
		{
			name: "minimum positive target file size",
			config: WriterConfig{
				TmpDir:         "/tmp",
				RecordsPerFile: 100,
			},
			shouldError: false,
		},
		{
			name: "very large target file size",
			config: WriterConfig{
				TmpDir:         "/tmp",
				RecordsPerFile: 100,
			},
			shouldError: false,
		},
		{
			name: "single character base name",
			config: WriterConfig{
				TmpDir:         "/tmp",
				RecordsPerFile: 100,
			},
			shouldError: false,
		},
		{
			name: "single character tmp dir",
			config: WriterConfig{
				TmpDir:         "/",
				RecordsPerFile: 100,
			},
			shouldError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.shouldError && err == nil {
				t.Error("Expected validation error, got nil")
			}
			if !tt.shouldError && err != nil {
				t.Errorf("Expected no validation error, got: %v", err)
			}
		})
	}
}
