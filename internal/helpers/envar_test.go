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

	"github.com/stretchr/testify/assert"
)

func TestGetBoolEnv(t *testing.T) {
	testEnvVar := "TEST_BOOL_ENV_VAR"

	// Clean up environment variable before and after tests
	originalValue := os.Getenv(testEnvVar)
	defer func() {
		if originalValue == "" {
			os.Unsetenv(testEnvVar)
		} else {
			os.Setenv(testEnvVar, originalValue)
		}
	}()

	tests := []struct {
		name         string
		envValue     string
		defaultValue bool
		expected     bool
		setEnv       bool // whether to set the environment variable at all
	}{
		// True values
		{"true lowercase", "true", false, true, true},
		{"true uppercase", "TRUE", false, true, true},
		{"true mixed case", "True", false, true, true},
		{"1", "1", false, true, true},
		{"yes lowercase", "yes", false, true, true},
		{"yes uppercase", "YES", false, true, true},
		{"on lowercase", "on", false, true, true},
		{"on uppercase", "ON", false, true, true},
		{"enable lowercase", "enable", false, true, true},
		{"enable uppercase", "ENABLE", false, true, true},
		{"enabled lowercase", "enabled", false, true, true},
		{"enabled uppercase", "ENABLED", false, true, true},

		// False values
		{"false lowercase", "false", true, false, true},
		{"false uppercase", "FALSE", true, false, true},
		{"false mixed case", "False", true, false, true},
		{"0", "0", true, false, true},
		{"no lowercase", "no", true, false, true},
		{"no uppercase", "NO", true, false, true},
		{"off lowercase", "off", true, false, true},
		{"off uppercase", "OFF", true, false, true},
		{"disable lowercase", "disable", true, false, true},
		{"disable uppercase", "DISABLE", true, false, true},
		{"disabled lowercase", "disabled", true, false, true},
		{"disabled uppercase", "DISABLED", true, false, true},

		// Whitespace handling
		{"true with spaces", "  true  ", false, true, true},
		{"false with tabs", "\tfalse\t", true, false, true},
		{"enabled with mixed whitespace", " \t enabled \n", false, true, true},

		// Empty string cases
		{"empty string with default true", "", true, true, true},
		{"empty string with default false", "", false, false, true},

		// Unset environment variable cases
		{"unset with default true", "", true, true, false},
		{"unset with default false", "", false, false, false},

		// Backward compatibility - any other non-empty value should be true
		{"random string", "random", false, true, true},
		{"number 2", "2", false, true, true},
		{"mixed text", "some_value", false, true, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clean up before each test
			os.Unsetenv(testEnvVar)

			if tt.setEnv {
				os.Setenv(testEnvVar, tt.envValue)
			}

			result := GetBoolEnv(testEnvVar, tt.defaultValue)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetBoolEnv_DefaultValueBehavior(t *testing.T) {
	testEnvVar := "TEST_BOOL_DEFAULT_VAR"

	// Ensure the environment variable is not set
	os.Unsetenv(testEnvVar)

	// Test with default true
	result := GetBoolEnv(testEnvVar, true)
	assert.True(t, result, "Should return default value (true) when env var is unset")

	// Test with default false
	result = GetBoolEnv(testEnvVar, false)
	assert.False(t, result, "Should return default value (false) when env var is unset")
}

func TestGetBoolEnv_BackwardCompatibility(t *testing.T) {
	testEnvVar := "TEST_BOOL_COMPAT_VAR"

	// Clean up
	defer os.Unsetenv(testEnvVar)

	// Test various non-standard values that should all be true for backward compatibility
	backwardCompatValues := []string{
		"anything",
		"123",
		"maybe",
		"sure",
		"yep",
		"whatever",
	}

	for _, value := range backwardCompatValues {
		t.Run("backward_compat_"+value, func(t *testing.T) {
			os.Setenv(testEnvVar, value)
			result := GetBoolEnv(testEnvVar, false)
			assert.True(t, result, "Non-standard value %q should be treated as true for backward compatibility", value)
		})
	}
}
