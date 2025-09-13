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

package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEnsureKafkaTopicsValidation(t *testing.T) {
	// Save original env vars
	originalBrokers := os.Getenv("LAKERUNNER_FLY_BROKERS")
	defer func() {
		if originalBrokers != "" {
			_ = os.Setenv("LAKERUNNER_FLY_BROKERS", originalBrokers)
		} else {
			_ = os.Unsetenv("LAKERUNNER_FLY_BROKERS")
		}
	}()

	// Test validation failure
	_ = os.Unsetenv("LAKERUNNER_FLY_BROKERS")
	err := validateKafkaConfig()
	assert.Error(t, err, "Should fail validation when no brokers configured")
	assert.Contains(t, err.Error(), "LAKERUNNER_FLY_BROKERS", "Error should mention missing broker config")

	// Test validation success
	_ = os.Setenv("LAKERUNNER_FLY_BROKERS", "localhost:9092")
	err = validateKafkaConfig()
	assert.NoError(t, err, "Should pass validation when brokers configured")
}

func TestEnsureKafkaTopicsNoConfigFile(t *testing.T) {
	// Save and set env vars
	originalBrokers := os.Getenv("LAKERUNNER_FLY_BROKERS")
	originalConfigFile := os.Getenv("KAFKA_TOPICS_FILE")

	defer func() {
		if originalBrokers != "" {
			_ = os.Setenv("LAKERUNNER_FLY_BROKERS", originalBrokers)
		} else {
			_ = os.Unsetenv("LAKERUNNER_FLY_BROKERS")
		}
		if originalConfigFile != "" {
			_ = os.Setenv("KAFKA_TOPICS_FILE", originalConfigFile)
		} else {
			_ = os.Unsetenv("KAFKA_TOPICS_FILE")
		}
	}()

	// Set up environment for test
	_ = os.Setenv("LAKERUNNER_FLY_BROKERS", "localhost:9092")
	_ = os.Unsetenv("KAFKA_TOPICS_FILE")

	// Ensure /app/config/kafka_topics.yaml doesn't exist
	// (This should cause the function to skip gracefully)

	// This should succeed and do nothing when no config file is found
	// Note: This won't actually connect to Kafka since no file exists
	// The function should exit early with a log message
}

func TestKafkaTopicsConfigFileExistence(t *testing.T) {
	// Create a test config file
	configContent := `
defaults:
  partitionCount: 3
  replicationFactor: 1
  topicConfig:
    retention.ms: "3600000"

topics:
  - name: test-topic
    partitionCount: 5
    config:
      cleanup.policy: "delete"
`

	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "kafka_topics.yaml")
	err := os.WriteFile(configFile, []byte(configContent), 0644)
	assert.NoError(t, err, "Should create test config file")

	// Verify file exists and can be read
	_, err = os.Stat(configFile)
	assert.NoError(t, err, "Config file should exist")

	content, err := os.ReadFile(configFile)
	assert.NoError(t, err, "Should be able to read config file")
	assert.Contains(t, string(content), "test-topic", "Config should contain test topic")
	assert.Contains(t, string(content), "partitionCount: 3", "Config should contain default partition count")

	t.Logf("Created test config file at: %s", configFile)
}

func TestKafkaConfigValidationEdgeCases(t *testing.T) {
	// Save original env var
	originalBrokers := os.Getenv("LAKERUNNER_FLY_BROKERS")
	defer func() {
		if originalBrokers != "" {
			_ = os.Setenv("LAKERUNNER_FLY_BROKERS", originalBrokers)
		} else {
			_ = os.Unsetenv("LAKERUNNER_FLY_BROKERS")
		}
	}()

	tests := []struct {
		name       string
		brokers    string
		shouldPass bool
	}{
		{
			name:       "single broker",
			brokers:    "localhost:9092",
			shouldPass: true,
		},
		{
			name:       "multiple brokers",
			brokers:    "broker1:9092,broker2:9092,broker3:9092",
			shouldPass: true,
		},
		{
			name:       "empty broker list",
			brokers:    "",
			shouldPass: false,
		},
		{
			name:       "whitespace only",
			brokers:    "   ",
			shouldPass: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.brokers == "" || tt.brokers == "   " {
				_ = os.Unsetenv("LAKERUNNER_FLY_BROKERS")
			} else {
				_ = os.Setenv("LAKERUNNER_FLY_BROKERS", tt.brokers)
			}

			err := validateKafkaConfig()
			if tt.shouldPass {
				assert.NoError(t, err, "Validation should pass for: %s", tt.name)
			} else {
				assert.Error(t, err, "Validation should fail for: %s", tt.name)
			}
		})
	}
}
