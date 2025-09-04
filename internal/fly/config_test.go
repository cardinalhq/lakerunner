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

package fly

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
	assert.False(t, cfg.SASLEnabled)
	assert.Equal(t, "SCRAM-SHA-256", cfg.SASLMechanism)
	assert.False(t, cfg.TLSEnabled)
	assert.Equal(t, 100, cfg.ProducerBatchSize)
	assert.Equal(t, 1*time.Second, cfg.ProducerBatchTimeout)
	assert.Equal(t, "snappy", cfg.ProducerCompression)
	assert.Equal(t, "lakerunner", cfg.ConsumerGroupPrefix)
	assert.Equal(t, 100, cfg.ConsumerBatchSize)
	assert.Equal(t, 500*time.Millisecond, cfg.ConsumerMaxWait)
	assert.Equal(t, 10*1024, cfg.ConsumerMinBytes)
	assert.Equal(t, 10*1024*1024, cfg.ConsumerMaxBytes)
	assert.False(t, cfg.Enabled)
	assert.False(t, cfg.EnabledForIngestion)
	assert.False(t, cfg.EnabledForCompaction)
	assert.False(t, cfg.EnabledForRollup)
}

func TestLoadFromEnv(t *testing.T) {
	// Save current environment and restore after test
	savedEnv := make(map[string]string)
	envVars := []string{
		"LAKERUNNER_KAFKA_BROKERS",
		"LAKERUNNER_KAFKA_SASL_ENABLED",
		"LAKERUNNER_KAFKA_SASL_MECHANISM",
		"LAKERUNNER_KAFKA_SASL_USERNAME",
		"LAKERUNNER_KAFKA_SASL_PASSWORD",
		"LAKERUNNER_KAFKA_TLS_ENABLED",
		"LAKERUNNER_KAFKA_TLS_SKIP_VERIFY",
		"LAKERUNNER_KAFKA_PRODUCER_BATCH_SIZE",
		"LAKERUNNER_KAFKA_PRODUCER_BATCH_TIMEOUT",
		"LAKERUNNER_KAFKA_PRODUCER_COMPRESSION",
		"LAKERUNNER_KAFKA_CONSUMER_GROUP_PREFIX",
		"LAKERUNNER_KAFKA_CONSUMER_BATCH_SIZE",
		"LAKERUNNER_KAFKA_CONSUMER_MAX_WAIT",
		"LAKERUNNER_KAFKA_CONSUMER_MIN_BYTES",
		"LAKERUNNER_KAFKA_CONSUMER_MAX_BYTES",
		"LAKERUNNER_KAFKA_ENABLED",
		"LAKERUNNER_KAFKA_ENABLED_INGESTION",
		"LAKERUNNER_KAFKA_ENABLED_COMPACTION",
		"LAKERUNNER_KAFKA_ENABLED_ROLLUP",
	}

	for _, env := range envVars {
		savedEnv[env] = os.Getenv(env)
		os.Unsetenv(env)
	}
	defer func() {
		for env, val := range savedEnv {
			if val != "" {
				os.Setenv(env, val)
			} else {
				os.Unsetenv(env)
			}
		}
	}()

	tests := []struct {
		name   string
		setEnv func()
		verify func(*Config)
	}{
		{
			name: "default values when no env set",
			setEnv: func() {
				// No environment variables set
			},
			verify: func(cfg *Config) {
				assert.Equal(t, []string{"localhost:9092"}, cfg.Brokers)
				assert.False(t, cfg.SASLEnabled)
				assert.False(t, cfg.Enabled)
			},
		},
		{
			name: "brokers from env",
			setEnv: func() {
				os.Setenv("LAKERUNNER_KAFKA_BROKERS", "broker1:9092,broker2:9092,broker3:9092")
			},
			verify: func(cfg *Config) {
				assert.Equal(t, []string{"broker1:9092", "broker2:9092", "broker3:9092"}, cfg.Brokers)
			},
		},
		{
			name: "SASL configuration from env",
			setEnv: func() {
				os.Setenv("LAKERUNNER_KAFKA_SASL_ENABLED", "true")
				os.Setenv("LAKERUNNER_KAFKA_SASL_MECHANISM", "SCRAM-SHA-512")
				os.Setenv("LAKERUNNER_KAFKA_SASL_USERNAME", "testuser")
				os.Setenv("LAKERUNNER_KAFKA_SASL_PASSWORD", "testpass")
			},
			verify: func(cfg *Config) {
				assert.True(t, cfg.SASLEnabled)
				assert.Equal(t, "SCRAM-SHA-512", cfg.SASLMechanism)
				assert.Equal(t, "testuser", cfg.SASLUsername)
				assert.Equal(t, "testpass", cfg.SASLPassword)
			},
		},
		{
			name: "TLS configuration from env",
			setEnv: func() {
				os.Setenv("LAKERUNNER_KAFKA_TLS_ENABLED", "true")
				os.Setenv("LAKERUNNER_KAFKA_TLS_SKIP_VERIFY", "true")
			},
			verify: func(cfg *Config) {
				assert.True(t, cfg.TLSEnabled)
				assert.True(t, cfg.TLSSkipVerify)
			},
		},
		{
			name: "producer settings from env",
			setEnv: func() {
				os.Setenv("LAKERUNNER_KAFKA_PRODUCER_BATCH_SIZE", "200")
				os.Setenv("LAKERUNNER_KAFKA_PRODUCER_BATCH_TIMEOUT", "5s")
				os.Setenv("LAKERUNNER_KAFKA_PRODUCER_COMPRESSION", "lz4")
			},
			verify: func(cfg *Config) {
				assert.Equal(t, 200, cfg.ProducerBatchSize)
				assert.Equal(t, 5*time.Second, cfg.ProducerBatchTimeout)
				assert.Equal(t, "lz4", cfg.ProducerCompression)
			},
		},
		{
			name: "consumer settings from env",
			setEnv: func() {
				os.Setenv("LAKERUNNER_KAFKA_CONSUMER_GROUP_PREFIX", "custom-prefix")
				os.Setenv("LAKERUNNER_KAFKA_CONSUMER_BATCH_SIZE", "50")
				os.Setenv("LAKERUNNER_KAFKA_CONSUMER_MAX_WAIT", "2s")
				os.Setenv("LAKERUNNER_KAFKA_CONSUMER_MIN_BYTES", "1024")
				os.Setenv("LAKERUNNER_KAFKA_CONSUMER_MAX_BYTES", "1048576")
			},
			verify: func(cfg *Config) {
				assert.Equal(t, "custom-prefix", cfg.ConsumerGroupPrefix)
				assert.Equal(t, 50, cfg.ConsumerBatchSize)
				assert.Equal(t, 2*time.Second, cfg.ConsumerMaxWait)
				assert.Equal(t, 1024, cfg.ConsumerMinBytes)
				assert.Equal(t, 1048576, cfg.ConsumerMaxBytes)
			},
		},
		{
			name: "feature flags from env",
			setEnv: func() {
				os.Setenv("LAKERUNNER_KAFKA_ENABLED", "true")
				os.Setenv("LAKERUNNER_KAFKA_ENABLED_INGESTION", "true")
				os.Setenv("LAKERUNNER_KAFKA_ENABLED_COMPACTION", "true")
				os.Setenv("LAKERUNNER_KAFKA_ENABLED_ROLLUP", "true")
			},
			verify: func(cfg *Config) {
				assert.True(t, cfg.Enabled)
				assert.True(t, cfg.EnabledForIngestion)
				assert.True(t, cfg.EnabledForCompaction)
				assert.True(t, cfg.EnabledForRollup)
			},
		},
		{
			name: "invalid numeric values are ignored",
			setEnv: func() {
				os.Setenv("LAKERUNNER_KAFKA_PRODUCER_BATCH_SIZE", "invalid")
				os.Setenv("LAKERUNNER_KAFKA_CONSUMER_BATCH_SIZE", "not-a-number")
				os.Setenv("LAKERUNNER_KAFKA_PRODUCER_BATCH_TIMEOUT", "invalid-duration")
			},
			verify: func(cfg *Config) {
				// Should keep default values when parsing fails
				assert.Equal(t, 100, cfg.ProducerBatchSize)
				assert.Equal(t, 100, cfg.ConsumerBatchSize)
				assert.Equal(t, 1*time.Second, cfg.ProducerBatchTimeout)
			},
		},
		{
			name: "case insensitive boolean parsing",
			setEnv: func() {
				os.Setenv("LAKERUNNER_KAFKA_ENABLED", "TRUE")
				os.Setenv("LAKERUNNER_KAFKA_SASL_ENABLED", "True")
				os.Setenv("LAKERUNNER_KAFKA_TLS_ENABLED", "tRuE")
			},
			verify: func(cfg *Config) {
				assert.True(t, cfg.Enabled)
				assert.True(t, cfg.SASLEnabled)
				assert.True(t, cfg.TLSEnabled)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Clear all env vars
			for _, env := range envVars {
				os.Unsetenv(env)
			}

			// Set test-specific env vars
			tt.setEnv()

			// Load config
			cfg := LoadFromEnv()

			// Verify
			tt.verify(cfg)
		})
	}
}

func TestGetConsumerGroup(t *testing.T) {
	cfg := &Config{
		ConsumerGroupPrefix: "lakerunner",
	}

	tests := []struct {
		service  string
		expected string
	}{
		{"ingest", "lakerunner.ingest"},
		{"compact", "lakerunner.compact"},
		{"rollup", "lakerunner.rollup"},
		{"", "lakerunner."},
		{"service-with-dash", "lakerunner.service-with-dash"},
	}

	for _, tt := range tests {
		t.Run(tt.service, func(t *testing.T) {
			got := cfg.GetConsumerGroup(tt.service)
			assert.Equal(t, tt.expected, got)
		})
	}
}
