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

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLoadEnvOverride(t *testing.T) {
	t.Setenv("LAKERUNNER_FLY_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("LAKERUNNER_FLY_SASL_ENABLED", "true")
	t.Setenv("LAKERUNNER_FLY_SASL_USERNAME", "alice")
	t.Setenv("LAKERUNNER_METRICS_INGESTION_PROCESS_EXEMPLARS", "false")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.Kafka.Brokers)
	require.True(t, cfg.Kafka.SASLEnabled)
	require.Equal(t, "alice", cfg.Kafka.SASLUsername)
	require.False(t, cfg.Metrics.Ingestion.ProcessExemplars)
}

func TestKafkaEnvarsOverFlyEnvars(t *testing.T) {
	// Test: Both FLY and KAFKA settings are set - KAFKA should win
	t.Run("KafkaPrecedence", func(t *testing.T) {
		// Set up both FLY and KAFKA environment variables with different values
		t.Setenv("LAKERUNNER_FLY_BROKERS", "fly-broker1:9092,fly-broker2:9092")
		t.Setenv("LAKERUNNER_KAFKA_BROKERS", "kafka-broker1:9092,kafka-broker2:9092")

		t.Setenv("LAKERUNNER_FLY_SASL_ENABLED", "false")
		t.Setenv("LAKERUNNER_KAFKA_SASL_ENABLED", "true")

		t.Setenv("LAKERUNNER_FLY_SASL_USERNAME", "fly-user")
		t.Setenv("LAKERUNNER_KAFKA_SASL_USERNAME", "kafka-user")

		t.Setenv("LAKERUNNER_FLY_CONSUMER_BATCH_SIZE", "100")
		t.Setenv("LAKERUNNER_KAFKA_CONSUMER_BATCH_SIZE", "200")

		cfg, err := Load()
		require.NoError(t, err)

		// Verify KAFKA settings take precedence
		require.Equal(t, []string{"kafka-broker1:9092", "kafka-broker2:9092"}, cfg.Kafka.Brokers, "Should use KAFKA_BROKERS, not FLY_BROKERS")
		require.True(t, cfg.Kafka.SASLEnabled, "Should use KAFKA_SASL_ENABLED=true, not FLY_SASL_ENABLED=false")
		require.Equal(t, "kafka-user", cfg.Kafka.SASLUsername, "Should use KAFKA_SASL_USERNAME, not FLY_SASL_USERNAME")
		require.Equal(t, 200, cfg.Kafka.ConsumerBatchSize, "Should use KAFKA_CONSUMER_BATCH_SIZE, not FLY_CONSUMER_BATCH_SIZE")
	})

	// Test: Only FLY settings are set - should work
	t.Run("FlySettingsWork", func(t *testing.T) {
		t.Setenv("LAKERUNNER_FLY_BROKERS", "fly-broker:9092")
		t.Setenv("LAKERUNNER_FLY_SASL_ENABLED", "true")
		t.Setenv("LAKERUNNER_FLY_SASL_USERNAME", "fly-user")

		cfg, err := Load()
		require.NoError(t, err)

		// Verify FLY settings work
		require.Equal(t, []string{"fly-broker:9092"}, cfg.Kafka.Brokers)
		require.True(t, cfg.Kafka.SASLEnabled)
		require.Equal(t, "fly-user", cfg.Kafka.SASLUsername)
	})
}
