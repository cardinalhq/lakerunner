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
	t.Setenv("LAKERUNNER_KAFKA_BROKERS", "broker1:9092,broker2:9092")
	t.Setenv("LAKERUNNER_KAFKA_SASL_ENABLED", "true")
	t.Setenv("LAKERUNNER_KAFKA_SASL_USERNAME", "alice")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, []string{"broker1:9092", "broker2:9092"}, cfg.Kafka.Brokers)
	require.True(t, cfg.Kafka.SASLEnabled)
	require.Equal(t, "alice", cfg.Kafka.SASLUsername)
}

func TestKafkaEnvVars(t *testing.T) {
	t.Setenv("LAKERUNNER_KAFKA_BROKERS", "kafka-broker1:9092,kafka-broker2:9092")
	t.Setenv("LAKERUNNER_KAFKA_SASL_ENABLED", "true")
	t.Setenv("LAKERUNNER_KAFKA_SASL_USERNAME", "kafka-user")
	t.Setenv("LAKERUNNER_KAFKA_CONSUMER_BATCH_SIZE", "200")

	cfg, err := Load()
	require.NoError(t, err)

	require.Equal(t, []string{"kafka-broker1:9092", "kafka-broker2:9092"}, cfg.Kafka.Brokers)
	require.True(t, cfg.Kafka.SASLEnabled)
	require.Equal(t, "kafka-user", cfg.Kafka.SASLUsername)
	require.Equal(t, 200, cfg.Kafka.ConsumerBatchSize)
}
