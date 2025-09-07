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
