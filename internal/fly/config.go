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

import "time"

// Config holds the Kafka configuration
type Config struct {
	// Broker configuration
	Brokers []string `mapstructure:"brokers"`

	// SASL authentication
	SASLEnabled   bool   `mapstructure:"sasl_enabled"`
	SASLMechanism string `mapstructure:"sasl_mechanism"` // "PLAIN", "SCRAM-SHA-256" or "SCRAM-SHA-512"
	SASLUsername  string `mapstructure:"sasl_username"`
	SASLPassword  string `mapstructure:"sasl_password"`

	// TLS configuration
	TLSEnabled    bool `mapstructure:"tls_enabled"`
	TLSSkipVerify bool `mapstructure:"tls_skip_verify"`

	// Producer settings
	ProducerBatchSize    int           `mapstructure:"producer_batch_size"`
	ProducerBatchTimeout time.Duration `mapstructure:"producer_batch_timeout"`
	ProducerCompression  string        `mapstructure:"producer_compression"`

	// Consumer settings
	ConsumerGroupPrefix string        `mapstructure:"consumer_group_prefix"`
	ConsumerBatchSize   int           `mapstructure:"consumer_batch_size"`
	ConsumerMaxWait     time.Duration `mapstructure:"consumer_max_wait"`
	ConsumerMinBytes    int           `mapstructure:"consumer_min_bytes"`
	ConsumerMaxBytes    int           `mapstructure:"consumer_max_bytes"`

	// Feature flag
	Enabled bool `mapstructure:"enabled"`
}

// DefaultConfig returns a default configuration
func DefaultConfig() *Config {
	return &Config{
		Brokers: []string{"localhost:9092"},

		SASLEnabled:   false,
		SASLMechanism: "SCRAM-SHA-256",
		SASLUsername:  "",
		SASLPassword:  "",

		TLSEnabled:    false,
		TLSSkipVerify: false,

		ProducerBatchSize:    100,
		ProducerBatchTimeout: 1 * time.Second,
		ProducerCompression:  "snappy",

		ConsumerGroupPrefix: "lakerunner",
		ConsumerBatchSize:   100,
		ConsumerMaxWait:     500 * time.Millisecond,
		ConsumerMinBytes:    10 * 1024,        // 10KB
		ConsumerMaxBytes:    10 * 1024 * 1024, // 10MB

		Enabled: false,
	}
}

// GetConsumerGroup returns the consumer group name for the given service
func (c *Config) GetConsumerGroup(service string) string {
	return c.ConsumerGroupPrefix + "." + service
}
