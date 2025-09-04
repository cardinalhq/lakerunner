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
	"strconv"
	"strings"
	"time"
)

// Config holds the Kafka configuration
type Config struct {
	// Broker configuration
	Brokers []string

	// SASL/SCRAM authentication
	SASLEnabled   bool
	SASLMechanism string // "SCRAM-SHA-256" or "SCRAM-SHA-512"
	SASLUsername  string
	SASLPassword  string

	// TLS configuration
	TLSEnabled    bool
	TLSSkipVerify bool

	// Producer settings
	ProducerBatchSize    int
	ProducerBatchTimeout time.Duration
	ProducerCompression  string

	// Consumer settings
	ConsumerGroupPrefix string
	ConsumerBatchSize   int
	ConsumerMaxWait     time.Duration
	ConsumerMinBytes    int
	ConsumerMaxBytes    int

	// Feature flags
	Enabled              bool
	EnabledForIngestion  bool
	EnabledForCompaction bool
	EnabledForRollup     bool
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

		Enabled:              false,
		EnabledForIngestion:  false,
		EnabledForCompaction: false,
		EnabledForRollup:     false,
	}
}

// LoadFromEnv loads configuration from environment variables
func LoadFromEnv() *Config {
	cfg := DefaultConfig()

	// Broker configuration
	if brokers := os.Getenv("LAKERUNNER_KAFKA_BROKERS"); brokers != "" {
		cfg.Brokers = strings.Split(brokers, ",")
	}

	// SASL/SCRAM authentication
	if enabled := os.Getenv("LAKERUNNER_KAFKA_SASL_ENABLED"); enabled != "" {
		cfg.SASLEnabled = strings.ToLower(enabled) == "true"
	}
	if mechanism := os.Getenv("LAKERUNNER_KAFKA_SASL_MECHANISM"); mechanism != "" {
		cfg.SASLMechanism = mechanism
	}
	if username := os.Getenv("LAKERUNNER_KAFKA_SASL_USERNAME"); username != "" {
		cfg.SASLUsername = username
	}
	if password := os.Getenv("LAKERUNNER_KAFKA_SASL_PASSWORD"); password != "" {
		cfg.SASLPassword = password
	}

	// TLS configuration
	if enabled := os.Getenv("LAKERUNNER_KAFKA_TLS_ENABLED"); enabled != "" {
		cfg.TLSEnabled = strings.ToLower(enabled) == "true"
	}
	if skipVerify := os.Getenv("LAKERUNNER_KAFKA_TLS_SKIP_VERIFY"); skipVerify != "" {
		cfg.TLSSkipVerify = strings.ToLower(skipVerify) == "true"
	}

	// Producer settings
	if size := os.Getenv("LAKERUNNER_KAFKA_PRODUCER_BATCH_SIZE"); size != "" {
		if v, err := strconv.Atoi(size); err == nil {
			cfg.ProducerBatchSize = v
		}
	}
	if timeout := os.Getenv("LAKERUNNER_KAFKA_PRODUCER_BATCH_TIMEOUT"); timeout != "" {
		if d, err := time.ParseDuration(timeout); err == nil {
			cfg.ProducerBatchTimeout = d
		}
	}
	if compression := os.Getenv("LAKERUNNER_KAFKA_PRODUCER_COMPRESSION"); compression != "" {
		cfg.ProducerCompression = compression
	}

	// Consumer settings
	if prefix := os.Getenv("LAKERUNNER_KAFKA_CONSUMER_GROUP_PREFIX"); prefix != "" {
		cfg.ConsumerGroupPrefix = prefix
	}
	if size := os.Getenv("LAKERUNNER_KAFKA_CONSUMER_BATCH_SIZE"); size != "" {
		if v, err := strconv.Atoi(size); err == nil {
			cfg.ConsumerBatchSize = v
		}
	}
	if wait := os.Getenv("LAKERUNNER_KAFKA_CONSUMER_MAX_WAIT"); wait != "" {
		if d, err := time.ParseDuration(wait); err == nil {
			cfg.ConsumerMaxWait = d
		}
	}
	if minBytes := os.Getenv("LAKERUNNER_KAFKA_CONSUMER_MIN_BYTES"); minBytes != "" {
		if v, err := strconv.Atoi(minBytes); err == nil {
			cfg.ConsumerMinBytes = v
		}
	}
	if maxBytes := os.Getenv("LAKERUNNER_KAFKA_CONSUMER_MAX_BYTES"); maxBytes != "" {
		if v, err := strconv.Atoi(maxBytes); err == nil {
			cfg.ConsumerMaxBytes = v
		}
	}

	// Feature flags
	if enabled := os.Getenv("LAKERUNNER_KAFKA_ENABLED"); enabled != "" {
		cfg.Enabled = strings.ToLower(enabled) == "true"
	}
	if enabled := os.Getenv("LAKERUNNER_KAFKA_ENABLED_INGESTION"); enabled != "" {
		cfg.EnabledForIngestion = strings.ToLower(enabled) == "true"
	}
	if enabled := os.Getenv("LAKERUNNER_KAFKA_ENABLED_COMPACTION"); enabled != "" {
		cfg.EnabledForCompaction = strings.ToLower(enabled) == "true"
	}
	if enabled := os.Getenv("LAKERUNNER_KAFKA_ENABLED_ROLLUP"); enabled != "" {
		cfg.EnabledForRollup = strings.ToLower(enabled) == "true"
	}

	return cfg
}

// GetConsumerGroup returns the consumer group name for the given service
func (c *Config) GetConsumerGroup(service string) string {
	return c.ConsumerGroupPrefix + "." + service
}
