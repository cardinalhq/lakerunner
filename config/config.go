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
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// Config aggregates configuration for the application.
// Each field is owned by its respective package.
type Config struct {
	Debug       bool              `mapstructure:"debug"`
	Kafka       KafkaConfig       `mapstructure:"kafka"`
	Metrics     MetricsConfig     `mapstructure:"metrics"`
	Batch       BatchConfig       `mapstructure:"batch"`
	DuckDB      DuckDBConfig      `mapstructure:"duckdb"`
	Logs        LogsConfig        `mapstructure:"logs"`
	Traces      TracesConfig      `mapstructure:"traces"`
	Admin       AdminConfig       `mapstructure:"admin"`
	SegLog      SegLogConfig      `mapstructure:"seglog"`
	KafkaTopics KafkaTopicsConfig `mapstructure:"kafka_topics"`

	// Derived fields (populated during Load())
	TopicRegistry *TopicRegistry // Kafka topic registry based on prefix
}

type MetricsConfig struct {
	Ingestion  IngestionConfig  `mapstructure:"ingestion"`
	Compaction CompactionConfig `mapstructure:"compaction"`
	Rollup     RollupConfig     `mapstructure:"rollup"`
}

type BatchConfig struct {
	TargetSizeBytes int64 `mapstructure:"target_size_bytes"`
	MaxBatchSize    int   `mapstructure:"max_batch_size"`
	MaxTotalSize    int64 `mapstructure:"max_total_size"`
	MaxAgeSeconds   int   `mapstructure:"max_age_seconds"`
	MinBatchSize    int   `mapstructure:"min_batch_size"`
}

type DuckDBConfig struct {
	ExtensionsPath  string `mapstructure:"extensions_path"`
	HTTPFSExtension string `mapstructure:"httpfs_extension"`
}

type LogsConfig struct {
	// Partitions will be auto-determined from Kafka topic
}

type TracesConfig struct {
	// Partitions will be auto-determined from Kafka topic
}

type AdminConfig struct {
	InitialAPIKey string `mapstructure:"initial_api_key"`
}

type SegLogConfig struct {
	Enabled bool `mapstructure:"enabled"` // Enable segment log tracing for debugging operations
}

// TopicCreationConfig holds configuration for creating Kafka topics
// WARNING: These settings are for topic creation only - never use partition counts in runtime code
type TopicCreationConfig struct {
	PartitionCount    *int                   `mapstructure:"partitionCount"`
	ReplicationFactor *int                   `mapstructure:"replicationFactor"`
	Options           map[string]interface{} `mapstructure:"options"`
}

type KafkaTopicsConfig struct {
	TopicPrefix string                            `mapstructure:"topicPrefix"` // Topic prefix (default: "lakerunner")
	Defaults    TopicCreationConfig               `mapstructure:"defaults"`    // Default settings for topic creation
	Topics      map[string]TopicCreationConfig    `mapstructure:"topics"`      // Per-service-type overrides for topic creation
}

// KafkaConfig holds the Kafka configuration (moved from fly package to avoid import cycle)
type KafkaConfig struct {
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

	// Connection settings
	ConnectionTimeout time.Duration `mapstructure:"connection_timeout"`
}

type CompactionConfig struct {
	TargetFileSizeBytes int64         `mapstructure:"target_file_size_bytes"` // Target file size in bytes for compaction (default: 1048576 = 1MB)
	MaxAccumulationTime time.Duration `mapstructure:"max_accumulation_time"`  // Maximum time to accumulate segments before compacting
}

type RollupConfig struct {
	BatchLimit int `mapstructure:"batch_limit"`
}

// IngestionConfig holds ingestion feature toggles.
type IngestionConfig struct {
	ProcessExemplars    bool          `mapstructure:"process_exemplars"`
	SingleInstanceMode  bool          `mapstructure:"single_instance_mode"`
	MaxAccumulationTime time.Duration `mapstructure:"max_accumulation_time"`
}

// DefaultIngestionConfig returns default settings for ingestion.
func DefaultIngestionConfig() IngestionConfig {
	return IngestionConfig{
		ProcessExemplars:    true,
		SingleInstanceMode:  false,
		MaxAccumulationTime: 10 * time.Second,
	}
}

// DefaultKafkaConfig returns default settings for Kafka.
func DefaultKafkaConfig() KafkaConfig {
	return KafkaConfig{
		Brokers: []string{"localhost:9092"},

		SASLEnabled:   false,
		SASLMechanism: "SCRAM-SHA-256",
		SASLUsername:  "",
		SASLPassword:  "",

		TLSEnabled:    false,
		TLSSkipVerify: false,

		ProducerBatchSize:    100,
		ProducerBatchTimeout: 10 * time.Millisecond,
		ProducerCompression:  "snappy",

		ConsumerGroupPrefix: "lakerunner",
		ConsumerBatchSize:   100,
		ConsumerMaxWait:     500 * time.Millisecond,
		ConsumerMinBytes:    10 * 1024,        // 10KB
		ConsumerMaxBytes:    10 * 1024 * 1024, // 10MB

		ConnectionTimeout: 10 * time.Second,
	}
}

// Load reads configuration from files and environment variables.
// Environment variables use the prefix "LAKERUNNER" and the dot character
// in keys is replaced by an underscore. For example, "kafka.brokers" becomes
// "LAKERUNNER_KAFKA_BROKERS".
func Load() (*Config, error) {
	cfg := &Config{
		Debug: false,
		Kafka: DefaultKafkaConfig(),
		Metrics: MetricsConfig{
			Ingestion: DefaultIngestionConfig(),
			Compaction: CompactionConfig{
				TargetFileSizeBytes: TargetFileSize,   // Default target file size
				MaxAccumulationTime: 30 * time.Second, // Default 30 seconds for compaction
			},
			Rollup: RollupConfig{
				BatchLimit: 100,
			},
		},
		Batch: BatchConfig{
			TargetSizeBytes: 100 * 1024 * 1024, // 100MB
			MaxBatchSize:    100,
			MaxTotalSize:    1024 * 1024 * 1024, // 1GB
			MaxAgeSeconds:   300,                // 5 minutes
			MinBatchSize:    1,
		},
		DuckDB: DuckDBConfig{
			ExtensionsPath:  "",
			HTTPFSExtension: "",
		},
		Logs:   LogsConfig{},
		Traces: TracesConfig{},
		Admin: AdminConfig{
			InitialAPIKey: "",
		},
		SegLog: SegLogConfig{
			Enabled: false, // Disabled by default for production
		},
		KafkaTopics: KafkaTopicsConfig{
			TopicPrefix: "lakerunner", // Default topic prefix
			Defaults: TopicCreationConfig{
				PartitionCount:    intPtr(16),
				ReplicationFactor: intPtr(3),
				Options: map[string]interface{}{
					"cleanup.policy": "delete",
					"retention.ms":   "604800000", // 7 days
				},
			},
		},
	}

	v := viper.New()
	v.SetConfigName("config")
	v.AddConfigPath(".")
	v.SetEnvPrefix("LAKERUNNER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	bindEnvs(v, cfg)
	_ = v.ReadInConfig()

	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}
	// Handle Kafka configuration with migration support from fly.* to kafka.*
	if b := v.GetString("kafka.brokers"); b != "" {
		cfg.Kafka.Brokers = strings.Split(b, ",")
	} else if b := v.GetString("fly.brokers"); b != "" {
		// Backward compatibility: fall back to old fly.brokers config
		cfg.Kafka.Brokers = strings.Split(b, ",")
	}

	// Migrate other fly.* configuration with backward compatibility
	if v.IsSet("kafka.sasl_enabled") {
		cfg.Kafka.SASLEnabled = v.GetBool("kafka.sasl_enabled")
	} else if v.IsSet("fly.sasl_enabled") {
		cfg.Kafka.SASLEnabled = v.GetBool("fly.sasl_enabled")
	}

	if u := v.GetString("kafka.sasl_username"); u != "" {
		cfg.Kafka.SASLUsername = u
	} else if u := v.GetString("fly.sasl_username"); u != "" {
		cfg.Kafka.SASLUsername = u
	}

	// Also check DEBUG environment variable (without prefix)
	if os.Getenv("DEBUG") != "" {
		cfg.Debug = true
	}

	// Initialize topic registry based on configured prefix
	topicPrefix := cfg.KafkaTopics.TopicPrefix
	if topicPrefix == "" {
		// Check environment variable for prefix
		topicPrefix = os.Getenv("LAKERUNNER_KAFKA_TOPIC_PREFIX")
		if topicPrefix == "" {
			topicPrefix = "lakerunner" // default prefix
		}
	}
	cfg.TopicRegistry = NewTopicRegistry(topicPrefix)

	return cfg, nil
}

// GetTopicRegistry returns a TopicRegistry configured with this config's prefix
func (c *Config) GetTopicRegistry() *TopicRegistry {
	return NewTopicRegistry(c.KafkaTopics.TopicPrefix)
}

// intPtr returns a pointer to an int value
func intPtr(i int) *int {
	return &i
}

// bindEnvs registers all keys within cfg so that viper will look up
// corresponding environment variables when unmarshalling.
func bindEnvs(v *viper.Viper, cfg any, parts ...string) {
	val := reflect.ValueOf(cfg)
	typ := reflect.TypeOf(cfg)
	if typ.Kind() == reflect.Pointer {
		val = val.Elem()
		typ = typ.Elem()
	}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("mapstructure")
		if tag == "" {
			tag = strings.ToLower(f.Name)
		}
		key := append(parts, tag)
		if f.Type.Kind() == reflect.Struct {
			bindEnvs(v, val.Field(i).Interface(), key...)
			continue
		}
		_ = v.BindEnv(strings.Join(key, "."))
	}
}
