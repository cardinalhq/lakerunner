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
	"fmt"
	"maps"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

// Config aggregates configuration for the application.
// Each field is owned by its respective package.
type Config struct {
	Debug       bool              `mapstructure:"debug"`
	Kafka       KafkaConfig       `mapstructure:"kafka"`
	KafkaTopics KafkaTopicsConfig `mapstructure:"kafka_topics"`
	Metrics     MetricsConfig     `mapstructure:"metrics"`
	Logs        LogsConfig        `mapstructure:"logs"`
	Traces      TracesConfig      `mapstructure:"traces"`
	DuckDB      DuckDBConfig      `mapstructure:"duckdb"`
	S3          S3Config          `mapstructure:"s3"`
	Azure       AzureConfig       `mapstructure:"azure"`
	Admin       AdminConfig       `mapstructure:"admin"`
	Scaling     ScalingConfig     `mapstructure:"scaling"`
	PubSub      PubSubConfig      `mapstructure:"pubsub"`
	Expiry      ExpiryConfig      `mapstructure:"expiry"`

	// Derived fields (populated during Load())
	TopicRegistry *TopicRegistry // Kafka topic registry based on prefix
}

type MetricsConfig struct {
	Ingestion IngestionConfig `mapstructure:"ingestion"`
}

type LogsConfig struct {
	Ingestion IngestionConfig `mapstructure:"ingestion"`
}

type TracesConfig struct {
	Ingestion IngestionConfig `mapstructure:"ingestion"`
}

type S3Config struct {
	AccessKeyID     string `mapstructure:"access_key_id"`
	SecretAccessKey string `mapstructure:"secret_access_key"`
	SessionToken    string `mapstructure:"session_token"`
	Region          string `mapstructure:"region"`
	URLStyle        string `mapstructure:"url_style"` // "path" or "vhost"
}

type AzureConfig struct {
	AuthType         string `mapstructure:"auth_type"` // "service_principal" or "connection_string"
	ClientID         string `mapstructure:"client_id"`
	ClientSecret     string `mapstructure:"client_secret"`
	TenantID         string `mapstructure:"tenant_id"`
	ConnectionString string `mapstructure:"connection_string"`
}

type AdminConfig struct {
	InitialAPIKey string `mapstructure:"initial_api_key"`
}

type PubSubConfig struct {
	Dedup PubSubDedupConfig `mapstructure:"dedup"`
}

type PubSubDedupConfig struct {
	RetentionDuration time.Duration `mapstructure:"retention_duration"`
	CleanupBatchSize  int           `mapstructure:"cleanup_batch_size"`
}

type ExpiryConfig struct {
	DefaultMaxAgeDaysLogs    int `mapstructure:"default_max_age_days_logs"`
	DefaultMaxAgeDaysMetrics int `mapstructure:"default_max_age_days_metrics"`
	DefaultMaxAgeDaysTraces  int `mapstructure:"default_max_age_days_traces"`
	BatchSize                int `mapstructure:"batch_size"`
}

// TopicCreationConfig holds configuration for creating Kafka topics
// WARNING: These settings are for topic creation only - never use partition counts in runtime code
type TopicCreationConfig struct {
	PartitionCount    *int           `mapstructure:"partitionCount"`
	ReplicationFactor *int           `mapstructure:"replicationFactor"`
	Options           map[string]any `mapstructure:"options"`
}

type KafkaTopicsConfig struct {
	TopicPrefix string                         `mapstructure:"topicPrefix"` // Topic prefix (default: "lakerunner")
	Defaults    TopicCreationConfig            `mapstructure:"defaults"`    // Default settings for topic creation
	Topics      map[string]TopicCreationConfig `mapstructure:"topics"`      // Per-service-type overrides for topic creation
}

// KafkaTopicsOverrideVersion is the current version for override files
const KafkaTopicsOverrideVersion = 2

// KafkaTopicsOverrideVersionCheck holds just the version field for initial parsing
type KafkaTopicsOverrideVersionCheck struct {
	Version int `yaml:"version"`
}

// TopicCreationOverrideConfig holds configuration for creating Kafka topics in override files
// Uses yaml tags instead of mapstructure tags
type TopicCreationOverrideConfig struct {
	PartitionCount    *int           `yaml:"partitionCount"`
	ReplicationFactor *int           `yaml:"replicationFactor"`
	Options           map[string]any `yaml:"options"`
}

// KafkaTopicsOverrideConfig is the full structure for external YAML override files
type KafkaTopicsOverrideConfig struct {
	Version  int                                    `yaml:"version"`
	Defaults TopicCreationOverrideConfig            `yaml:"defaults"`
	Workers  map[string]TopicCreationOverrideConfig `yaml:"workers"`
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
	TargetFileSizeBytes int64 `mapstructure:"target_file_size_bytes"` // Target file size in bytes for compaction (default: 1048576 = 1MB)
}

// IngestionConfig holds ingestion feature toggles.
type IngestionConfig struct {
	SingleInstanceMode bool `mapstructure:"single_instance_mode"`
}

// GetConsumerGroup returns the consumer group name for the given service
func (c *KafkaConfig) GetConsumerGroup(service string) string {
	return c.ConsumerGroupPrefix + "." + service
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
			Ingestion: IngestionConfig{
				SingleInstanceMode: false,
			},
		},
		DuckDB:  DefaultDuckDBConfig(),
		Scaling: GetDefaultScalingConfig(),
		S3: S3Config{
			AccessKeyID:     "",
			SecretAccessKey: "",
			SessionToken:    "",
			Region:          "",
			URLStyle:        "",
		},
		Azure: AzureConfig{
			AuthType:         "",
			ClientID:         "",
			ClientSecret:     "",
			TenantID:         "",
			ConnectionString: "",
		},
		Logs: LogsConfig{
			Ingestion: IngestionConfig{
				SingleInstanceMode: false,
			},
		},
		Traces: TracesConfig{
			Ingestion: IngestionConfig{
				SingleInstanceMode: false,
			},
		},
		Admin: AdminConfig{
			InitialAPIKey: "",
		},
		PubSub: PubSubConfig{
			Dedup: PubSubDedupConfig{
				RetentionDuration: 24 * time.Hour, // Default 24 hours
				CleanupBatchSize:  1000,           // Default batch size
			},
		},
		KafkaTopics: KafkaTopicsConfig{
			TopicPrefix: "lakerunner", // Default topic prefix
			Defaults: TopicCreationConfig{
				PartitionCount:    intPtr(16),
				ReplicationFactor: intPtr(3),
				Options: map[string]any{
					"cleanup.policy": "delete",
					"retention.ms":   "604800000", // 7 days
				},
			},
		},
		Expiry: ExpiryConfig{
			DefaultMaxAgeDaysLogs:    -1,    // -1 means not configured
			DefaultMaxAgeDaysMetrics: -1,    // -1 means not configured
			DefaultMaxAgeDaysTraces:  -1,    // -1 means not configured
			BatchSize:                20000, // Default batch size for expiry operations
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
	var usedFlyConfig bool

	// Brokers
	if b := v.GetString("kafka.brokers"); b != "" {
		cfg.Kafka.Brokers = strings.Split(b, ",")
	} else if b := v.GetString("fly.brokers"); b != "" {
		cfg.Kafka.Brokers = strings.Split(b, ",")
		usedFlyConfig = true
	}

	// SASL authentication
	if v.IsSet("kafka.sasl_enabled") {
		cfg.Kafka.SASLEnabled = v.GetBool("kafka.sasl_enabled")
	} else if v.IsSet("fly.sasl_enabled") {
		cfg.Kafka.SASLEnabled = v.GetBool("fly.sasl_enabled")
		usedFlyConfig = true
	}

	if u := v.GetString("kafka.sasl_mechanism"); u != "" {
		cfg.Kafka.SASLMechanism = u
	} else if u := v.GetString("fly.sasl_mechanism"); u != "" {
		cfg.Kafka.SASLMechanism = u
		usedFlyConfig = true
	}

	if u := v.GetString("kafka.sasl_username"); u != "" {
		cfg.Kafka.SASLUsername = u
	} else if u := v.GetString("fly.sasl_username"); u != "" {
		cfg.Kafka.SASLUsername = u
		usedFlyConfig = true
	}

	if u := v.GetString("kafka.sasl_password"); u != "" {
		cfg.Kafka.SASLPassword = u
	} else if u := v.GetString("fly.sasl_password"); u != "" {
		cfg.Kafka.SASLPassword = u
		usedFlyConfig = true
	}

	// TLS configuration
	if v.IsSet("kafka.tls_enabled") {
		cfg.Kafka.TLSEnabled = v.GetBool("kafka.tls_enabled")
	} else if v.IsSet("fly.tls_enabled") {
		cfg.Kafka.TLSEnabled = v.GetBool("fly.tls_enabled")
		usedFlyConfig = true
	}

	if v.IsSet("kafka.tls_skip_verify") {
		cfg.Kafka.TLSSkipVerify = v.GetBool("kafka.tls_skip_verify")
	} else if v.IsSet("fly.tls_skip_verify") {
		cfg.Kafka.TLSSkipVerify = v.GetBool("fly.tls_skip_verify")
		usedFlyConfig = true
	}

	// Producer settings
	if v.IsSet("kafka.producer_batch_size") {
		cfg.Kafka.ProducerBatchSize = v.GetInt("kafka.producer_batch_size")
	} else if v.IsSet("fly.producer_batch_size") {
		cfg.Kafka.ProducerBatchSize = v.GetInt("fly.producer_batch_size")
		usedFlyConfig = true
	}

	if v.IsSet("kafka.producer_batch_timeout") {
		cfg.Kafka.ProducerBatchTimeout = v.GetDuration("kafka.producer_batch_timeout")
	} else if v.IsSet("fly.producer_batch_timeout") {
		cfg.Kafka.ProducerBatchTimeout = v.GetDuration("fly.producer_batch_timeout")
		usedFlyConfig = true
	}

	if u := v.GetString("kafka.producer_compression"); u != "" {
		cfg.Kafka.ProducerCompression = u
	} else if u := v.GetString("fly.producer_compression"); u != "" {
		cfg.Kafka.ProducerCompression = u
		usedFlyConfig = true
	}

	// Consumer settings
	if u := v.GetString("kafka.consumer_group_prefix"); u != "" {
		cfg.Kafka.ConsumerGroupPrefix = u
	} else if u := v.GetString("fly.consumer_group_prefix"); u != "" {
		cfg.Kafka.ConsumerGroupPrefix = u
		usedFlyConfig = true
	}

	if v.IsSet("kafka.consumer_batch_size") {
		cfg.Kafka.ConsumerBatchSize = v.GetInt("kafka.consumer_batch_size")
	} else if v.IsSet("fly.consumer_batch_size") {
		cfg.Kafka.ConsumerBatchSize = v.GetInt("fly.consumer_batch_size")
		usedFlyConfig = true
	}

	if v.IsSet("kafka.consumer_max_wait") {
		cfg.Kafka.ConsumerMaxWait = v.GetDuration("kafka.consumer_max_wait")
	} else if v.IsSet("fly.consumer_max_wait") {
		cfg.Kafka.ConsumerMaxWait = v.GetDuration("fly.consumer_max_wait")
		usedFlyConfig = true
	}

	if v.IsSet("kafka.consumer_min_bytes") {
		cfg.Kafka.ConsumerMinBytes = v.GetInt("kafka.consumer_min_bytes")
	} else if v.IsSet("fly.consumer_min_bytes") {
		cfg.Kafka.ConsumerMinBytes = v.GetInt("fly.consumer_min_bytes")
		usedFlyConfig = true
	}

	if v.IsSet("kafka.consumer_max_bytes") {
		cfg.Kafka.ConsumerMaxBytes = v.GetInt("kafka.consumer_max_bytes")
	} else if v.IsSet("fly.consumer_max_bytes") {
		cfg.Kafka.ConsumerMaxBytes = v.GetInt("fly.consumer_max_bytes")
		usedFlyConfig = true
	}

	// Connection settings
	if v.IsSet("kafka.connection_timeout") {
		cfg.Kafka.ConnectionTimeout = v.GetDuration("kafka.connection_timeout")
	} else if v.IsSet("fly.connection_timeout") {
		cfg.Kafka.ConnectionTimeout = v.GetDuration("fly.connection_timeout")
		usedFlyConfig = true
	}

	// Log deprecation warning if any fly.* config was used
	if usedFlyConfig {
		fmt.Fprintf(os.Stderr, "WARNING: fly.* configuration keys are deprecated. Please migrate to kafka.* keys. See documentation for migration guide.\n")
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

// LoadKafkaTopicsOverride loads and validates a Kafka topics override configuration from a file
// This function is separate from the main config loading to avoid blocking service startup
// when override files have issues - only topic configuration operations will fail
func LoadKafkaTopicsOverride(filename string) (*KafkaTopicsOverrideConfig, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read kafka topics override file: %w", err)
	}

	// First, check version with lenient parsing
	var versionCheck KafkaTopicsOverrideVersionCheck
	if err := yaml.Unmarshal(data, &versionCheck); err != nil {
		return nil, fmt.Errorf("failed to parse version from kafka topics override file: %w", err)
	}

	// Validate version
	if versionCheck.Version != KafkaTopicsOverrideVersion {
		return nil, fmt.Errorf("unsupported kafka topics override file version %d, expected version %d",
			versionCheck.Version, KafkaTopicsOverrideVersion)
	}

	// Now parse the full config with strict mode
	var config KafkaTopicsOverrideConfig
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true) // Enable strict mode - fail on unknown fields

	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to parse kafka topics override file (strict mode): %w", err)
	}

	return &config, nil
}

// convertTopicCreationOverrideConfig converts from override config to regular config
func convertTopicCreationOverrideConfig(override TopicCreationOverrideConfig) TopicCreationConfig {
	return TopicCreationConfig(override)
}

// MergeKafkaTopicsOverride merges an override config into the base KafkaTopicsConfig
func MergeKafkaTopicsOverride(base KafkaTopicsConfig, override *KafkaTopicsOverrideConfig) KafkaTopicsConfig {
	result := KafkaTopicsConfig{
		TopicPrefix: base.TopicPrefix,
		Defaults:    base.Defaults,
		Topics:      make(map[string]TopicCreationConfig),
	}

	maps.Copy(result.Topics, base.Topics)

	// Merge defaults (override takes precedence for non-nil values)
	if override.Defaults.PartitionCount != nil {
		result.Defaults.PartitionCount = override.Defaults.PartitionCount
	}
	if override.Defaults.ReplicationFactor != nil {
		result.Defaults.ReplicationFactor = override.Defaults.ReplicationFactor
	}
	if len(override.Defaults.Options) > 0 {
		if result.Defaults.Options == nil {
			result.Defaults.Options = make(map[string]any)
		}
		maps.Copy(result.Defaults.Options, override.Defaults.Options)
	}

	// Merge per-topic configs (override completely replaces base for each topic)
	for topicKey, topicConfig := range override.Workers {
		result.Topics[topicKey] = convertTopicCreationOverrideConfig(topicConfig)
	}

	return result
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
