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
	"strings"
)

// Topic keys for semantic access to topics
const (
	TopicObjstoreIngestLogs     = "objstore.ingest.logs"
	TopicObjstoreIngestMetrics  = "objstore.ingest.metrics"
	TopicObjstoreIngestTraces   = "objstore.ingest.traces"
	TopicSegmentsLogsIngest     = "segments.logs.ingest"
	TopicSegmentsMetricsIngest  = "segments.metrics.ingest"
	TopicSegmentsTracesIngest   = "segments.traces.ingest"
	TopicSegmentsLogsCompact    = "segments.logs.compact"
	TopicSegmentsMetricsCompact = "segments.metrics.compact"
	TopicSegmentsTracesCompact  = "segments.traces.compact"
	TopicSegmentsMetricsRollup  = "segments.metrics.rollup"
	TopicBoxerLogsIngest        = "boxer.logs.ingest"
	TopicBoxerMetricsIngest     = "boxer.metrics.ingest"
	TopicBoxerTracesIngest      = "boxer.traces.ingest"
	TopicBoxerLogsCompact       = "boxer.logs.compact"
	TopicBoxerMetricsCompact    = "boxer.metrics.compact"
	TopicBoxerTracesCompact     = "boxer.traces.compact"
	TopicBoxerMetricsRollup     = "boxer.metrics.rollup"
)

// TopicSpec defines metadata for a Kafka topic
type TopicSpec struct {
	Key           string // Internal key for lookups
	Name          string // Full topic name with prefix
	ConsumerGroup string // Consumer group name
	ServiceType   string // Service type for external scaling
}

// ServiceMapping represents a mapping between service types and their Kafka topic/consumer group
type ServiceMapping struct {
	ServiceType   string
	Topic         string
	ConsumerGroup string
}

// TopicRegistry manages all Kafka topic definitions and provides type-safe access
type TopicRegistry struct {
	prefix string
	specs  map[string]TopicSpec
}

// NewTopicRegistry creates a new topic registry with the given prefix
func NewTopicRegistry(prefix string) *TopicRegistry {
	if prefix == "" {
		prefix = "lakerunner"
	}

	tr := &TopicRegistry{
		prefix: prefix,
		specs:  make(map[string]TopicSpec),
	}

	// Workers read from the boxed items, and process them as a single unit.
	tr.registerTopic(TopicSegmentsLogsIngest, "segments.logs.ingest", "segments.logs.ingest", ServiceTypeWorkerIngestLogs)
	tr.registerTopic(TopicSegmentsMetricsIngest, "segments.metrics.ingest", "segments.metrics.ingest", ServiceTypeWorkerIngestMetrics)
	tr.registerTopic(TopicSegmentsTracesIngest, "segments.traces.ingest", "segments.traces.ingest", ServiceTypeWorkerIngestTraces)
	tr.registerTopic(TopicSegmentsLogsCompact, "segments.logs.compact", "compact.logs", ServiceTypeWorkerCompactLogs)
	tr.registerTopic(TopicSegmentsMetricsCompact, "segments.metrics.compact", "compact.metrics", ServiceTypeWorkerCompactMetrics)
	tr.registerTopic(TopicSegmentsTracesCompact, "segments.traces.compact", "compact.traces", ServiceTypeWorkerCompactTraces)
	tr.registerTopic(TopicSegmentsMetricsRollup, "segments.metrics.rollup", "rollup.metrics", ServiceTypeWorkerRollupMetrics)

	// Boxer-ingest services read from objstore topics (raw events from pubsub)
	tr.registerTopic(TopicObjstoreIngestLogs, "objstore.ingest.logs", "boxer.logs.ingest", ServiceTypeBoxerIngestLogs)
	tr.registerTopic(TopicObjstoreIngestMetrics, "objstore.ingest.metrics", "boxer.metrics.ingest", ServiceTypeBoxerIngestMetrics)
	tr.registerTopic(TopicObjstoreIngestTraces, "objstore.ingest.traces", "boxer.traces.ingest", ServiceTypeBoxerIngestTraces)
	tr.registerTopic(TopicBoxerLogsCompact, "boxer.logs.compact", "boxer.logs.compact", ServiceTypeBoxerCompactLogs)
	tr.registerTopic(TopicBoxerMetricsCompact, "boxer.metrics.compact", "boxer.metrics.compact", ServiceTypeBoxerCompactMetrics)
	tr.registerTopic(TopicBoxerTracesCompact, "boxer.traces.compact", "boxer.traces.compact", ServiceTypeBoxerCompactTraces)
	tr.registerTopic(TopicBoxerMetricsRollup, "boxer.metrics.rollup", "boxer.metrics.rollup", ServiceTypeBoxerRollupMetrics)

	return tr
}

// registerTopic adds a topic specification to the registry
func (tr *TopicRegistry) registerTopic(key, suffix, consumerGroupSuffix, serviceType string) {
	tr.specs[key] = TopicSpec{
		Key:           key,
		Name:          fmt.Sprintf("%s.%s", tr.prefix, suffix),
		ConsumerGroup: fmt.Sprintf("%s.%s", tr.prefix, consumerGroupSuffix),
		ServiceType:   serviceType,
	}
}

// GetTopic returns the full topic name for the given key
func (tr *TopicRegistry) GetTopic(key string) string {
	spec, exists := tr.specs[key]
	if !exists {
		panic(fmt.Sprintf("unknown topic key: %s", key))
	}
	return spec.Name
}

// GetConsumerGroup returns the consumer group name for the given topic key
func (tr *TopicRegistry) GetConsumerGroup(key string) string {
	spec, exists := tr.specs[key]
	if !exists {
		panic(fmt.Sprintf("unknown topic key: %s", key))
	}
	return spec.ConsumerGroup
}

// GetTopicByServiceType returns the topic name for a service type
func (tr *TopicRegistry) GetTopicByServiceType(serviceType string) (string, bool) {
	for _, spec := range tr.specs {
		if spec.ServiceType == serviceType {
			return spec.Name, true
		}
	}
	return "", false
}

// GetConsumerGroupByServiceType returns the consumer group for a service type
func (tr *TopicRegistry) GetConsumerGroupByServiceType(serviceType string) (string, bool) {
	for _, spec := range tr.specs {
		if spec.ServiceType == serviceType {
			return spec.ConsumerGroup, true
		}
	}
	return "", false
}

// GetServiceMapping returns the complete mapping for a service type
func (tr *TopicRegistry) GetServiceMapping(serviceType string) (ServiceMapping, bool) {
	for _, spec := range tr.specs {
		if spec.ServiceType == serviceType {
			return ServiceMapping{
				ServiceType:   serviceType,
				Topic:         spec.Name,
				ConsumerGroup: spec.ConsumerGroup,
			}, true
		}
	}
	return ServiceMapping{}, false
}

// GetServiceNameByTopic returns the service name for a given topic name
func (tr *TopicRegistry) GetServiceNameByTopic(topic string) string {
	for _, spec := range tr.specs {
		if spec.Name == topic {
			return spec.ServiceType
		}
	}
	return "unknown"
}

// GetServiceNameByConsumerGroup returns the service name for a given consumer group
func (tr *TopicRegistry) GetServiceNameByConsumerGroup(consumerGroup string) string {
	for _, spec := range tr.specs {
		if spec.ConsumerGroup == consumerGroup {
			return spec.ServiceType
		}
	}
	return "unknown"
}

// GetAllServiceMappings returns all service mappings for external scaling
func (tr *TopicRegistry) GetAllServiceMappings() []ServiceMapping {
	var mappings []ServiceMapping
	for _, spec := range tr.specs {
		mappings = append(mappings, ServiceMapping{
			ServiceType:   spec.ServiceType,
			Topic:         spec.Name,
			ConsumerGroup: spec.ConsumerGroup,
		})
	}
	return mappings
}

// GetAllTopics returns a list of all topic names (for backward compatibility)
func (tr *TopicRegistry) GetAllTopics() []string {
	var topics []string
	for _, spec := range tr.specs {
		topics = append(topics, spec.Name)
	}
	return topics
}

// GetObjstoreIngestTopic dynamically constructs objstore ingest topic names
func (tr *TopicRegistry) GetObjstoreIngestTopic(signal string) string {
	switch strings.ToLower(signal) {
	case "logs":
		return tr.GetTopic(TopicObjstoreIngestLogs)
	case "metrics":
		return tr.GetTopic(TopicObjstoreIngestMetrics)
	case "traces":
		return tr.GetTopic(TopicObjstoreIngestTraces)
	default:
		// Fallback for dynamic construction
		return fmt.Sprintf("%s.objstore.ingest.%s", tr.prefix, signal)
	}
}

// KafkaSyncConfig represents the format expected by kafka-sync tool
type KafkaSyncConfig struct {
	Defaults KafkaSyncDefaults `yaml:"defaults"`
	Topics   []KafkaSyncTopic  `yaml:"topics"`
}

type KafkaSyncDefaults struct {
	PartitionCount    int            `yaml:"partitionCount"`
	ReplicationFactor int            `yaml:"replicationFactor"`
	TopicConfig       map[string]any `yaml:"topicConfig"`
}

type KafkaSyncTopic struct {
	Name              string         `yaml:"name"`
	PartitionCount    int            `yaml:"partitionCount,omitempty"`
	ReplicationFactor int            `yaml:"replicationFactor,omitempty"`
	TopicConfig       map[string]any `yaml:"topicConfig,omitempty"`
}

// GenerateKafkaSyncConfig creates a kafka-sync compatible config from our KafkaTopicsConfig
func (tr *TopicRegistry) GenerateKafkaSyncConfig(config KafkaTopicsConfig) KafkaSyncConfig {
	// Set up defaults
	defaults := KafkaSyncDefaults{
		PartitionCount:    16, // Safe default
		ReplicationFactor: 3,  // Safe default
		TopicConfig:       make(map[string]any),
	}

	// Override with configured defaults
	if config.Defaults.PartitionCount != nil {
		defaults.PartitionCount = *config.Defaults.PartitionCount
	}
	if config.Defaults.ReplicationFactor != nil {
		defaults.ReplicationFactor = *config.Defaults.ReplicationFactor
	}
	if config.Defaults.Options != nil {
		for k, v := range config.Defaults.Options {
			defaults.TopicConfig[k] = v
		}
	}

	// Generate topics list from registry
	var topics []KafkaSyncTopic

	// Get all service mappings to ensure we cover all topics
	serviceMappings := tr.GetAllServiceMappings()

	// Track topics we've already added (handle duplicate topic names)
	addedTopics := make(map[string]bool)

	for _, mapping := range serviceMappings {
		if addedTopics[mapping.Topic] {
			continue // Skip duplicate topic names
		}
		addedTopics[mapping.Topic] = true

		topic := KafkaSyncTopic{
			Name: mapping.Topic,
		}

		// Check if we have specific config for this service type
		if serviceConfig, exists := config.Topics[mapping.ServiceType]; exists {
			if serviceConfig.PartitionCount != nil {
				topic.PartitionCount = *serviceConfig.PartitionCount
			}
			if serviceConfig.ReplicationFactor != nil {
				topic.ReplicationFactor = *serviceConfig.ReplicationFactor
			}
			if len(serviceConfig.Options) > 0 {
				topic.TopicConfig = make(map[string]any)
				for k, v := range serviceConfig.Options {
					topic.TopicConfig[k] = v
				}
			}
		}

		topics = append(topics, topic)
	}

	return KafkaSyncConfig{
		Defaults: defaults,
		Topics:   topics,
	}
}
