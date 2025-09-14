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
	"os"
	"strings"
)

// Topic keys for semantic access to topics
const (
	TopicObjstoreIngestLogs     = "objstore.ingest.logs"
	TopicObjstoreIngestMetrics  = "objstore.ingest.metrics"
	TopicObjstoreIngestTraces   = "objstore.ingest.traces"
	TopicSegmentsLogsCompact    = "segments.logs.compact"
	TopicSegmentsMetricsCompact = "segments.metrics.compact"
	TopicSegmentsTracesCompact  = "segments.traces.compact"
	TopicSegmentsMetricsRollup  = "segments.metrics.rollup"
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

	// Initialize all topic specifications with exact current names/groups
	tr.registerTopic(TopicObjstoreIngestLogs, "objstore.ingest.logs", "ingest.logs", "ingest-logs")
	tr.registerTopic(TopicObjstoreIngestMetrics, "objstore.ingest.metrics", "ingest.metrics", "ingest-metrics")
	tr.registerTopic(TopicObjstoreIngestTraces, "objstore.ingest.traces", "ingest.traces", "ingest-traces")
	tr.registerTopic(TopicSegmentsLogsCompact, "segments.logs.compact", "compact.logs", "compact-logs")
	tr.registerTopic(TopicSegmentsMetricsCompact, "segments.metrics.compact", "compact.metrics", "compact-metrics")
	tr.registerTopic(TopicSegmentsTracesCompact, "segments.traces.compact", "compact.traces", "compact-traces")
	tr.registerTopic(TopicSegmentsMetricsRollup, "segments.metrics.rollup", "rollup.metrics", "rollup-metrics")
	tr.registerTopic(TopicBoxerLogsCompact, "boxer.logs.compact", "boxer.logs.compact", "boxer-compact-logs")
	tr.registerTopic(TopicBoxerMetricsCompact, "boxer.metrics.compact", "boxer.metrics.compact", "boxer-compact-metrics")
	tr.registerTopic(TopicBoxerTracesCompact, "boxer.traces.compact", "boxer.traces.compact", "boxer-compact-traces")
	tr.registerTopic(TopicBoxerMetricsRollup, "boxer.metrics.rollup", "boxer.metrics.rollup", "boxer-rollup-metrics")

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

// Global registry instance
var defaultTopicRegistry *TopicRegistry

// DefaultTopicRegistry returns the default global topic registry
func DefaultTopicRegistry() *TopicRegistry {
	if defaultTopicRegistry == nil {
		prefix := os.Getenv("LAKERUNNER_KAFKA_TOPIC_PREFIX")
		if prefix == "" {
			prefix = "lakerunner"
		}
		defaultTopicRegistry = NewTopicRegistry(prefix)
	}
	return defaultTopicRegistry
}

// Backward compatibility - keep original KafkaTopics var for existing code
var KafkaTopics = DefaultTopicRegistry().GetAllTopics()
