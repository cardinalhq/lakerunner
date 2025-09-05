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
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

// TopicInfo contains information about a Kafka topic
type TopicInfo struct {
	Name       string
	Partitions []PartitionInfo
}

// PartitionInfo contains information about a topic partition
type PartitionInfo struct {
	ID            int
	HighWaterMark int64
}

// ConsumerGroupInfo contains information about a consumer group
type ConsumerGroupInfo struct {
	GroupID         string
	Topic           string
	Partition       int
	CommittedOffset int64
	HighWaterMark   int64
	Lag             int64
}

// AdminClient provides Kafka administrative operations
type AdminClient struct {
	factory *Factory
}

// NewAdminClient creates a new Kafka admin client
func NewAdminClient(config *Config) *AdminClient {
	return &AdminClient{factory: NewFactory(config)}
}

// GetTopicInfo retrieves information about a specific topic
func (a *AdminClient) GetTopicInfo(ctx context.Context, topic string) (*TopicInfo, error) {
	// Create Kafka client using centralized factory
	client, err := a.factory.CreateKafkaClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Get metadata for all topics to find our target topic with retry
	fmt.Printf("Info: Getting metadata for topic %s\n", topic)

	var targetTopic *kafka.Topic
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		resp, err := client.Metadata(ctx, &kafka.MetadataRequest{})
		if err != nil {
			return nil, fmt.Errorf("failed to get Kafka metadata: %w", err)
		}

		// Find the specific topic
		for _, t := range resp.Topics {
			if t.Name == topic && t.Error == nil && !t.Internal {
				targetTopic = &t
				break
			}
		}

		if targetTopic != nil {
			break
		}

		// If topic not found and we have retries left, wait and try again
		if attempt < maxRetries-1 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	if targetTopic == nil {
		return nil, fmt.Errorf("topic %s not found", topic)
	}

	fmt.Printf("Info: Found %d partitions for topic %s\n", len(targetTopic.Partitions), topic)

	topicInfo := &TopicInfo{
		Name:       topic,
		Partitions: make([]PartitionInfo, 0, len(targetTopic.Partitions)),
	}

	// Get high water mark for each partition using authenticated client
	for _, partition := range targetTopic.Partitions {
		// Use ListOffsets API through the authenticated client to get high water mark
		req := &kafka.ListOffsetsRequest{
			Topics: map[string][]kafka.OffsetRequest{
				topic: {kafka.LastOffsetOf(partition.ID)},
			},
		}

		resp, err := client.ListOffsets(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("failed to list offsets for %s:%d: %w", topic, partition.ID, err)
		}

		// Extract the high water mark from the response
		var highWaterMark int64 = 0
		if topicOffsets, exists := resp.Topics[topic]; exists {
			for _, partitionOffset := range topicOffsets {
				if partitionOffset.Partition == partition.ID {
					if partitionOffset.Error != nil {
						return nil, fmt.Errorf("failed to get offset for %s:%d: %w", topic, partition.ID, partitionOffset.Error)
					}
					highWaterMark = partitionOffset.LastOffset
					break
				}
			}
		}

		topicInfo.Partitions = append(topicInfo.Partitions, PartitionInfo{
			ID:            partition.ID,
			HighWaterMark: highWaterMark,
		})
	}

	return topicInfo, nil
}

// GetConsumerGroupLag retrieves lag information for a consumer group on a specific topic
func (a *AdminClient) GetConsumerGroupLag(ctx context.Context, topic, groupID string) ([]ConsumerGroupInfo, error) {
	// First get topic information
	topicInfo, err := a.GetTopicInfo(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("failed to get topic info: %w", err)
	}

	var result []ConsumerGroupInfo

	// Create authenticated client to get committed offsets
	client, err := a.factory.CreateKafkaClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Get committed offsets for all partitions using OffsetFetch API
	req := &kafka.OffsetFetchRequest{
		GroupID: groupID,
		Topics: map[string][]int{
			topic: make([]int, len(topicInfo.Partitions)),
		},
	}

	// Add all partition IDs to the request
	for i, partition := range topicInfo.Partitions {
		req.Topics[topic][i] = partition.ID
	}

	resp, err := client.OffsetFetch(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch committed offsets for group %s: %w", groupID, err)
	}

	// Process results for each partition
	for _, partition := range topicInfo.Partitions {
		committedOffset := int64(-1)

		// Find the committed offset for this partition
		if topicOffsets, exists := resp.Topics[topic]; exists {
			for _, partitionOffset := range topicOffsets {
				if partitionOffset.Partition == partition.ID {
					if partitionOffset.Error != nil {
						// If there's an error getting the offset, it might mean no commits yet
						committedOffset = int64(-1)
					} else {
						committedOffset = partitionOffset.CommittedOffset
					}
					break
				}
			}
		}

		// Calculate lag
		lag := int64(0)
		if committedOffset >= 0 {
			lag = max(partition.HighWaterMark-committedOffset, 0)
		} else {
			// If no committed offset, lag is the total messages in partition
			lag = partition.HighWaterMark
		}

		result = append(result, ConsumerGroupInfo{
			GroupID:         groupID,
			Topic:           topic,
			Partition:       partition.ID,
			CommittedOffset: committedOffset,
			HighWaterMark:   partition.HighWaterMark,
			Lag:             lag,
		})
	}

	return result, nil
}

// GetMultipleConsumerGroupLag retrieves lag information for multiple topic/group combinations
func (a *AdminClient) GetMultipleConsumerGroupLag(ctx context.Context, topicGroups map[string]string) ([]ConsumerGroupInfo, error) {
	var allLags []ConsumerGroupInfo

	for topic, groupID := range topicGroups {
		lags, err := a.GetConsumerGroupLag(ctx, topic, groupID)
		if err != nil {
			// Continue on error but log it
			fmt.Printf("Warning: failed to get lag for topic %s, group %s: %v\n", topic, groupID, err)
			continue
		}
		allLags = append(allLags, lags...)
	}

	return allLags, nil
}

// TopicExists checks if a topic exists
func (a *AdminClient) TopicExists(ctx context.Context, topic string) (bool, error) {
	// Create Kafka client using centralized factory
	client, err := a.factory.CreateKafkaClient()
	if err != nil {
		return false, fmt.Errorf("failed to create Kafka client: %w", err)
	}

	// Get metadata for all topics to find our target topic
	resp, err := client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return false, fmt.Errorf("failed to get Kafka metadata: %w", err)
	}

	// Check if topic exists
	for _, t := range resp.Topics {
		if t.Name == topic && t.Error == nil && !t.Internal {
			return true, nil
		}
	}

	return false, nil
}
