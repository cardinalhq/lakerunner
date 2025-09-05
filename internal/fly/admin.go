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
	config *Config
}

// NewAdminClient creates a new Kafka admin client
func NewAdminClient(config *Config) *AdminClient {
	return &AdminClient{config: config}
}

// GetTopicInfo retrieves information about a specific topic
func (a *AdminClient) GetTopicInfo(ctx context.Context, topic string) (*TopicInfo, error) {
	// Connect to Kafka
	conn, err := kafka.Dial("tcp", a.config.Brokers[0])
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	// Get topic partitions
	partitions, err := conn.ReadPartitions(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to read partitions for topic %s: %w", topic, err)
	}

	topicInfo := &TopicInfo{
		Name:       topic,
		Partitions: make([]PartitionInfo, 0, len(partitions)),
	}

	// Get high water mark for each partition
	for _, partition := range partitions {
		partConn, err := kafka.DialLeader(ctx, "tcp", a.config.Brokers[0], topic, partition.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to partition leader for %s:%d: %w", topic, partition.ID, err)
		}

		highWaterMark, err := partConn.ReadLastOffset()
		partConn.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read last offset for %s:%d: %w", topic, partition.ID, err)
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

	// Get committed offset for each partition
	for _, partition := range topicInfo.Partitions {
		// Create a temporary reader to get the committed offset
		reader := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   a.config.Brokers,
			Topic:     topic,
			Partition: partition.ID,
			GroupID:   groupID,
		})

		// Get stats which includes offset information
		stats := reader.Stats()
		committedOffset := int64(-1)

		// The Offset field in stats gives us the current position
		if stats.Offset >= 0 {
			committedOffset = stats.Offset
		}

		reader.Close()

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
	conn, err := kafka.Dial("tcp", a.config.Brokers[0])
	if err != nil {
		return false, fmt.Errorf("failed to connect to Kafka: %w", err)
	}
	defer conn.Close()

	_, err = conn.ReadPartitions(topic)
	if err != nil {
		// If we can't read partitions, assume topic doesn't exist
		return false, nil
	}

	return true, nil
}
