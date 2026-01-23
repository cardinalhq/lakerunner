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
	"os"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/cardinalhq/lakerunner/config"
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
	client *kafka.Client
}

// NewAdminClient creates a new Kafka admin client
func NewAdminClient(cfg *config.KafkaConfig) (*AdminClient, error) {
	factory := NewFactory(cfg)
	client, err := factory.CreateKafkaClient()
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka client: %w", err)
	}
	return &AdminClient{client: client}, nil
}

// GetTopicInfo retrieves information about a specific topic
func (a *AdminClient) GetTopicInfo(ctx context.Context, topic string) (*TopicInfo, error) {
	client := a.client

	// Get metadata for all topics to find our target topic with retry
	var targetTopic *kafka.Topic
	maxRetries := 3
	for attempt := range maxRetries {
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

	// Use client to get committed offsets
	client := a.client

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
	// Use Kafka client
	client := a.client

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

// topicPartitionInfo holds partition information for a topic
type topicPartitionInfo struct {
	Topic      string
	Partitions []int
}

// highWaterMarkMap maps topic -> partition -> high water mark
type highWaterMarkMap map[string]map[int]int64

// fetchTopicMetadata retrieves topic metadata and partition information
func (a *AdminClient) fetchTopicMetadata(ctx context.Context, client *kafka.Client, topics []string) ([]topicPartitionInfo, map[string][]kafka.OffsetRequest, error) {
	var topicInfo []topicPartitionInfo
	listOffsetReqs := make(map[string][]kafka.OffsetRequest)

	for _, topic := range topics {
		// Get metadata to find partitions
		resp, err := client.Metadata(ctx, &kafka.MetadataRequest{
			Topics: []string{topic},
		})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: failed to get metadata for topic %s: %v\n", topic, err)
			continue
		}

		// Find the topic in response
		var found bool
		for _, t := range resp.Topics {
			if t.Name == topic && t.Error == nil && !t.Internal {
				partitions := make([]int, 0, len(t.Partitions))
				offsetReqs := make([]kafka.OffsetRequest, 0, len(t.Partitions))

				for _, p := range t.Partitions {
					partitions = append(partitions, p.ID)
					offsetReqs = append(offsetReqs, kafka.LastOffsetOf(p.ID))
				}

				topicInfo = append(topicInfo, topicPartitionInfo{
					Topic:      topic,
					Partitions: partitions,
				})
				listOffsetReqs[topic] = offsetReqs
				found = true
				break
			}
		}

		if !found {
			fmt.Fprintf(os.Stderr, "Warning: topic %s not found\n", topic)
		}
	}

	return topicInfo, listOffsetReqs, nil
}

// fetchHighWaterMarks retrieves high water marks for all topic partitions
func (a *AdminClient) fetchHighWaterMarks(ctx context.Context, client *kafka.Client, listOffsetReqs map[string][]kafka.OffsetRequest) (highWaterMarkMap, error) {
	highWaterMarks := make(highWaterMarkMap)

	if len(listOffsetReqs) == 0 {
		return highWaterMarks, nil
	}

	listOffsetsResp, err := client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: listOffsetReqs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list offsets: %w", err)
	}

	// Store high water marks
	for topic, partitionOffsets := range listOffsetsResp.Topics {
		if highWaterMarks[topic] == nil {
			highWaterMarks[topic] = make(map[int]int64)
		}
		for _, po := range partitionOffsets {
			if po.Error == nil {
				highWaterMarks[topic][po.Partition] = po.LastOffset
			}
		}
	}

	return highWaterMarks, nil
}

// fetchConsumerGroupOffsets retrieves committed offsets for a single consumer group
func (a *AdminClient) fetchConsumerGroupOffsets(ctx context.Context, client *kafka.Client, groupID string, topicInfo []topicPartitionInfo) (map[string][]kafka.OffsetFetchPartition, error) {
	// Build request for all topics and partitions for this group
	offsetFetchTopics := make(map[string][]int)
	for _, info := range topicInfo {
		offsetFetchTopics[info.Topic] = info.Partitions
	}

	if len(offsetFetchTopics) == 0 {
		return nil, nil
	}

	// Fetch all offsets for this group in one request
	offsetResp, err := client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: groupID,
		Topics:  offsetFetchTopics,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch offsets for group %s: %w", groupID, err)
	}

	return offsetResp.Topics, nil
}

// calculateLag computes the lag for a partition given committed offset and high water mark
func calculateLag(committedOffset, highWaterMark int64) int64 {
	if committedOffset >= 0 {
		return max(highWaterMark-committedOffset, 0)
	}
	// If no committed offset, lag is the total messages in partition
	return highWaterMark
}

// processConsumerGroupLags processes offset responses and creates ConsumerGroupInfo records
func (a *AdminClient) processConsumerGroupLags(groupID string, offsetTopics map[string][]kafka.OffsetFetchPartition, highWaterMarks highWaterMarkMap) []ConsumerGroupInfo {
	var result []ConsumerGroupInfo

	for topic, partitionOffsets := range offsetTopics {
		hwmMap, exists := highWaterMarks[topic]
		if !exists {
			continue
		}

		for _, po := range partitionOffsets {
			// Skip if there's an error
			if po.Error != nil {
				continue
			}

			// Skip if the group has never consumed from this topic/partition
			// (CommittedOffset of -1 means no offset has been committed)
			if po.CommittedOffset < 0 {
				continue
			}

			hwm, exists := hwmMap[po.Partition]
			if !exists {
				continue
			}

			lag := calculateLag(po.CommittedOffset, hwm)

			result = append(result, ConsumerGroupInfo{
				GroupID:         groupID,
				Topic:           topic,
				Partition:       po.Partition,
				CommittedOffset: po.CommittedOffset,
				HighWaterMark:   hwm,
				Lag:             lag,
			})
		}
	}

	return result
}

// GetAllConsumerGroupLags efficiently retrieves lag information for multiple groups and topics in batch
func (a *AdminClient) GetAllConsumerGroupLags(ctx context.Context, topics []string, groups []string) ([]ConsumerGroupInfo, error) {
	// Use client
	client := a.client

	// Fetch topic metadata and build offset requests
	topicInfo, listOffsetReqs, err := a.fetchTopicMetadata(ctx, client, topics)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch topic metadata: %w", err)
	}

	// Batch fetch all high water marks
	highWaterMarks, err := a.fetchHighWaterMarks(ctx, client, listOffsetReqs)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch high water marks: %w", err)
	}

	// Fetch committed offsets for all groups and process results
	var result []ConsumerGroupInfo
	for _, groupID := range groups {
		offsetTopics, err := a.fetchConsumerGroupOffsets(ctx, client, groupID, topicInfo)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
			continue
		}
		if offsetTopics == nil {
			continue
		}

		groupLags := a.processConsumerGroupLags(groupID, offsetTopics, highWaterMarks)
		result = append(result, groupLags...)
	}

	return result, nil
}

// partitionOffset represents an offset for a specific partition
type partitionOffset struct {
	Partition int
	Offset    int64
}

// CommitConsumerGroupOffsets commits offsets for a consumer group
func (a *AdminClient) CommitConsumerGroupOffsets(ctx context.Context, groupID, topic string, offsets []partitionOffset) error {
	if len(offsets) == 0 {
		return nil
	}

	// Build the offset commit request
	topicOffsets := make([]kafka.OffsetCommit, 0, len(offsets))
	for _, po := range offsets {
		topicOffsets = append(topicOffsets, kafka.OffsetCommit{
			Partition: po.Partition,
			Offset:    po.Offset,
		})
	}

	req := &kafka.OffsetCommitRequest{
		GroupID: groupID,
		Topics: map[string][]kafka.OffsetCommit{
			topic: topicOffsets,
		},
	}

	resp, err := a.client.OffsetCommit(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to commit offsets: %w", err)
	}

	// Check for errors in the response
	if topicResp, exists := resp.Topics[topic]; exists {
		for _, partitionResp := range topicResp {
			if partitionResp.Error != nil {
				return fmt.Errorf("failed to commit offset for partition %d: %w", partitionResp.Partition, partitionResp.Error)
			}
		}
	}

	return nil
}

// GetTopicsForConsumerGroup returns the topics that a consumer group has offsets for
func (a *AdminClient) GetTopicsForConsumerGroup(ctx context.Context, groupID string) ([]string, error) {
	// Get metadata for all topics
	resp, err := a.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to get Kafka metadata: %w", err)
	}

	// Collect all non-internal topics
	var topics []string
	for _, t := range resp.Topics {
		if t.Error == nil && !t.Internal {
			topics = append(topics, t.Name)
		}
	}

	// Check which topics this consumer group has offsets for
	var result []string
	for _, topic := range topics {
		lags, err := a.GetConsumerGroupLag(ctx, topic, groupID)
		if err != nil {
			continue
		}
		// If any partition has a committed offset >= 0, the group has consumed from this topic
		for _, lag := range lags {
			if lag.CommittedOffset >= 0 {
				result = append(result, topic)
				break
			}
		}
	}

	return result, nil
}
