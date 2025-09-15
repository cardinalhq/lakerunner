//go:build kafkatest

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
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
)

func TestAdminClient_TopicExists(t *testing.T) {
	existingTopic := fmt.Sprintf("test-topic-exists-%s", uuid.New().String())
	nonExistentTopic := fmt.Sprintf("test-topic-not-exists-%s", uuid.New().String())

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, existingTopic)
	defer kafkaContainer.CleanupAfterTest(t, []string{existingTopic}, []string{})

	// Create admin client
	cfg := &config.KafkaConfig{
		Brokers: []string{kafkaContainer.Broker()},
	}
	adminClient, err := NewAdminClient(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("existing topic returns true", func(t *testing.T) {
		// Produce a message to ensure topic is created
		produceTestMessage(t, kafkaContainer, existingTopic, "key1", "value1")

		exists, err := adminClient.TopicExists(ctx, existingTopic)
		require.NoError(t, err)
		assert.True(t, exists)
	})

	t.Run("non-existent topic returns false", func(t *testing.T) {
		exists, err := adminClient.TopicExists(ctx, nonExistentTopic)
		require.NoError(t, err)
		assert.False(t, exists)
	})
}

func TestAdminClient_GetTopicInfo(t *testing.T) {
	topic := fmt.Sprintf("test-topic-info-%s", uuid.New().String())

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{})

	// Create admin client
	cfg := &config.KafkaConfig{
		Brokers: []string{kafkaContainer.Broker()},
	}
	adminClient, err := NewAdminClient(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("non-existent topic returns error", func(t *testing.T) {
		nonExistentTopic := fmt.Sprintf("test-topic-nonexistent-%s", uuid.New().String())
		_, err := adminClient.GetTopicInfo(ctx, nonExistentTopic)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "not found")
	})

	t.Run("existing topic returns info with correct high water mark", func(t *testing.T) {
		// Produce some test messages
		messageCount := 5
		for i := 0; i < messageCount; i++ {
			produceTestMessage(t, kafkaContainer, topic, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		}

		topicInfo, err := adminClient.GetTopicInfo(ctx, topic)
		require.NoError(t, err)
		require.NotNil(t, topicInfo)

		assert.Equal(t, topic, topicInfo.Name)
		assert.Greater(t, len(topicInfo.Partitions), 0, "Should have at least one partition")

		// Check that high water mark reflects produced messages
		totalMessages := int64(0)
		for _, partition := range topicInfo.Partitions {
			assert.GreaterOrEqual(t, partition.HighWaterMark, int64(0))
			totalMessages += partition.HighWaterMark
		}
		assert.GreaterOrEqual(t, totalMessages, int64(messageCount), "High water mark should reflect produced messages")
	})
}

func TestAdminClient_GetConsumerGroupLag(t *testing.T) {
	topic := fmt.Sprintf("test-consumer-lag-%s", uuid.New().String())
	groupID := fmt.Sprintf("test-group-%d", time.Now().UnixNano())

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, topic)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic}, []string{groupID})

	// Create admin client
	cfg := &config.KafkaConfig{
		Brokers: []string{kafkaContainer.Broker()},
	}
	adminClient, err := NewAdminClient(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Produce messages to create topic and add data
	messageCount := 10
	for i := 0; i < messageCount; i++ {
		produceTestMessage(t, kafkaContainer, topic, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	t.Run("consumer group with no committed offset shows full lag", func(t *testing.T) {
		newGroupID := fmt.Sprintf("new-group-%d", time.Now().UnixNano())
		defer kafkaContainer.CleanupAfterTest(t, []string{}, []string{newGroupID})

		lags, err := adminClient.GetConsumerGroupLag(ctx, topic, newGroupID)
		require.NoError(t, err)
		require.Greater(t, len(lags), 0, "Should have lag info for at least one partition")

		// Verify basic structure
		for _, lag := range lags {
			assert.Equal(t, topic, lag.Topic)
			assert.Equal(t, newGroupID, lag.GroupID)
			assert.GreaterOrEqual(t, lag.Partition, 0)
			assert.Equal(t, int64(-1), lag.CommittedOffset, "Should have no committed offset for new group")
			assert.Greater(t, lag.HighWaterMark, int64(0), "Should have messages in topic")
			assert.Equal(t, lag.HighWaterMark, lag.Lag, "Lag should equal high water mark for new group")
		}
	})

	t.Run("consumer group after consuming shows reduced lag", func(t *testing.T) {
		// Create a consumer and consume some messages
		consumerConfig := kafkaContainer.CreateConsumerConfig(topic, groupID)
		consumer := NewConsumer(consumerConfig)
		defer consumer.Close()

		// Consume some messages (up to half of what we produced)
		consumedCount := 0
		targetConsume := messageCount / 2

		// Create a cancellable context for controlled consumer shutdown
		consumerCtx, cancelConsumer := context.WithCancel(ctx)
		defer cancelConsumer()

		// Run consumer in a goroutine
		consumeErr := make(chan error, 1)
		go func() {
			err := consumer.Consume(consumerCtx, func(ctx context.Context, messages []ConsumedMessage) error {
				for range messages {
					consumedCount++
					if consumedCount >= targetConsume {
						// Cancel the context to stop consuming after this batch
						go func() {
							// Give a moment for the handler to return and commit
							time.Sleep(100 * time.Millisecond)
							cancelConsumer()
						}()
						break
					}
				}
				// Return nil to allow the consumer to commit the messages
				return nil
			})
			consumeErr <- err
		}()

		// Wait for consumer to finish or timeout
		select {
		case err := <-consumeErr:
			// Expect context.Canceled when we stop consuming (may be wrapped)
			assert.True(t, errors.Is(err, context.Canceled) || err == nil, "Consumer should stop gracefully")
		case <-time.After(5 * time.Second):
			cancelConsumer()
			<-consumeErr // Wait for consumer to finish
		}

		// Wait for offsets to be committed with a timeout
		maxWaitTime := 10 * time.Second
		checkInterval := 100 * time.Millisecond
		deadline := time.Now().Add(maxWaitTime)

		var lags []ConsumerGroupInfo
		var lastErr error
		offsetsCommitted := false

		for time.Now().Before(deadline) {
			lags, lastErr = adminClient.GetConsumerGroupLag(ctx, topic, groupID)
			if lastErr != nil {
				time.Sleep(checkInterval)
				continue
			}

			// Check if offsets have been committed
			if len(lags) > 0 {
				allHaveOffsets := true
				for _, lag := range lags {
					if lag.CommittedOffset < 0 {
						allHaveOffsets = false
						break
					}
				}
				if allHaveOffsets {
					offsetsCommitted = true
					break
				}
			}

			time.Sleep(checkInterval)
		}

		require.NoError(t, lastErr, "Failed to get consumer group lag")
		require.True(t, offsetsCommitted, "Offsets should be committed within %v", maxWaitTime)
		require.Greater(t, len(lags), 0, "Should have lag info for at least one partition")

		// Verify that lag is now reduced
		for _, lag := range lags {
			assert.Equal(t, topic, lag.Topic)
			assert.Equal(t, groupID, lag.GroupID)
			assert.GreaterOrEqual(t, lag.Partition, 0)
			assert.GreaterOrEqual(t, lag.CommittedOffset, int64(0), "Should have some committed offset after consuming")
			assert.Greater(t, lag.HighWaterMark, int64(0), "Should have messages in topic")
			assert.LessOrEqual(t, lag.Lag, lag.HighWaterMark, "Lag should be <= total messages")
		}
	})
}

func TestAdminClient_GetMultipleConsumerGroupLag(t *testing.T) {
	topic1 := fmt.Sprintf("test-multi-lag-1-%s", uuid.New().String())
	topic2 := fmt.Sprintf("test-multi-lag-2-%s", uuid.New().String())
	group1 := fmt.Sprintf("test-group-1-%d", time.Now().UnixNano())
	group2 := fmt.Sprintf("test-group-2-%d", time.Now().UnixNano())

	// Use shared Kafka container with both topics
	kafkaContainer := NewKafkaTestContainer(t, topic1, topic2)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic1, topic2}, []string{group1, group2})

	// Create admin client
	cfg := &config.KafkaConfig{
		Brokers: []string{kafkaContainer.Broker()},
	}
	adminClient, err := NewAdminClient(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Produce messages to both topics
	for i := 0; i < 3; i++ {
		produceTestMessage(t, kafkaContainer, topic1, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		produceTestMessage(t, kafkaContainer, topic2, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	t.Run("multiple topics return lag info", func(t *testing.T) {
		topicGroups := map[string]string{
			topic1: group1,
			topic2: group2,
		}

		lags, err := adminClient.GetMultipleConsumerGroupLag(ctx, topicGroups)
		require.NoError(t, err)

		// Should have results for both topics (at least one partition each)
		assert.GreaterOrEqual(t, len(lags), 2, "Should have results for both topics")

		// Check that we have results for both topics
		topicsFound := make(map[string]bool)
		for _, lag := range lags {
			topicsFound[lag.Topic] = true
			assert.Greater(t, lag.HighWaterMark, int64(0), "Should have messages")
			assert.Equal(t, int64(-1), lag.CommittedOffset, "Should have no committed offset")
			assert.Equal(t, lag.HighWaterMark, lag.Lag, "Should show full lag")
		}
		assert.True(t, topicsFound[topic1], "Should have results for topic1")
		assert.True(t, topicsFound[topic2], "Should have results for topic2")
	})

	t.Run("handles non-existent topics gracefully", func(t *testing.T) {
		nonExistentTopic := fmt.Sprintf("non-existent-%s", uuid.New().String())
		topicGroupsWithMissing := map[string]string{
			topic1:           group1,
			nonExistentTopic: "group-missing",
		}

		lags, err := adminClient.GetMultipleConsumerGroupLag(ctx, topicGroupsWithMissing)
		require.NoError(t, err)

		// Should still get results for the existing topic
		assert.Greater(t, len(lags), 0, "Should have results for existing topic")

		// Verify we got results for the existing topic
		foundExisting := false
		for _, lag := range lags {
			if lag.Topic == topic1 {
				foundExisting = true
				break
			}
		}
		assert.True(t, foundExisting, "Should have results for existing topic even when some topics are missing")
	})
}

func TestAdminClient_GetAllConsumerGroupLags(t *testing.T) {
	topic1 := fmt.Sprintf("test-batch-lag-1-%s", uuid.New().String())
	topic2 := fmt.Sprintf("test-batch-lag-2-%s", uuid.New().String())
	topic3 := fmt.Sprintf("test-batch-lag-3-%s", uuid.New().String())
	group1 := fmt.Sprintf("test-batch-group-1-%d", time.Now().UnixNano())
	group2 := fmt.Sprintf("test-batch-group-2-%d", time.Now().UnixNano())
	group3 := fmt.Sprintf("test-batch-group-3-%d", time.Now().UnixNano())

	// Use shared Kafka container with all topics
	kafkaContainer := NewKafkaTestContainer(t, topic1, topic2, topic3)
	defer kafkaContainer.CleanupAfterTest(t, []string{topic1, topic2, topic3}, []string{group1, group2, group3})

	// Create admin client
	cfg := &config.KafkaConfig{
		Brokers: []string{kafkaContainer.Broker()},
	}
	adminClient, err := NewAdminClient(cfg)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Produce messages to all topics
	for i := 0; i < 5; i++ {
		produceTestMessage(t, kafkaContainer, topic1, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		produceTestMessage(t, kafkaContainer, topic2, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
		produceTestMessage(t, kafkaContainer, topic3, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i))
	}

	// Create consumers for different combinations of topics/groups
	// For simpler testing, have each group consume all messages from their topics
	// group1 consumes from topic1 and topic2
	consumeAllMessages(t, kafkaContainer, topic1, group1)
	consumeAllMessages(t, kafkaContainer, topic2, group1)

	// group2 consumes only from topic1
	consumeAllMessages(t, kafkaContainer, topic1, group2)

	// group3 consumes from topic2 and topic3
	consumeAllMessages(t, kafkaContainer, topic2, group3)
	consumeAllMessages(t, kafkaContainer, topic3, group3)

	t.Run("batch fetch all consumer group lags", func(t *testing.T) {
		topics := []string{topic1, topic2, topic3}
		groups := []string{group1, group2, group3}

		lags, err := adminClient.GetAllConsumerGroupLags(ctx, topics, groups)
		require.NoError(t, err)

		// Should have results for multiple topic/group combinations
		assert.Greater(t, len(lags), 0, "Should have lag results")

		// Track which combinations we found
		foundCombinations := make(map[string]bool)
		for _, lag := range lags {
			key := fmt.Sprintf("%s:%s", lag.Topic, lag.GroupID)
			foundCombinations[key] = true

			// Verify lag structure
			assert.GreaterOrEqual(t, lag.Partition, 0)
			assert.GreaterOrEqual(t, lag.HighWaterMark, int64(5), "Should have at least 5 messages per topic")

			// Check that groups that consumed all messages have no lag
			if lag.Topic == topic1 && lag.GroupID == group1 {
				assert.Equal(t, int64(5), lag.CommittedOffset, "group1 should have consumed all messages from topic1")
				assert.Equal(t, int64(0), lag.Lag, "group1 should have no lag for topic1")
			}
			if lag.Topic == topic2 && lag.GroupID == group1 {
				assert.Equal(t, int64(5), lag.CommittedOffset, "group1 should have consumed all messages from topic2")
				assert.Equal(t, int64(0), lag.Lag, "group1 should have no lag for topic2")
			}
			if lag.Topic == topic1 && lag.GroupID == group2 {
				assert.Equal(t, int64(5), lag.CommittedOffset, "group2 should have consumed all messages from topic1")
				assert.Equal(t, int64(0), lag.Lag, "group2 should have no lag for topic1")
			}
			if lag.Topic == topic3 && lag.GroupID == group3 {
				assert.Equal(t, int64(5), lag.CommittedOffset, "group3 should have consumed all messages from topic3")
				assert.Equal(t, int64(0), lag.Lag, "group3 should have no lag for topic3")
			}
		}

		// Verify we got the expected combinations
		assert.True(t, foundCombinations[fmt.Sprintf("%s:%s", topic1, group1)], "Should have group1 consuming topic1")
		assert.True(t, foundCombinations[fmt.Sprintf("%s:%s", topic2, group1)], "Should have group1 consuming topic2")
		assert.True(t, foundCombinations[fmt.Sprintf("%s:%s", topic1, group2)], "Should have group2 consuming topic1")
		assert.True(t, foundCombinations[fmt.Sprintf("%s:%s", topic2, group3)], "Should have group3 consuming topic2")
		assert.True(t, foundCombinations[fmt.Sprintf("%s:%s", topic3, group3)], "Should have group3 consuming topic3")

		// Should NOT have combinations where groups haven't consumed
		assert.False(t, foundCombinations[fmt.Sprintf("%s:%s", topic3, group1)], "group1 should not have consumed topic3")
		assert.False(t, foundCombinations[fmt.Sprintf("%s:%s", topic2, group2)], "group2 should not have consumed topic2")
		assert.False(t, foundCombinations[fmt.Sprintf("%s:%s", topic3, group2)], "group2 should not have consumed topic3")
		assert.False(t, foundCombinations[fmt.Sprintf("%s:%s", topic1, group3)], "group3 should not have consumed topic1")
	})

	t.Run("handles empty topic list", func(t *testing.T) {
		lags, err := adminClient.GetAllConsumerGroupLags(ctx, []string{}, []string{group1})
		require.NoError(t, err)
		assert.Empty(t, lags, "Should return empty result for empty topic list")
	})

	t.Run("handles empty group list", func(t *testing.T) {
		lags, err := adminClient.GetAllConsumerGroupLags(ctx, []string{topic1}, []string{})
		require.NoError(t, err)
		assert.Empty(t, lags, "Should return empty result for empty group list")
	})

	t.Run("handles non-existent topics gracefully", func(t *testing.T) {
		nonExistentTopic := fmt.Sprintf("non-existent-%s", uuid.New().String())
		topics := []string{topic1, nonExistentTopic}
		groups := []string{group1}

		lags, err := adminClient.GetAllConsumerGroupLags(ctx, topics, groups)
		require.NoError(t, err)

		// Should still get results for the existing topic
		foundExisting := false
		for _, lag := range lags {
			if lag.Topic == topic1 {
				foundExisting = true
				break
			}
			// Should not have any results for non-existent topic
			assert.NotEqual(t, nonExistentTopic, lag.Topic)
		}
		assert.True(t, foundExisting, "Should have results for existing topic")
	})

	t.Run("handles non-existent groups gracefully", func(t *testing.T) {
		nonExistentGroup := fmt.Sprintf("non-existent-group-%s", uuid.New().String())
		topics := []string{topic1}
		groups := []string{group1, nonExistentGroup}

		lags, err := adminClient.GetAllConsumerGroupLags(ctx, topics, groups)
		require.NoError(t, err)

		// Should get results for the existing group
		foundExisting := false
		foundNonExistent := false
		for _, lag := range lags {
			if lag.GroupID == group1 {
				foundExisting = true
			}
			// Non-existent group will NOT show up in results
			// since we skip entries with CommittedOffset < 0
			if lag.GroupID == nonExistentGroup {
				foundNonExistent = true
			}
		}
		assert.True(t, foundExisting, "Should have results for existing group")
		assert.False(t, foundNonExistent, "Should not have results for non-existent group")
	})
}

// Helper function to consume all available messages from a topic
func consumeAllMessages(t *testing.T, kafkaContainer *KafkaTestContainer, topic, groupID string) {
	t.Helper()
	config := kafkaContainer.CreateConsumerConfig(topic, groupID)
	consumer := NewConsumer(config)
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	consumedCount := 0
	consumeErr := make(chan error, 1)

	// Run consumer in a goroutine - it will consume until no more messages arrive
	go func() {
		err := consumer.Consume(ctx, func(ctx context.Context, messages []ConsumedMessage) error {
			consumedCount += len(messages)
			// Continue consuming - timeout will stop us when no more messages come
			return nil
		})
		consumeErr <- err
	}()

	// Wait for timeout (meaning no more messages)
	select {
	case err := <-consumeErr:
		// Expect context.DeadlineExceeded when we've consumed everything
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && err != nil {
			require.NoError(t, err)
		}
	case <-time.After(6 * time.Second):
		cancel()
		<-consumeErr
	}

	// Wait for offsets to be committed
	time.Sleep(500 * time.Millisecond)

	t.Logf("Consumed %d messages from topic %s for group %s", consumedCount, topic, groupID)
}

// Helper function to produce a single test message
func produceTestMessage(t *testing.T, kafkaContainer *KafkaTestContainer, topic, key, value string) {
	t.Helper()
	config := kafkaContainer.CreateProducerConfig()
	producer := NewProducer(config)
	defer producer.Close()

	err := producer.Send(context.Background(), topic, Message{
		Key:   []byte(key),
		Value: []byte(value),
	})
	require.NoError(t, err)
}

func TestCalculateLag(t *testing.T) {
	tests := []struct {
		name            string
		committedOffset int64
		highWaterMark   int64
		expectedLag     int64
	}{
		{
			name:            "normal lag calculation",
			committedOffset: 100,
			highWaterMark:   150,
			expectedLag:     50,
		},
		{
			name:            "no lag",
			committedOffset: 100,
			highWaterMark:   100,
			expectedLag:     0,
		},
		{
			name:            "committed ahead of high water mark",
			committedOffset: 150,
			highWaterMark:   100,
			expectedLag:     0, // Should be max(100-150, 0) = 0
		},
		{
			name:            "no committed offset",
			committedOffset: -1,
			highWaterMark:   100,
			expectedLag:     100, // Lag equals high water mark
		},
		{
			name:            "zero high water mark with no committed offset",
			committedOffset: -1,
			highWaterMark:   0,
			expectedLag:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lag := calculateLag(tt.committedOffset, tt.highWaterMark)
			assert.Equal(t, tt.expectedLag, lag)
		})
	}
}

func TestProcessConsumerGroupLags(t *testing.T) {
	adminClient := &AdminClient{}

	highWaterMarks := HighWaterMarkMap{
		"topic1": {
			0: 100,
			1: 200,
		},
		"topic2": {
			0: 50,
		},
	}

	tests := []struct {
		name         string
		groupID      string
		offsetTopics map[string][]kafka.OffsetFetchPartition
		expected     []ConsumerGroupInfo
	}{
		{
			name:    "successful processing",
			groupID: "test-group",
			offsetTopics: map[string][]kafka.OffsetFetchPartition{
				"topic1": {
					{
						Partition:       0,
						CommittedOffset: 90,
						Error:           nil,
					},
					{
						Partition:       1,
						CommittedOffset: 180,
						Error:           nil,
					},
				},
				"topic2": {
					{
						Partition:       0,
						CommittedOffset: 40,
						Error:           nil,
					},
				},
			},
			expected: []ConsumerGroupInfo{
				{
					GroupID:         "test-group",
					Topic:           "topic1",
					Partition:       0,
					CommittedOffset: 90,
					HighWaterMark:   100,
					Lag:             10,
				},
				{
					GroupID:         "test-group",
					Topic:           "topic1",
					Partition:       1,
					CommittedOffset: 180,
					HighWaterMark:   200,
					Lag:             20,
				},
				{
					GroupID:         "test-group",
					Topic:           "topic2",
					Partition:       0,
					CommittedOffset: 40,
					HighWaterMark:   50,
					Lag:             10,
				},
			},
		},
		{
			name:    "with errors and missing partitions",
			groupID: "test-group",
			offsetTopics: map[string][]kafka.OffsetFetchPartition{
				"topic1": {
					{
						Partition:       0,
						CommittedOffset: 90,
						Error:           nil,
					},
					{
						Partition:       2, // Not in high water marks
						CommittedOffset: 10,
						Error:           nil,
					},
				},
				"topic3": { // Topic not in high water marks
					{
						Partition:       0,
						CommittedOffset: 10,
						Error:           nil,
					},
				},
			},
			expected: []ConsumerGroupInfo{
				{
					GroupID:         "test-group",
					Topic:           "topic1",
					Partition:       0,
					CommittedOffset: 90,
					HighWaterMark:   100,
					Lag:             10,
				},
			},
		},
		{
			name:         "empty offset topics",
			groupID:      "test-group",
			offsetTopics: map[string][]kafka.OffsetFetchPartition{},
			expected:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := adminClient.processConsumerGroupLags(tt.groupID, tt.offsetTopics, highWaterMarks)

			// Sort both slices by Topic, then Partition for consistent comparison
			sortConsumerGroupInfo := func(infos []ConsumerGroupInfo) {
				for i := 0; i < len(infos)-1; i++ {
					for j := i + 1; j < len(infos); j++ {
						if infos[i].Topic > infos[j].Topic ||
							(infos[i].Topic == infos[j].Topic && infos[i].Partition > infos[j].Partition) {
							infos[i], infos[j] = infos[j], infos[i]
						}
					}
				}
			}

			sortConsumerGroupInfo(result)
			sortConsumerGroupInfo(tt.expected)

			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTopicPartitionInfo(t *testing.T) {
	info := TopicPartitionInfo{
		Topic:      "test-topic",
		Partitions: []int{0, 1, 2},
	}

	assert.Equal(t, "test-topic", info.Topic)
	assert.Equal(t, []int{0, 1, 2}, info.Partitions)
}

func TestHighWaterMarkMap(t *testing.T) {
	hwm := make(HighWaterMarkMap)
	hwm["topic1"] = make(map[int]int64)
	hwm["topic1"][0] = 100
	hwm["topic1"][1] = 200

	assert.Equal(t, int64(100), hwm["topic1"][0])
	assert.Equal(t, int64(200), hwm["topic1"][1])

	// Test accessing non-existent topic
	_, exists := hwm["non-existent"]
	assert.False(t, exists)
}

// Integration test to ensure the refactored function still behaves the same
func TestGetAllConsumerGroupLags_Refactored(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// This test requires a running Kafka instance
	cfg := &config.KafkaConfig{
		Brokers:     []string{"localhost:9092"},
		SASLEnabled: false,
		TLSEnabled:  false,
	}

	adminClient, err := NewAdminClient(cfg)
	require.NoError(t, err)
	ctx := context.Background()

	// Test with empty inputs
	result, err := adminClient.GetAllConsumerGroupLags(ctx, []string{}, []string{})
	require.NoError(t, err)
	assert.Empty(t, result)

	// Test with non-existent topics and groups
	result, err = adminClient.GetAllConsumerGroupLags(ctx, []string{"non-existent-topic"}, []string{"non-existent-group"})
	require.NoError(t, err)
	assert.Empty(t, result)
}
