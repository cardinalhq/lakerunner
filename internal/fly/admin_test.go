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
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAdminClient_TopicExists(t *testing.T) {
	existingTopic := fmt.Sprintf("test-topic-exists-%s", uuid.New().String())
	nonExistentTopic := fmt.Sprintf("test-topic-not-exists-%s", uuid.New().String())

	// Use shared Kafka container with topic
	kafkaContainer := NewKafkaTestContainer(t, existingTopic)
	defer kafkaContainer.CleanupAfterTest(t, []string{existingTopic}, []string{})

	// Create admin client
	config := &Config{
		Brokers: []string{kafkaContainer.Broker()},
	}
	adminClient := NewAdminClient(config)

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
	config := &Config{
		Brokers: []string{kafkaContainer.Broker()},
	}
	adminClient := NewAdminClient(config)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("non-existent topic returns error", func(t *testing.T) {
		nonExistentTopic := fmt.Sprintf("test-topic-nonexistent-%s", uuid.New().String())
		_, err := adminClient.GetTopicInfo(ctx, nonExistentTopic)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read partitions")
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
	config := &Config{
		Brokers: []string{kafkaContainer.Broker()},
	}
	adminClient := NewAdminClient(config)

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

		err := consumer.Consume(ctx, func(ctx context.Context, messages []ConsumedMessage) error {
			for range messages {
				consumedCount++
				if consumedCount >= targetConsume {
					break
				}
			}
			// Stop consuming when we've consumed enough
			if consumedCount >= targetConsume {
				return context.Canceled
			}
			return nil
		})

		// Expect context.Canceled when we stop consuming
		assert.True(t, err == context.Canceled || err == nil, "Consumer should stop gracefully")

		// Give Kafka a moment to commit offsets
		time.Sleep(2 * time.Second)

		// Now check lag
		lags, err := adminClient.GetConsumerGroupLag(ctx, topic, groupID)
		require.NoError(t, err)
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
	config := &Config{
		Brokers: []string{kafkaContainer.Broker()},
	}
	adminClient := NewAdminClient(config)

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
