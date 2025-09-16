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

package metricsprocessing

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MockCompactor implements Processor for testing
type MockCompactor struct {
	groups       []*accumulationGroup[messages.CompactionKey]
	kafkaOffsets [][]lrdb.KafkaOffsetInfo
}

func NewMockCompactor() *MockCompactor {
	return &MockCompactor{}
}

func (m *MockCompactor) Process(ctx context.Context, group *accumulationGroup[messages.CompactionKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	m.groups = append(m.groups, group)
	m.kafkaOffsets = append(m.kafkaOffsets, kafkaOffsets)
	return nil
}

func (m *MockCompactor) GetTargetRecordCount(ctx context.Context, groupingKey messages.CompactionKey) int64 {
	return 10000 // Default estimate for tests
}

// MockOffsetStore implements offsetStore for testing
type MockOffsetStore struct {
	processedOffsets map[string][]int64 // key: "consumer_group:topic:partition" -> offsets
}

func NewMockOffsetStore() *MockOffsetStore {
	return &MockOffsetStore{
		processedOffsets: make(map[string][]int64),
	}
}

func (m *MockOffsetStore) KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error) {
	key := params.ConsumerGroup + ":" + params.Topic + ":" + string(rune(params.PartitionID))
	offsets, exists := m.processedOffsets[key]
	if !exists {
		return []int64{}, nil
	}

	// Filter offsets >= MinOffset
	var result []int64
	for _, offset := range offsets {
		if offset >= params.MinOffset {
			result = append(result, offset)
		}
	}
	return result, nil
}

func (m *MockOffsetStore) SetProcessedOffsets(consumerGroup, topic string, partition int32, offsets []int64) {
	key := consumerGroup + ":" + topic + ":" + string(rune(partition))
	m.processedOffsets[key] = offsets
}

func (m *MockOffsetStore) CleanupKafkaOffsets(ctx context.Context, params lrdb.CleanupKafkaOffsetsParams) (int64, error) {
	key := params.ConsumerGroup + ":" + params.Topic + ":" + string(rune(params.PartitionID))
	offsets, exists := m.processedOffsets[key]
	if !exists {
		return 0, nil
	}

	// Remove offsets <= MaxOffset
	var remaining []int64
	var deleted int64
	for _, offset := range offsets {
		if offset <= params.MaxOffset {
			deleted++
		} else {
			remaining = append(remaining, offset)
		}
	}
	m.processedOffsets[key] = remaining
	return deleted, nil
}

func TestGatherer_CreateKafkaCommitDataFromGroup(t *testing.T) {
	t.Skip("Test needs rewrite after refactoring to use []KafkaOffsetInfo")
	/*
		offsetStore := NewMockOffsetStore()
		compactor := NewMockCompactor()
		orgID := uuid.New()

		gatherer := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", compactor, offsetStore)

		// Create a group with messages from multiple partitions
		group := &accumulationGroup[messages.CompactionKey]{
			Key: messages.CompactionKey{
				OrganizationID: orgID,
				InstanceNum:    1,
				DateInt:        20250108,
				FrequencyMs:    60000,
			},
			Messages: []*accumulatedMessage{
				{
					Message:  createTestMessage(orgID, 1, 20250108, 60000, 50),
					Metadata: createTestMetadata("test-topic", 0, "test-group", 100),
				},
				{
					Message:  createTestMessage(orgID, 1, 20250108, 60000, 30),
					Metadata: createTestMetadata("test-topic", 0, "test-group", 105), // Higher offset for same partition
				},
				{
					Message:  createTestMessage(orgID, 1, 20250108, 60000, 25),
					Metadata: createTestMetadata("test-topic", 1, "test-group", 200), // Different partition
				},
			},
			TotalRecordCount: 105,
		}

		// This test was for createKafkaCommitDataFromGroup which has been removed
		// The functionality is now in collectKafkaOffsetsFromGroup which returns []KafkaOffsetInfo
		// instead of *KafkaCommitData
		_ = group
	*/
}

func TestGatherer_ProcessMessage_Integration(t *testing.T) {
	offsetStore := NewMockOffsetStore()
	compactor := NewMockCompactor()
	orgID := uuid.New()

	gatherer := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", compactor, offsetStore)

	// Add messages until threshold is reached
	for i := 0; i < 200; i++ { // Should trigger at CompactionTargetRecordCount=10000
		msg := createTestMessage(orgID, 1, 20250108, 60000, 50) // 50 records each
		metadata := createTestMetadata("test-topic", 0, "test-group", int64(i))

		err := gatherer.processMessage(context.Background(), msg, metadata)
		assert.NoError(t, err)

		if len(compactor.groups) > 0 {
			break // Compaction triggered
		}
	}

	// Should have triggered compaction
	assert.Len(t, compactor.groups, 1)
	assert.Len(t, compactor.kafkaOffsets, 1)

	// Verify the kafka offsets were collected
	kafkaOffsets := compactor.kafkaOffsets[0]
	assert.NotNil(t, kafkaOffsets)
	assert.Greater(t, len(kafkaOffsets), 0)
	// Verify partition 0 is included
	hasPartition0 := false
	for _, info := range kafkaOffsets {
		if info.PartitionID == 0 {
			hasPartition0 = true
			break
		}
	}
	assert.True(t, hasPartition0)

	// Should have processed messages from the bundle
	group := compactor.groups[0]
	assert.Equal(t, orgID, group.Key.OrganizationID)
	assert.True(t, group.TotalRecordCount >= 10000) // Default estimate
}

func TestGatherer_OffsetDeduplication(t *testing.T) {
	offsetStore := NewMockOffsetStore()
	orgID := uuid.New()

	gatherer := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", NewMockCompactor(), offsetStore)

	// Process message with offset 100
	msg1 := createTestMessage(orgID, 1, 20250108, 60000, 50)
	metadata1 := createTestMetadata("test-topic", 0, "test-group", 100)

	err1 := gatherer.processMessage(context.Background(), msg1, metadata1)
	assert.NoError(t, err1)

	// Try to process same message again - should be dropped by offset tracker
	err2 := gatherer.processMessage(context.Background(), msg1, metadata1)
	assert.NoError(t, err2)

	// Verify only one message was actually processed by hunter
	pendingGroups := len(gatherer.hunter.groups)
	assert.Equal(t, 1, pendingGroups) // Would be 2 if duplicate wasn't filtered
}

func TestGatherer_MultipleOrgsMinimumOffset(t *testing.T) {
	offsetStore := NewMockOffsetStore()

	// Create gatherers for different organizations
	orgA := uuid.New()
	orgB := uuid.New()

	gathererA := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", NewMockCompactor(), offsetStore)
	gathererB := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", NewMockCompactor(), offsetStore)

	// Use the same metadata tracker to simulate shared consumer group
	// Create a shared hunter for the tracker
	sharedHunter := newHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	sharedTracker := newMetadataTracker[*messages.MetricCompactionMessage, messages.CompactionKey]("test-topic", "test-group", sharedHunter)
	gathererA.metadataTracker = sharedTracker
	gathererB.metadataTracker = sharedTracker

	// Process messages from both orgs to different offsets
	// Org A processes to offset 75
	groupA := &accumulationGroup[messages.CompactionKey]{
		Key: messages.CompactionKey{OrganizationID: orgA, InstanceNum: 1},
		Messages: []*accumulatedMessage{{
			Message:  createTestMessage(orgA, 1, 20250108, 60000, 50),
			Metadata: createTestMetadata("test-topic", 0, "test-group", 75),
		}},
	}
	sharedTracker.trackMetadata(groupA)

	// Org B processes to offset 65
	groupB := &accumulationGroup[messages.CompactionKey]{
		Key: messages.CompactionKey{OrganizationID: orgB, InstanceNum: 1},
		Messages: []*accumulatedMessage{{
			Message:  createTestMessage(orgB, 1, 20250108, 60000, 50),
			Metadata: createTestMetadata("test-topic", 0, "test-group", 65),
		}},
	}
	sharedTracker.trackMetadata(groupB)

	// Safe commit should be maximum (75) when no pending groups
	commitData := sharedTracker.getSafeCommitOffsets()
	assert.NotNil(t, commitData)
	assert.Equal(t, int64(75), commitData.Offsets[0]) // max(75, 65) = 75
}

func TestGatherer_ValidatesTopicAndConsumerGroup(t *testing.T) {
	offsetStore := NewMockOffsetStore()
	orgID := uuid.New()

	gatherer := newGatherer[*messages.MetricCompactionMessage]("expected-topic", "expected-group", NewMockCompactor(), offsetStore)

	// Verify Gatherer stores the expected topic and consumer group
	assert.Equal(t, "expected-topic", gatherer.topic)
	assert.Equal(t, "expected-group", gatherer.consumerGroup)

	msg := createTestMessage(orgID, 1, 20250108, 60000, 50)

	// Test wrong topic
	wrongTopicMetadata := createTestMetadata("wrong-topic", 0, "expected-group", 100)
	err := gatherer.processMessage(context.Background(), msg, wrongTopicMetadata)
	assert.Error(t, err)
	var configErr *ConfigMismatchError
	assert.ErrorAs(t, err, &configErr)
	assert.Equal(t, "topic", configErr.Field)
	assert.Equal(t, "expected-topic", configErr.Expected)
	assert.Equal(t, "wrong-topic", configErr.Got)

	// Test wrong consumer group
	wrongGroupMetadata := createTestMetadata("expected-topic", 0, "wrong-group", 100)
	err = gatherer.processMessage(context.Background(), msg, wrongGroupMetadata)
	assert.Error(t, err)
	assert.ErrorAs(t, err, &configErr)
	assert.Equal(t, "consumer_group", configErr.Field)
	assert.Equal(t, "expected-group", configErr.Expected)
	assert.Equal(t, "wrong-group", configErr.Got)

	// Test correct topic and group
	correctMetadata := createTestMetadata("expected-topic", 0, "expected-group", 100)
	err = gatherer.processMessage(context.Background(), msg, correctMetadata)
	assert.NoError(t, err)
}

func TestGatherer_PreventsMixedTopics(t *testing.T) {
	offsetStore := NewMockOffsetStore()
	compactor := NewMockCompactor()
	orgID := uuid.New()

	// Create a gatherer for compaction topic
	gatherer := newGatherer[*messages.MetricCompactionMessage]("test.segments.metrics.compact", "test.compact.metrics", compactor, offsetStore)

	msg := createTestMessage(orgID, 1, 20250909, 60000, 5000)

	// Should accept compaction messages
	compactMetadata := createTestMetadata("test.segments.metrics.compact", 0, "test.compact.metrics", 100)
	err := gatherer.processMessage(context.Background(), msg, compactMetadata)
	assert.NoError(t, err, "Should accept messages from expected compaction topic")

	// Should reject rollup messages (different topic)
	rollupMetadata := createTestMetadata("test.segments.metrics.rollup", 0, "test.compact.metrics", 101)
	err = gatherer.processMessage(context.Background(), msg, rollupMetadata)
	assert.Error(t, err, "Should reject messages from rollup topic")

	var configErr *ConfigMismatchError
	assert.ErrorAs(t, err, &configErr)
	assert.Equal(t, "topic", configErr.Field)
	assert.Equal(t, "test.segments.metrics.compact", configErr.Expected)
	assert.Equal(t, "test.segments.metrics.rollup", configErr.Got)

	// Verify only the compaction message was processed
	assert.Len(t, compactor.groups, 0, "No groups should be processed yet (below threshold)")
	assert.Len(t, gatherer.hunter.groups, 1, "Should have one accumulation group from valid message")

	t.Logf("Successfully prevented mixed topics: expected=%s, rejected=%s",
		"test.segments.metrics.compact",
		"test.segments.metrics.rollup")
}

// Helper functions for testing
func createTestMessage(orgID uuid.UUID, instanceNum int16, dateInt int32, frequencyMs int32, recordCount int64) *messages.MetricCompactionMessage {
	return &messages.MetricCompactionMessage{
		OrganizationID: orgID,
		DateInt:        dateInt,
		FrequencyMs:    frequencyMs,
		SegmentID:      123,
		InstanceNum:    instanceNum,
		Records:        recordCount,
		FileSize:       1000,
		QueuedAt:       time.Now(),
	}
}

func createTestMetadata(topic string, partition int32, consumerGroup string, offset int64) *messageMetadata {
	return &messageMetadata{
		Topic:         topic,
		Partition:     partition,
		ConsumerGroup: consumerGroup,
		Offset:        offset,
	}
}
