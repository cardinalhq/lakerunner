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

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// MockCompactor implements Processor for testing
type MockCompactor struct {
	groups     []*AccumulationGroup[messages.CompactionKey]
	commitData []*KafkaCommitData
}

func NewMockCompactor() *MockCompactor {
	return &MockCompactor{}
}

func (m *MockCompactor) Process(ctx context.Context, group *AccumulationGroup[messages.CompactionKey], kafkaCommitData *KafkaCommitData) error {
	m.groups = append(m.groups, group)
	m.commitData = append(m.commitData, kafkaCommitData)
	return nil
}

func (m *MockCompactor) GetTargetRecordCount(ctx context.Context, groupingKey messages.CompactionKey) int64 {
	return 10000 // Default estimate for tests
}

// MockOffsetCallbacks implements OffsetCallbacks for testing
type MockOffsetCallbacks struct {
	offsets map[string]int64 // key: "topic:partition:consumer_group:org_id:instance_num" -> offset
}

func NewMockOffsetCallbacks() *MockOffsetCallbacks {
	return &MockOffsetCallbacks{
		offsets: make(map[string]int64),
	}
}

func (m *MockOffsetCallbacks) GetLastProcessedOffset(ctx context.Context, metadata *MessageMetadata, groupingKey messages.CompactionKey) (int64, error) {
	key := metadata.Topic + ":" + string(rune(metadata.Partition)) + ":" + metadata.ConsumerGroup + ":" + groupingKey.OrganizationID.String() + ":" + string(rune(groupingKey.InstanceNum))
	offset, exists := m.offsets[key]
	if !exists {
		// Return -1 if no row found (never seen before)
		return -1, nil
	}
	return offset, nil
}

func (m *MockOffsetCallbacks) MarkOffsetsProcessed(ctx context.Context, key messages.CompactionKey, offsets map[int32]int64) error {
	// Store the highest offset per partition for this key
	for partition, offset := range offsets {
		keyStr := "topic:" + string(rune(partition)) + ":group:" + key.OrganizationID.String() + ":" + string(rune(key.InstanceNum))
		m.offsets[keyStr] = offset
	}
	return nil
}

func (m *MockOffsetCallbacks) SetOffset(topic string, partition int32, consumerGroup string, orgID uuid.UUID, instanceNum int16, offset int64) {
	key := topic + ":" + string(rune(partition)) + ":" + consumerGroup + ":" + orgID.String() + ":" + string(rune(instanceNum))
	m.offsets[key] = offset
}

func TestGatherer_CreateKafkaCommitDataFromGroup(t *testing.T) {
	offsetCallbacks := NewMockOffsetCallbacks()
	compactor := NewMockCompactor()
	orgID := uuid.New()

	gatherer := NewGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", compactor, offsetCallbacks)

	// Create a group with messages from multiple partitions
	group := &AccumulationGroup[messages.CompactionKey]{
		Key: messages.CompactionKey{
			OrganizationID: orgID,
			InstanceNum:    1,
			DateInt:        20250108,
			FrequencyMs:    60000,
		},
		Messages: []*AccumulatedMessage{
			{
				Message:  createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50),
				Metadata: createTestMetadata("test-topic", 0, "test-group", 100),
			},
			{
				Message:  createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 30),
				Metadata: createTestMetadata("test-topic", 0, "test-group", 105), // Higher offset for same partition
			},
			{
				Message:  createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 25),
				Metadata: createTestMetadata("test-topic", 1, "test-group", 200), // Different partition
			},
		},
		TotalRecordCount: 105,
	}

	commitData := gatherer.createKafkaCommitDataFromGroup(group)

	assert.NotNil(t, commitData)
	assert.Equal(t, "test-topic", commitData.Topic)
	assert.Equal(t, "test-group", commitData.ConsumerGroup)
	assert.Len(t, commitData.Offsets, 2)

	// Should have highest offset per partition
	assert.Equal(t, int64(105), commitData.Offsets[0]) // Max of 100, 105
	assert.Equal(t, int64(200), commitData.Offsets[1])
}

func TestGatherer_ProcessMessage_Integration(t *testing.T) {
	offsetCallbacks := NewMockOffsetCallbacks()
	compactor := NewMockCompactor()
	orgID := uuid.New()

	gatherer := NewGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", compactor, offsetCallbacks)

	// Add messages until threshold is reached
	for i := 0; i < 200; i++ { // Should trigger at CompactionTargetRecordCount=10000
		msg := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50) // 50 records each
		metadata := createTestMetadata("test-topic", 0, "test-group", int64(i))

		err := gatherer.ProcessMessage(context.Background(), msg, metadata)
		assert.NoError(t, err)

		if len(compactor.groups) > 0 {
			break // Compaction triggered
		}
	}

	// Should have triggered compaction
	assert.Len(t, compactor.groups, 1)
	assert.Len(t, compactor.commitData, 1)

	// Verify the commit data contains the actual message offsets
	commitData := compactor.commitData[0]
	assert.NotNil(t, commitData)
	assert.Equal(t, "test-topic", commitData.Topic)
	assert.Equal(t, "test-group", commitData.ConsumerGroup)
	assert.Contains(t, commitData.Offsets, int32(0))

	// Should have processed messages from the bundle
	group := compactor.groups[0]
	assert.Equal(t, orgID, group.Key.OrganizationID)
	assert.True(t, group.TotalRecordCount >= 10000) // Default estimate
}

func TestGatherer_OffsetDeduplication(t *testing.T) {
	offsetCallbacks := NewMockOffsetCallbacks()
	orgID := uuid.New()

	gatherer := NewGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", NewMockCompactor(), offsetCallbacks)

	// Process message with offset 100
	msg1 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata1 := createTestMetadata("test-topic", 0, "test-group", 100)

	err1 := gatherer.ProcessMessage(context.Background(), msg1, metadata1)
	assert.NoError(t, err1)

	// Try to process same message again - should be dropped by offset tracker
	err2 := gatherer.ProcessMessage(context.Background(), msg1, metadata1)
	assert.NoError(t, err2)

	// Verify only one message was actually processed by hunter
	pendingGroups := len(gatherer.hunter.groups)
	assert.Equal(t, 1, pendingGroups) // Would be 2 if duplicate wasn't filtered
}

func TestGatherer_MultipleOrgsMinimumOffset(t *testing.T) {
	offsetCallbacks := NewMockOffsetCallbacks()

	// Create gatherers for different organizations
	orgA := uuid.New()
	orgB := uuid.New()

	gathererA := NewGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", NewMockCompactor(), offsetCallbacks)
	gathererB := NewGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", NewMockCompactor(), offsetCallbacks)

	// Use the same metadata tracker to simulate shared consumer group
	sharedTracker := NewMetadataTracker[messages.CompactionKey]("test-topic", "test-group")
	gathererA.metadataTracker = sharedTracker
	gathererB.metadataTracker = sharedTracker

	// Process messages from both orgs to different offsets
	// Org A processes to offset 75
	groupA := &AccumulationGroup[messages.CompactionKey]{
		Key: messages.CompactionKey{OrganizationID: orgA, InstanceNum: 1},
		Messages: []*AccumulatedMessage{{
			Message:  createTestMessage(orgA, 1, 20250108, 60000, 1, 4, 50),
			Metadata: createTestMetadata("test-topic", 0, "test-group", 75),
		}},
	}
	sharedTracker.TrackMetadata(groupA)

	// Org B processes to offset 65
	groupB := &AccumulationGroup[messages.CompactionKey]{
		Key: messages.CompactionKey{OrganizationID: orgB, InstanceNum: 1},
		Messages: []*AccumulatedMessage{{
			Message:  createTestMessage(orgB, 1, 20250108, 60000, 1, 4, 50),
			Metadata: createTestMetadata("test-topic", 0, "test-group", 65),
		}},
	}
	sharedTracker.TrackMetadata(groupB)

	// Safe commit should be minimum (65)
	commitData := sharedTracker.GetSafeCommitOffsets()
	assert.NotNil(t, commitData)
	assert.Equal(t, int64(65), commitData.Offsets[0]) // min(75, 65) = 65
}

func TestGatherer_ValidatesTopicAndConsumerGroup(t *testing.T) {
	offsetCallbacks := NewMockOffsetCallbacks()
	orgID := uuid.New()

	gatherer := NewGatherer[*messages.MetricCompactionMessage]("expected-topic", "expected-group", NewMockCompactor(), offsetCallbacks)

	msg := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)

	// Test wrong topic
	wrongTopicMetadata := createTestMetadata("wrong-topic", 0, "expected-group", 100)
	err := gatherer.ProcessMessage(context.Background(), msg, wrongTopicMetadata)
	assert.Error(t, err)
	var configErr *ConfigMismatchError
	assert.ErrorAs(t, err, &configErr)
	assert.Equal(t, "topic", configErr.Field)
	assert.Equal(t, "expected-topic", configErr.Expected)
	assert.Equal(t, "wrong-topic", configErr.Got)

	// Test wrong consumer group
	wrongGroupMetadata := createTestMetadata("expected-topic", 0, "wrong-group", 100)
	err = gatherer.ProcessMessage(context.Background(), msg, wrongGroupMetadata)
	assert.Error(t, err)
	assert.ErrorAs(t, err, &configErr)
	assert.Equal(t, "consumer_group", configErr.Field)
	assert.Equal(t, "expected-group", configErr.Expected)
	assert.Equal(t, "wrong-group", configErr.Got)

	// Test correct topic and group
	correctMetadata := createTestMetadata("expected-topic", 0, "expected-group", 100)
	err = gatherer.ProcessMessage(context.Background(), msg, correctMetadata)
	assert.NoError(t, err)
}

// Helper functions for testing
func createTestMessage(orgID uuid.UUID, instanceNum int16, dateInt int32, frequencyMs int32, slotID int32, slotCount int32, recordCount int64) *messages.MetricCompactionMessage {
	return &messages.MetricCompactionMessage{
		OrganizationID: orgID,
		DateInt:        dateInt,
		FrequencyMs:    frequencyMs,
		SegmentID:      123,
		InstanceNum:    instanceNum,
		SlotID:         slotID,
		SlotCount:      slotCount,
		Records:        recordCount,
		FileSize:       1000,
		QueuedAt:       time.Now(),
	}
}

func createTestMetadata(topic string, partition int32, consumerGroup string, offset int64) *MessageMetadata {
	return &MessageMetadata{
		Topic:         topic,
		Partition:     partition,
		ConsumerGroup: consumerGroup,
		Offset:        offset,
	}
}
