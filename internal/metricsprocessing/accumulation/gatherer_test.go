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

package accumulation

import (
	"context"
	"testing"

	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

// MockCompactor implements Compactor for testing
type MockCompactor struct {
	groups     []*AccumulationGroup[CompactionKey]
	commitData []*KafkaCommitData
	estimates  []int64
}

func NewMockCompactor() *MockCompactor {
	return &MockCompactor{}
}

func (m *MockCompactor) Process(ctx context.Context, group *AccumulationGroup[CompactionKey], kafkaCommitData *KafkaCommitData, recordCountEstimate int64) error {
	m.groups = append(m.groups, group)
	m.commitData = append(m.commitData, kafkaCommitData)
	m.estimates = append(m.estimates, recordCountEstimate)
	return nil
}

// MockOffsetStore implements OffsetStore for testing
type MockOffsetStore struct {
	offsets map[string]int64 // key: "topic:partition:consumer_group:org_id:instance_num" -> offset
}

func NewMockOffsetStore() *MockOffsetStore {
	return &MockOffsetStore{
		offsets: make(map[string]int64),
	}
}

func (m *MockOffsetStore) KafkaJournalGetLastProcessedWithOrgInstance(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams) (int64, error) {
	key := params.Topic + ":" + string(rune(params.Partition)) + ":" + params.ConsumerGroup + ":" + params.OrganizationID.String() + ":" + string(rune(params.InstanceNum))
	offset, exists := m.offsets[key]
	if !exists {
		// Return error to simulate no row found
		return 0, assert.AnError
	}
	return offset, nil
}

func (m *MockOffsetStore) SetOffset(topic string, partition int32, consumerGroup string, orgID uuid.UUID, instanceNum int16, offset int64) {
	key := topic + ":" + string(rune(partition)) + ":" + consumerGroup + ":" + orgID.String() + ":" + string(rune(instanceNum))
	m.offsets[key] = offset
}

func TestGatherer_CreateKafkaCommitDataFromGroup(t *testing.T) {
	store := NewMockOffsetStore()
	compactor := NewMockCompactor()
	orgID := uuid.New()

	gatherer := NewGatherer("test-topic", "test-group", store, compactor)

	// Create a group with messages from multiple partitions
	group := &AccumulationGroup[CompactionKey]{
		Key: CompactionKey{
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
	store := NewMockOffsetStore()
	compactor := NewMockCompactor()
	orgID := uuid.New()

	gatherer := NewGatherer("test-topic", "test-group", store, compactor)

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
	assert.True(t, group.TotalRecordCount >= CompactionTargetRecordCount)
}

func TestGatherer_OffsetDeduplication(t *testing.T) {
	store := NewMockOffsetStore()
	orgID := uuid.New()

	gatherer := NewGatherer("test-topic", "test-group", store, NewMockCompactor())

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
	store := NewMockOffsetStore()

	// Create gatherers for different organizations
	orgA := uuid.New()
	orgB := uuid.New()

	gathererA := NewGatherer("test-topic", "test-group", store, NewMockCompactor())
	gathererB := NewGatherer("test-topic", "test-group", store, NewMockCompactor())

	// Use the same metadata tracker to simulate shared consumer group
	sharedTracker := NewMetadataTracker("test-topic", "test-group")
	gathererA.metadataTracker = sharedTracker
	gathererB.metadataTracker = sharedTracker

	// Process messages from both orgs to different offsets
	// Org A processes to offset 75
	groupA := &AccumulationGroup[CompactionKey]{
		Key: CompactionKey{OrganizationID: orgA, InstanceNum: 1},
		Messages: []*AccumulatedMessage{{
			Message:  createTestMessage(orgA, 1, 20250108, 60000, 1, 4, 50),
			Metadata: createTestMetadata("test-topic", 0, "test-group", 75),
		}},
	}
	sharedTracker.TrackMetadata(groupA)

	// Org B processes to offset 65
	groupB := &AccumulationGroup[CompactionKey]{
		Key: CompactionKey{OrganizationID: orgB, InstanceNum: 1},
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
	store := NewMockOffsetStore()
	orgID := uuid.New()

	gatherer := NewGatherer("expected-topic", "expected-group", store, NewMockCompactor())

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
