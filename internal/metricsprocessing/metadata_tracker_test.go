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
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Custom test message type - completely different from messages.MetricCompactionMessage
type TestProcessingMessage struct {
	CustomerID   uuid.UUID `json:"customer_id"`
	ServiceName  string    `json:"service"`
	Region       string    `json:"region"`
	Environment  string    `json:"env"`
	RequestCount int64     `json:"requests"`
	Timestamp    time.Time `json:"ts"`
}

// Custom grouping key - used for logical grouping in the Gatherer
type TestGroupingKey struct {
	CustomerID  uuid.UUID
	ServiceName string
	Region      string
}

// Custom offset tracking key - completely different structure for offset callbacks
type TestOffsetKey struct {
	CustomerID  uuid.UUID
	Region      string
	ServiceName string
	Environment string
}

// Implement GroupableMessage interface
func (m *TestProcessingMessage) GroupingKey() TestGroupingKey {
	return TestGroupingKey{
		CustomerID:  m.CustomerID,
		ServiceName: m.ServiceName,
		Region:      m.Region,
	}
}

// Helper to convert grouping key to offset key (demonstrating different key structures)
func groupingKeyToOffsetKey(gk TestGroupingKey, env string) TestOffsetKey {
	return TestOffsetKey{
		CustomerID:  gk.CustomerID,
		Region:      gk.Region,
		ServiceName: gk.ServiceName,
		Environment: env,
	}
}

// testMessage implements messages.GroupableMessage for testing
type testMessage struct {
	key    TestGroupingKey
	count  int64
}

func (tm *testMessage) GroupingKey() any {
	return tm.key
}

func (tm *testMessage) RecordCount() int64 {
	return tm.count
}

func (tm *testMessage) Unmarshal(data []byte) error {
	return nil
}

func (tm *testMessage) Marshal() ([]byte, error) {
	return nil, nil
}

func TestMetadataTracker_SafeCommitOffsets(t *testing.T) {
	hunter := newHunter[*testMessage, TestGroupingKey]()
	tracker := newMetadataTracker[*testMessage, TestGroupingKey]("test-topic", "test-group", hunter)

	customerA := uuid.New()
	customerB := uuid.New()
	customerC := uuid.New()

	// Simulate processing messages from different customers on different partitions
	// Partition 0: CustomerA=10, CustomerB=15, CustomerC=54
	tracker.partitionOffsets[0] = map[TestGroupingKey]int64{
		{CustomerID: customerA, ServiceName: "api", Region: "us-west"}: 10,
		{CustomerID: customerB, ServiceName: "api", Region: "us-west"}: 15,
		{CustomerID: customerC, ServiceName: "api", Region: "us-west"}: 54,
	}

	// Partition 1: CustomerA=20, CustomerB=25
	tracker.partitionOffsets[1] = map[TestGroupingKey]int64{
		{CustomerID: customerA, ServiceName: "api", Region: "us-west"}: 20,
		{CustomerID: customerB, ServiceName: "api", Region: "us-west"}: 25,
	}

	// Get safe commit offsets - should be minimums
	commitData := tracker.getSafeCommitOffsets()

	assert.NotNil(t, commitData)
	assert.Equal(t, "test-topic", commitData.Topic)
	assert.Equal(t, "test-group", commitData.ConsumerGroup)
	assert.Len(t, commitData.Offsets, 2)

	// Partition 0: max(10, 15, 54) = 54 (no pending groups)
	assert.Equal(t, int64(54), commitData.Offsets[0])
	// Partition 1: max(20, 25) = 25 (no pending groups)
	assert.Equal(t, int64(25), commitData.Offsets[1])
}

func TestMetadataTracker_OnlyAdvancingOffsets(t *testing.T) {
	hunter := newHunter[*testMessage, TestGroupingKey]()
	tracker := newMetadataTracker[*testMessage, TestGroupingKey]("test-topic", "test-group", hunter)

	orgA := uuid.New()
	orgB := uuid.New()

	// Set some already committed offsets
	tracker.lastCommittedOffsets[0] = 15
	tracker.lastCommittedOffsets[1] = 30

	// Current processed offsets
	tracker.partitionOffsets[0] = map[TestGroupingKey]int64{
		{CustomerID: orgA, ServiceName: "api", Region: "us-east"}: 10, // Behind committed offset
		{CustomerID: orgB, ServiceName: "web", Region: "us-west"}: 12, // Behind committed offset
	}
	tracker.partitionOffsets[1] = map[TestGroupingKey]int64{
		{CustomerID: orgA, ServiceName: "api", Region: "us-east"}: 35, // Ahead of committed offset
		{CustomerID: orgB, ServiceName: "web", Region: "us-west"}: 40, // Ahead of committed offset
	}

	commitData := tracker.getSafeCommitOffsets()

	// Should only return partition 1 since partition 0 can't advance (max=12 < committed=15)
	assert.NotNil(t, commitData)
	assert.Len(t, commitData.Offsets, 1)
	assert.Equal(t, int64(40), commitData.Offsets[1]) // max(35, 40) = 40
}

func TestMetadataTracker_NoAdvancement(t *testing.T) {
	hunter := newHunter[*testMessage, TestGroupingKey]()
	tracker := newMetadataTracker[*testMessage, TestGroupingKey]("test-topic", "test-group", hunter)

	orgA := uuid.New()

	// Set committed offset higher than current processed
	tracker.lastCommittedOffsets[0] = 50
	tracker.partitionOffsets[0] = map[TestGroupingKey]int64{
		{CustomerID: orgA, ServiceName: "api", Region: "us-east"}: 30,
	}

	commitData := tracker.getSafeCommitOffsets()

	// Should return nil since no offsets can advance
	assert.Nil(t, commitData)
}

func TestMetadataTracker_MarkOffsetsCommitted(t *testing.T) {
	hunter := newHunter[*testMessage, TestGroupingKey]()
	tracker := newMetadataTracker[*testMessage, TestGroupingKey]("test-topic", "test-group", hunter)

	// Set some initial committed offsets
	tracker.lastCommittedOffsets[0] = 10
	tracker.lastCommittedOffsets[1] = 20

	// Mark new offsets as committed
	newOffsets := map[int32]int64{
		0: 25,
		1: 30,
		2: 5, // New partition
	}

	tracker.markOffsetsCommitted(newOffsets)

	// Check that all offsets were updated correctly
	assert.Equal(t, int64(25), tracker.lastCommittedOffsets[0])
	assert.Equal(t, int64(30), tracker.lastCommittedOffsets[1])
	assert.Equal(t, int64(5), tracker.lastCommittedOffsets[2])
}

func TestMetadataTracker_TrackMetadata(t *testing.T) {
	hunter := newHunter[*testMessage, TestGroupingKey]()
	tracker := newMetadataTracker[*testMessage, TestGroupingKey]("test-topic", "test-group", hunter)

	customerA := uuid.New()
	customerB := uuid.New()

	// Create a group with multiple messages using TestGroupingKey
	group := &accumulationGroup[TestGroupingKey]{
		Key: TestGroupingKey{
			CustomerID:  customerA,
			ServiceName: "api-service",
			Region:      "us-east",
		},
		Messages: []*accumulatedMessage{
			{
				Metadata: &messageMetadata{
					Partition: 0,
					Offset:    100,
				},
			},
			{
				Metadata: &messageMetadata{
					Partition: 0,
					Offset:    105,
				},
			},
			{
				Metadata: &messageMetadata{
					Partition: 1,
					Offset:    200,
				},
			},
		},
	}

	// Track the metadata
	tracker.trackMetadata(group)

	// Check that the highest offsets were tracked correctly
	groupingKeyA := TestGroupingKey{CustomerID: customerA, ServiceName: "api-service", Region: "us-east"}

	assert.Contains(t, tracker.partitionOffsets[0], groupingKeyA)
	assert.Equal(t, int64(105), tracker.partitionOffsets[0][groupingKeyA]) // highest offset on partition 0

	assert.Contains(t, tracker.partitionOffsets[1], groupingKeyA)
	assert.Equal(t, int64(200), tracker.partitionOffsets[1][groupingKeyA]) // offset on partition 1

	// Track metadata from another customer on the same partitions
	group2 := &accumulationGroup[TestGroupingKey]{
		Key: TestGroupingKey{
			CustomerID:  customerB,
			ServiceName: "web-service",
			Region:      "us-west",
		},
		Messages: []*accumulatedMessage{
			{
				Metadata: &messageMetadata{
					Partition: 0,
					Offset:    90, // Lower than orgA
				},
			},
		},
	}

	tracker.trackMetadata(group2)

	groupingKeyB := TestGroupingKey{CustomerID: customerB, ServiceName: "web-service", Region: "us-west"}

	// Both customers should be tracked separately
	assert.Contains(t, tracker.partitionOffsets[0], groupingKeyA)
	assert.Contains(t, tracker.partitionOffsets[0], groupingKeyB)
	assert.Equal(t, int64(105), tracker.partitionOffsets[0][groupingKeyA])
	assert.Equal(t, int64(90), tracker.partitionOffsets[0][groupingKeyB])

	// Demonstrate the TestOffsetKey usage - convert grouping keys to offset keys
	offsetKeyA := groupingKeyToOffsetKey(groupingKeyA, "production")
	offsetKeyB := groupingKeyToOffsetKey(groupingKeyB, "staging")

	// These offset keys would be used by a different system for actual Kafka offset storage
	expectedOffsetKeyA := TestOffsetKey{CustomerID: customerA, Region: "us-east", ServiceName: "api-service", Environment: "production"}
	expectedOffsetKeyB := TestOffsetKey{CustomerID: customerB, Region: "us-west", ServiceName: "web-service", Environment: "staging"}

	assert.Equal(t, expectedOffsetKeyA, offsetKeyA)
	assert.Equal(t, expectedOffsetKeyB, offsetKeyB)
}

// TestCustomTypesWorkflow demonstrates the full workflow with all three custom types
func TestCustomTypesWorkflow(t *testing.T) {
	customerA := uuid.New()
	customerB := uuid.New()

	// 1. Create custom messages (what would come from Kafka)
	msg1 := &TestProcessingMessage{
		CustomerID:   customerA,
		ServiceName:  "payment-service",
		Region:       "us-east",
		Environment:  "production",
		RequestCount: 1500,
		Timestamp:    time.Now(),
	}

	msg2 := &TestProcessingMessage{
		CustomerID:   customerB,
		ServiceName:  "user-service",
		Region:       "eu-west",
		Environment:  "staging",
		RequestCount: 750,
		Timestamp:    time.Now(),
	}

	// 2. Demonstrate GroupingKey extraction (what Gatherer would use)
	groupKey1 := msg1.GroupingKey()
	groupKey2 := msg2.GroupingKey()

	expectedGroupKey1 := TestGroupingKey{CustomerID: customerA, ServiceName: "payment-service", Region: "us-east"}
	expectedGroupKey2 := TestGroupingKey{CustomerID: customerB, ServiceName: "user-service", Region: "eu-west"}

	assert.Equal(t, expectedGroupKey1, groupKey1)
	assert.Equal(t, expectedGroupKey2, groupKey2)

	// 3. Demonstrate conversion to OffsetKey (what offset callbacks would use)
	offsetKey1 := groupingKeyToOffsetKey(groupKey1, msg1.Environment)
	offsetKey2 := groupingKeyToOffsetKey(groupKey2, msg2.Environment)

	expectedOffsetKey1 := TestOffsetKey{CustomerID: customerA, Region: "us-east", ServiceName: "payment-service", Environment: "production"}
	expectedOffsetKey2 := TestOffsetKey{CustomerID: customerB, Region: "eu-west", ServiceName: "user-service", Environment: "staging"}

	assert.Equal(t, expectedOffsetKey1, offsetKey1)
	assert.Equal(t, expectedOffsetKey2, offsetKey2)

	// 4. Show that the three types are structurally different
	assert.NotEqual(t, len(msg1.ServiceName), len(groupKey1.CustomerID.String()), "Message and GroupingKey are different structures")
	assert.NotEqual(t, groupKey1.Region, offsetKey1.Environment, "GroupingKey and OffsetKey have different fields")

	t.Logf("Message: %+v", msg1)
	t.Logf("GroupingKey: %+v", groupKey1)
	t.Logf("OffsetKey: %+v", offsetKey1)
}

// TestCollectKafkaOffsetsFromGroup tests the collectKafkaOffsetsFromGroup helper function in gatherer
func TestCollectKafkaOffsetsFromGroup(t *testing.T) {
	tests := []struct {
		name             string
		group            *accumulationGroup[messages.IngestKey]
		kafkaCommitData  *KafkaCommitData
		expectedPartsLen int // Since map iteration order is non-deterministic
	}{
		{
			name:             "nil kafkaCommitData returns nil",
			group:            &accumulationGroup[messages.IngestKey]{},
			kafkaCommitData:  nil,
			expectedPartsLen: 0,
		},
		{
			name: "empty messages returns nil",
			group: &accumulationGroup[messages.IngestKey]{
				Messages: []*accumulatedMessage{},
			},
			kafkaCommitData: &KafkaCommitData{
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
			},
			expectedPartsLen: 0,
		},
		{
			name: "single partition with multiple offsets",
			group: &accumulationGroup[messages.IngestKey]{
				Key: messages.IngestKey{
					OrganizationID: uuid.New(),
					InstanceNum:    1,
				},
				Messages: []*accumulatedMessage{
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     0,
							ConsumerGroup: "test-group",
							Offset:        100,
						},
					},
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     0,
							ConsumerGroup: "test-group",
							Offset:        101,
						},
					},
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     0,
							ConsumerGroup: "test-group",
							Offset:        102,
						},
					},
				},
				CreatedAt: time.Now(),
			},
			kafkaCommitData: &KafkaCommitData{
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
				Offsets:       map[int32]int64{0: 102},
			},
			expectedPartsLen: 1,
		},
		{
			name: "multiple partitions with offsets",
			group: &accumulationGroup[messages.IngestKey]{
				Key: messages.IngestKey{
					OrganizationID: uuid.New(),
					InstanceNum:    1,
				},
				Messages: []*accumulatedMessage{
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     0,
							ConsumerGroup: "test-group",
							Offset:        100,
						},
					},
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     1,
							ConsumerGroup: "test-group",
							Offset:        200,
						},
					},
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     0,
							ConsumerGroup: "test-group",
							Offset:        101,
						},
					},
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     1,
							ConsumerGroup: "test-group",
							Offset:        201,
						},
					},
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     2,
							ConsumerGroup: "test-group",
							Offset:        300,
						},
					},
				},
				CreatedAt: time.Now(),
			},
			kafkaCommitData: &KafkaCommitData{
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
				Offsets:       map[int32]int64{0: 101, 1: 201, 2: 300},
			},
			expectedPartsLen: 3,
		},
		{
			name: "non-contiguous offsets in same partition",
			group: &accumulationGroup[messages.IngestKey]{
				Key: messages.IngestKey{
					OrganizationID: uuid.New(),
					InstanceNum:    1,
				},
				Messages: []*accumulatedMessage{
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     0,
							ConsumerGroup: "test-group",
							Offset:        100,
						},
					},
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     0,
							ConsumerGroup: "test-group",
							Offset:        105, // Gap in offsets
						},
					},
					{
						Message: &messages.ObjStoreNotificationMessage{},
						Metadata: &messageMetadata{
							Topic:         "test-topic",
							Partition:     0,
							ConsumerGroup: "test-group",
							Offset:        110, // Another gap
						},
					},
				},
				CreatedAt: time.Now(),
			},
			kafkaCommitData: &KafkaCommitData{
				Topic:         "test-topic",
				ConsumerGroup: "test-group",
				Offsets:       map[int32]int64{0: 110},
			},
			expectedPartsLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the gatherer's collectKafkaOffsetsFromGroup method
			g := &gatherer[*messages.ObjStoreNotificationMessage, messages.IngestKey]{
				topic:         "test-topic",
				consumerGroup: "test-group",
			}
			if tt.kafkaCommitData != nil {
				g.topic = tt.kafkaCommitData.Topic
				g.consumerGroup = tt.kafkaCommitData.ConsumerGroup
			}
			result := g.collectKafkaOffsetsFromGroup(tt.group)

			if tt.expectedPartsLen == 0 {
				assert.Nil(t, result)
				return
			}

			// Check we have the right number of partitions
			require.Equal(t, tt.expectedPartsLen, len(result))

			// For each partition in the result, verify it has the right data
			partitionOffsets := make(map[int32][]int64)
			for _, kafkaOffset := range result {
				assert.Equal(t, tt.kafkaCommitData.ConsumerGroup, kafkaOffset.ConsumerGroup)
				assert.Equal(t, tt.kafkaCommitData.Topic, kafkaOffset.Topic)
				partitionOffsets[kafkaOffset.PartitionID] = kafkaOffset.Offsets
			}

			// Verify offsets are collected correctly for each partition
			expectedPartitionOffsets := make(map[int32][]int64)
			for _, msg := range tt.group.Messages {
				expectedPartitionOffsets[msg.Metadata.Partition] = append(
					expectedPartitionOffsets[msg.Metadata.Partition],
					msg.Metadata.Offset,
				)
			}

			assert.Equal(t, len(expectedPartitionOffsets), len(partitionOffsets))
			for partition, offsets := range expectedPartitionOffsets {
				assert.ElementsMatch(t, offsets, partitionOffsets[partition],
					"Partition %d offsets should match", partition)
			}
		})
	}
}

// TestCollectKafkaOffsetsFromGroup_CompactionKey tests with CompactionKey type to ensure generic works properly
func TestCollectKafkaOffsetsFromGroup_CompactionKey(t *testing.T) {
	// Test with CompactionKey type to ensure generic works properly
	group := &accumulationGroup[messages.CompactionKey]{
		Key: messages.CompactionKey{
			OrganizationID: uuid.New(),
			DateInt:        20240115,
			InstanceNum:    1,
		},
		Messages: []*accumulatedMessage{
			{
				Message: &messages.MetricCompactionMessage{},
				Metadata: &messageMetadata{
					Topic:         "compaction-topic",
					Partition:     0,
					ConsumerGroup: "compaction-group",
					Offset:        1000,
				},
			},
			{
				Message: &messages.MetricCompactionMessage{},
				Metadata: &messageMetadata{
					Topic:         "compaction-topic",
					Partition:     0,
					ConsumerGroup: "compaction-group",
					Offset:        1001,
				},
			},
		},
		CreatedAt: time.Now(),
	}

	kafkaCommitData := &KafkaCommitData{
		Topic:         "compaction-topic",
		ConsumerGroup: "compaction-group",
		Offsets:       map[int32]int64{0: 1001},
	}

	// Test the gatherer's collectKafkaOffsetsFromGroup method
	g := &gatherer[*messages.MetricCompactionMessage, messages.CompactionKey]{
		topic:         kafkaCommitData.Topic,
		consumerGroup: kafkaCommitData.ConsumerGroup,
	}
	result := g.collectKafkaOffsetsFromGroup(group)

	require.Len(t, result, 1)
	assert.Equal(t, "compaction-group", result[0].ConsumerGroup)
	assert.Equal(t, "compaction-topic", result[0].Topic)
	assert.Equal(t, int32(0), result[0].PartitionID)
	assert.ElementsMatch(t, []int64{1000, 1001}, result[0].Offsets)
}
