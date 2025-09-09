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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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

func TestMetadataTracker_SafeCommitOffsets(t *testing.T) {
	tracker := NewMetadataTracker[TestGroupingKey]("test-topic", "test-group")

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
	commitData := tracker.GetSafeCommitOffsets()

	assert.NotNil(t, commitData)
	assert.Equal(t, "test-topic", commitData.Topic)
	assert.Equal(t, "test-group", commitData.ConsumerGroup)
	assert.Len(t, commitData.Offsets, 2)

	// Partition 0: min(10, 15, 54) = 10
	assert.Equal(t, int64(10), commitData.Offsets[0])
	// Partition 1: min(20, 25) = 20
	assert.Equal(t, int64(20), commitData.Offsets[1])
}

func TestMetadataTracker_OnlyAdvancingOffsets(t *testing.T) {
	tracker := NewMetadataTracker[TestGroupingKey]("test-topic", "test-group")

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

	commitData := tracker.GetSafeCommitOffsets()

	// Should only return partition 1 since partition 0 can't advance (min=10 < committed=15)
	assert.NotNil(t, commitData)
	assert.Len(t, commitData.Offsets, 1)
	assert.Equal(t, int64(35), commitData.Offsets[1]) // min(35, 40) = 35
}

func TestMetadataTracker_NoAdvancement(t *testing.T) {
	tracker := NewMetadataTracker[TestGroupingKey]("test-topic", "test-group")

	orgA := uuid.New()

	// Set committed offset higher than current processed
	tracker.lastCommittedOffsets[0] = 50
	tracker.partitionOffsets[0] = map[TestGroupingKey]int64{
		{CustomerID: orgA, ServiceName: "api", Region: "us-east"}: 30,
	}

	commitData := tracker.GetSafeCommitOffsets()

	// Should return nil since no offsets can advance
	assert.Nil(t, commitData)
}

func TestMetadataTracker_MarkOffsetsCommitted(t *testing.T) {
	tracker := NewMetadataTracker[TestGroupingKey]("test-topic", "test-group")

	// Set some initial committed offsets
	tracker.lastCommittedOffsets[0] = 10
	tracker.lastCommittedOffsets[1] = 20

	// Mark new offsets as committed
	newOffsets := map[int32]int64{
		0: 25,
		1: 30,
		2: 5, // New partition
	}

	tracker.MarkOffsetsCommitted(newOffsets)

	// Check that all offsets were updated correctly
	assert.Equal(t, int64(25), tracker.lastCommittedOffsets[0])
	assert.Equal(t, int64(30), tracker.lastCommittedOffsets[1])
	assert.Equal(t, int64(5), tracker.lastCommittedOffsets[2])
}

func TestMetadataTracker_TrackMetadata(t *testing.T) {
	tracker := NewMetadataTracker[TestGroupingKey]("test-topic", "test-group")

	customerA := uuid.New()
	customerB := uuid.New()

	// Create a group with multiple messages using TestGroupingKey
	group := &AccumulationGroup[TestGroupingKey]{
		Key: TestGroupingKey{
			CustomerID:  customerA,
			ServiceName: "api-service",
			Region:      "us-east",
		},
		Messages: []*AccumulatedMessage{
			{
				Metadata: &MessageMetadata{
					Partition: 0,
					Offset:    100,
				},
			},
			{
				Metadata: &MessageMetadata{
					Partition: 0,
					Offset:    105,
				},
			},
			{
				Metadata: &MessageMetadata{
					Partition: 1,
					Offset:    200,
				},
			},
		},
	}

	// Track the metadata
	tracker.TrackMetadata(group)

	// Check that the highest offsets were tracked correctly
	groupingKeyA := TestGroupingKey{CustomerID: customerA, ServiceName: "api-service", Region: "us-east"}

	assert.Contains(t, tracker.partitionOffsets[0], groupingKeyA)
	assert.Equal(t, int64(105), tracker.partitionOffsets[0][groupingKeyA]) // highest offset on partition 0

	assert.Contains(t, tracker.partitionOffsets[1], groupingKeyA)
	assert.Equal(t, int64(200), tracker.partitionOffsets[1][groupingKeyA]) // offset on partition 1

	// Track metadata from another customer on the same partitions
	group2 := &AccumulationGroup[TestGroupingKey]{
		Key: TestGroupingKey{
			CustomerID:  customerB,
			ServiceName: "web-service",
			Region:      "us-west",
		},
		Messages: []*AccumulatedMessage{
			{
				Metadata: &MessageMetadata{
					Partition: 0,
					Offset:    90, // Lower than orgA
				},
			},
		},
	}

	tracker.TrackMetadata(group2)

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
