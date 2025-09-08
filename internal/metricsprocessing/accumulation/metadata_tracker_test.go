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
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestMetadataTracker_SafeCommitOffsets(t *testing.T) {
	tracker := NewMetadataTracker("test-topic", "test-group")

	orgA := uuid.New()
	orgB := uuid.New()
	orgC := uuid.New()

	// Simulate processing messages from different orgs on different partitions
	// Partition 0: OrgA=10, OrgB=15, OrgC=54
	tracker.partitionOffsets[0] = map[OrgInstanceKey]int64{
		{OrganizationID: orgA, InstanceNum: 1}: 10,
		{OrganizationID: orgB, InstanceNum: 1}: 15,
		{OrganizationID: orgC, InstanceNum: 1}: 54,
	}

	// Partition 1: OrgA=20, OrgB=25
	tracker.partitionOffsets[1] = map[OrgInstanceKey]int64{
		{OrganizationID: orgA, InstanceNum: 1}: 20,
		{OrganizationID: orgB, InstanceNum: 1}: 25,
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
	tracker := NewMetadataTracker("test-topic", "test-group")

	orgA := uuid.New()
	orgB := uuid.New()

	// Set some already committed offsets
	tracker.lastCommittedOffsets[0] = 15
	tracker.lastCommittedOffsets[1] = 30

	// Current processed offsets
	tracker.partitionOffsets[0] = map[OrgInstanceKey]int64{
		{OrganizationID: orgA, InstanceNum: 1}: 10, // Behind committed offset
		{OrganizationID: orgB, InstanceNum: 1}: 12, // Behind committed offset
	}
	tracker.partitionOffsets[1] = map[OrgInstanceKey]int64{
		{OrganizationID: orgA, InstanceNum: 1}: 35, // Ahead of committed offset
		{OrganizationID: orgB, InstanceNum: 1}: 40, // Ahead of committed offset
	}

	commitData := tracker.GetSafeCommitOffsets()

	// Should only return partition 1 since partition 0 can't advance (min=10 < committed=15)
	assert.NotNil(t, commitData)
	assert.Len(t, commitData.Offsets, 1)
	assert.Equal(t, int64(35), commitData.Offsets[1]) // min(35, 40) = 35
}

func TestMetadataTracker_NoAdvancement(t *testing.T) {
	tracker := NewMetadataTracker("test-topic", "test-group")

	orgA := uuid.New()

	// Set committed offset higher than current processed
	tracker.lastCommittedOffsets[0] = 50
	tracker.partitionOffsets[0] = map[OrgInstanceKey]int64{
		{OrganizationID: orgA, InstanceNum: 1}: 30,
	}

	commitData := tracker.GetSafeCommitOffsets()

	// Should return nil since no offsets can advance
	assert.Nil(t, commitData)
}

func TestMetadataTracker_MarkOffsetsCommitted(t *testing.T) {
	tracker := NewMetadataTracker("test-topic", "test-group")

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
	tracker := NewMetadataTracker("test-topic", "test-group")

	orgA := uuid.New()
	orgB := uuid.New()

	// Create a group with multiple messages
	group := &AccumulationGroup[CompactionKey]{
		Key: CompactionKey{
			OrganizationID: orgA,
			InstanceNum:    1,
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
	orgInstanceKeyA := OrgInstanceKey{OrganizationID: orgA, InstanceNum: 1}

	assert.Contains(t, tracker.partitionOffsets[0], orgInstanceKeyA)
	assert.Equal(t, int64(105), tracker.partitionOffsets[0][orgInstanceKeyA]) // highest offset on partition 0

	assert.Contains(t, tracker.partitionOffsets[1], orgInstanceKeyA)
	assert.Equal(t, int64(200), tracker.partitionOffsets[1][orgInstanceKeyA]) // offset on partition 1

	// Track metadata from another org on the same partitions
	group2 := &AccumulationGroup[CompactionKey]{
		Key: CompactionKey{
			OrganizationID: orgB,
			InstanceNum:    1,
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

	orgInstanceKeyB := OrgInstanceKey{OrganizationID: orgB, InstanceNum: 1}

	// Both orgs should be tracked separately
	assert.Contains(t, tracker.partitionOffsets[0], orgInstanceKeyA)
	assert.Contains(t, tracker.partitionOffsets[0], orgInstanceKeyB)
	assert.Equal(t, int64(105), tracker.partitionOffsets[0][orgInstanceKeyA])
	assert.Equal(t, int64(90), tracker.partitionOffsets[0][orgInstanceKeyB])
}
