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

func TestHunter_AddMessage_BelowThreshold(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID := uuid.New()

	msg := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata := createTestMetadata("metrics", 0, "test-group", 100)

	result := hunter.AddMessage(msg, metadata, 100)

	assert.Nil(t, result)

	assert.Len(t, hunter.groups, 1)

	for _, group := range hunter.groups {
		assert.Len(t, group.Messages, 1)
		assert.Equal(t, int64(50), group.TotalRecordCount)
		assert.Equal(t, int64(100), group.LatestOffsets[0])
	}
}

func TestHunter_AddMessage_ExceedsThreshold(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID := uuid.New()

	// Add first message (50 records)
	msg1 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata1 := createTestMetadata("metrics", 0, "test-group", 100)
	result1 := hunter.AddMessage(msg1, metadata1, 100)
	assert.Nil(t, result1)

	// Add second message that will exceed threshold (60 records, total = 110)
	msg2 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 60)
	metadata2 := createTestMetadata("metrics", 0, "test-group", 101)
	result2 := hunter.AddMessage(msg2, metadata2, 100)

	require.NotNil(t, result2)
	assert.Equal(t, msg2, result2.TriggeringRecord.Message)
	assert.Equal(t, metadata2, result2.TriggeringRecord.Metadata)

	group := result2.Group
	assert.Len(t, group.Messages, 2)
	assert.Equal(t, int64(110), group.TotalRecordCount)
	assert.Equal(t, int64(101), group.LatestOffsets[0])

	// Group should be removed from hunter after being returned
	assert.Len(t, hunter.groups, 0)
}

func TestHunter_AddMessage_DifferentKeys(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID1 := uuid.New()
	orgID2 := uuid.New()

	// Messages with different organization IDs should create separate groups
	msg1 := createTestMessage(orgID1, 1, 20250108, 60000, 1, 4, 50)
	metadata1 := createTestMetadata("metrics", 0, "test-group", 100)
	result1 := hunter.AddMessage(msg1, metadata1, 100)
	assert.Nil(t, result1)

	msg2 := createTestMessage(orgID2, 1, 20250108, 60000, 1, 4, 50)
	metadata2 := createTestMetadata("metrics", 0, "test-group", 101)
	result2 := hunter.AddMessage(msg2, metadata2, 100)
	assert.Nil(t, result2)

	assert.Len(t, hunter.groups, 2)
}

func TestHunter_AddMessage_SameKeyDifferentPartitions(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID := uuid.New()

	// Messages with same accumulation key but different partitions
	msg1 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata1 := createTestMetadata("metrics", 0, "test-group", 100)
	result1 := hunter.AddMessage(msg1, metadata1, 200)
	assert.Nil(t, result1)

	msg2 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 60)
	metadata2 := createTestMetadata("metrics", 1, "test-group", 200)
	result2 := hunter.AddMessage(msg2, metadata2, 200)
	assert.Nil(t, result2)

	assert.Len(t, hunter.groups, 1)

	for _, group := range hunter.groups {
		assert.Len(t, group.Messages, 2)
		assert.Equal(t, int64(110), group.TotalRecordCount)
		assert.Equal(t, int64(100), group.LatestOffsets[0])
		assert.Equal(t, int64(200), group.LatestOffsets[1])
	}
}

func TestHunter_AddMessage_OffsetTracking(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID := uuid.New()

	// Add message with offset 100
	msg1 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 30)
	metadata1 := createTestMetadata("metrics", 0, "test-group", 100)
	hunter.AddMessage(msg1, metadata1, 200)

	// Add message with higher offset 150
	msg2 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 30)
	metadata2 := createTestMetadata("metrics", 0, "test-group", 150)
	hunter.AddMessage(msg2, metadata2, 200)

	// Add message with lower offset 120 (should not update latest offset)
	msg3 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 30)
	metadata3 := createTestMetadata("metrics", 0, "test-group", 120)
	hunter.AddMessage(msg3, metadata3, 200)

	assert.Len(t, hunter.groups, 1)

	for _, group := range hunter.groups {
		assert.Equal(t, int64(150), group.LatestOffsets[0])
	}
}

func TestHunter_AddMessage_DifferentPartitions(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID := uuid.New()

	msg1 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata1 := createTestMetadata("metrics", 0, "group1", 100)
	hunter.AddMessage(msg1, metadata1, 200)

	msg2 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata2 := createTestMetadata("metrics", 1, "group1", 200) // Same topic, different partition
	hunter.AddMessage(msg2, metadata2, 200)

	assert.Len(t, hunter.groups, 1)

	for _, group := range hunter.groups {
		assert.Len(t, group.Messages, 2)
		// Both partitions should be tracked
		assert.Equal(t, int64(100), group.LatestOffsets[0]) // Partition 0
		assert.Equal(t, int64(200), group.LatestOffsets[1]) // Partition 1
	}
}

func TestHunter_TestKey_Equality(t *testing.T) {
	orgID := uuid.New()

	key1 := messages.CompactionKey{
		OrganizationID: orgID,
		InstanceNum:    1,
		DateInt:        20250108,
		FrequencyMs:    60000,
	}

	key2 := messages.CompactionKey{
		OrganizationID: orgID,
		InstanceNum:    1,
		DateInt:        20250108,
		FrequencyMs:    60000,
	}

	key3 := messages.CompactionKey{
		OrganizationID: orgID,
		InstanceNum:    2, // Different instance
		DateInt:        20250108,
		FrequencyMs:    60000,
	}

	assert.Equal(t, key1, key2)
	assert.NotEqual(t, key1, key3)
}

func TestHunter_FirstMessageExceedsThreshold(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID := uuid.New()

	// First message already exceeds threshold, but should not be returned
	// since shouldReturn checks len(group.Messages) > 0 before adding
	msg := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 150)
	metadata := createTestMetadata("metrics", 0, "test-group", 100)
	result := hunter.AddMessage(msg, metadata, 100)

	// Should not return since this is the first message in the group
	assert.Nil(t, result)

	assert.Len(t, hunter.groups, 1)

	for _, group := range hunter.groups {
		assert.Len(t, group.Messages, 1)
		assert.Equal(t, int64(150), group.TotalRecordCount)
	}
}

func TestHunter_NewHunter(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()

	assert.NotNil(t, hunter)
	assert.NotNil(t, hunter.groups)
	assert.Len(t, hunter.groups, 0)
}

func TestHunter_SelectGroups(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID1 := uuid.New()
	orgID2 := uuid.New()

	// Add messages for two different organizations
	msg1 := createTestMessage(orgID1, 1, 20250108, 60000, 1, 4, 50)
	metadata1 := createTestMetadata("metrics", 0, "test-group", 100)
	hunter.AddMessage(msg1, metadata1, 200)

	msg2 := createTestMessage(orgID2, 1, 20250108, 60000, 1, 4, 40)
	metadata2 := createTestMetadata("metrics", 0, "test-group", 101)
	hunter.AddMessage(msg2, metadata2, 200)

	msg3 := createTestMessage(orgID1, 2, 20250108, 60000, 1, 4, 30) // Different instance
	metadata3 := createTestMetadata("metrics", 0, "test-group", 102)
	hunter.AddMessage(msg3, metadata3, 200)

	assert.Len(t, hunter.groups, 3)

	// Select only groups from orgID1
	selectedGroups := hunter.SelectGroups(func(key messages.CompactionKey, group *AccumulationGroup[messages.CompactionKey]) bool {
		return key.OrganizationID == orgID1
	})

	// Should return 2 groups (orgID1 with instance 1 and 2)
	assert.Len(t, selectedGroups, 2)

	// Should remove selected groups from hunter, leaving only orgID2 group
	assert.Len(t, hunter.groups, 1)

	// Verify the remaining group is for orgID2
	for _, group := range hunter.groups {
		assert.Equal(t, orgID2, group.Key.OrganizationID)
	}

	// Verify returned groups are for orgID1
	for _, group := range selectedGroups {
		assert.Equal(t, orgID1, group.Key.OrganizationID)
	}
}

func TestHunter_SelectGroups_ByRecordCount(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID := uuid.New()

	// Add messages with different record counts
	msg1 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 100)
	metadata1 := createTestMetadata("metrics", 0, "test-group", 100)
	hunter.AddMessage(msg1, metadata1, 200)

	msg2 := createTestMessage(orgID, 2, 20250108, 60000, 1, 4, 50)
	metadata2 := createTestMetadata("metrics", 0, "test-group", 101)
	hunter.AddMessage(msg2, metadata2, 200)

	msg3 := createTestMessage(orgID, 3, 20250108, 60000, 1, 4, 25)
	metadata3 := createTestMetadata("metrics", 0, "test-group", 102)
	hunter.AddMessage(msg3, metadata3, 200)

	assert.Len(t, hunter.groups, 3)

	// Select only groups with record count >= 50
	selectedGroups := hunter.SelectGroups(func(key messages.CompactionKey, group *AccumulationGroup[messages.CompactionKey]) bool {
		return group.TotalRecordCount >= 50
	})

	// Should return 2 groups (100 and 50 record counts)
	assert.Len(t, selectedGroups, 2)

	// Should leave 1 group with 25 records
	assert.Len(t, hunter.groups, 1)
	for _, group := range hunter.groups {
		assert.Equal(t, int64(25), group.TotalRecordCount)
	}
}

func TestHunter_SelectGroups_NoMatches(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID := uuid.New()

	msg := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata := createTestMetadata("metrics", 0, "test-group", 100)
	hunter.AddMessage(msg, metadata, 200)

	// Select with criteria that matches nothing
	selectedGroups := hunter.SelectGroups(func(key messages.CompactionKey, group *AccumulationGroup[messages.CompactionKey]) bool {
		return group.TotalRecordCount > 1000
	})

	// Should return empty slice
	assert.Len(t, selectedGroups, 0)

	// Should leave all groups in hunter
	assert.Len(t, hunter.groups, 1)
}

func TestHunter_SelectStaleGroups(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()

	orgA := uuid.New()
	orgB := uuid.New()
	orgC := uuid.New()

	// Create messages with different timestamps by manipulating the hunter's group timestamps
	msgA := createTestMessage(orgA, 1, 20250108, 60000, 1, 4, 100)
	msgB := createTestMessage(orgB, 1, 20250108, 60000, 1, 4, 100)
	msgC := createTestMessage(orgC, 1, 20250108, 60000, 1, 4, 100)

	// Add messages to create groups
	hunter.AddMessage(msgA, &MessageMetadata{Topic: "test", Partition: 0, Offset: 1}, 10000)
	hunter.AddMessage(msgB, &MessageMetadata{Topic: "test", Partition: 0, Offset: 2}, 10000)
	hunter.AddMessage(msgC, &MessageMetadata{Topic: "test", Partition: 0, Offset: 3}, 10000)

	// Verify all groups were created
	assert.Len(t, hunter.groups, 3)

	// Manually set timestamps to simulate different ages
	now := time.Now()

	// GroupA: 10 minutes old (stale)
	keyA := messages.CompactionKey{OrganizationID: orgA, InstanceNum: 1, DateInt: 20250108, FrequencyMs: 60000}
	hunter.groups[keyA].CreatedAt = now.Add(-10 * time.Minute)
	hunter.groups[keyA].LastUpdatedAt = now.Add(-10 * time.Minute)

	// GroupB: 2 minutes old (fresh)
	keyB := messages.CompactionKey{OrganizationID: orgB, InstanceNum: 1, DateInt: 20250108, FrequencyMs: 60000}
	hunter.groups[keyB].CreatedAt = now.Add(-2 * time.Minute)
	hunter.groups[keyB].LastUpdatedAt = now.Add(-2 * time.Minute)

	// GroupC: 7 minutes old (stale)
	keyC := messages.CompactionKey{OrganizationID: orgC, InstanceNum: 1, DateInt: 20250108, FrequencyMs: 60000}
	hunter.groups[keyC].CreatedAt = now.Add(-7 * time.Minute)
	hunter.groups[keyC].LastUpdatedAt = now.Add(-7 * time.Minute)

	// Select groups older than 5 minutes
	staleGroups := hunter.SelectStaleGroups(5 * time.Minute)

	// Should return groups A and C (both older than 5 minutes)
	assert.Len(t, staleGroups, 2)

	// Groups should be removed from hunter
	assert.Len(t, hunter.groups, 1)

	// Only group B should remain (the fresh one)
	_, remainsB := hunter.groups[keyB]
	assert.True(t, remainsB)

	// Verify the returned groups are the stale ones
	staleOrgIDs := make(map[uuid.UUID]bool)
	for _, group := range staleGroups {
		staleOrgIDs[group.Key.OrganizationID] = true
	}
	assert.True(t, staleOrgIDs[orgA]) // GroupA should be in stale results
	assert.True(t, staleOrgIDs[orgC]) // GroupC should be in stale results
}

func TestHunter_SelectStaleGroups_NoStaleGroups(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()

	orgA := uuid.New()
	msgA := createTestMessage(orgA, 1, 20250108, 60000, 1, 4, 100)

	// Add message to create a group
	hunter.AddMessage(msgA, &MessageMetadata{Topic: "test", Partition: 0, Offset: 1}, 10000)

	// Group is fresh (just created)
	assert.Len(t, hunter.groups, 1)

	// Select groups older than 5 minutes (should find none)
	staleGroups := hunter.SelectStaleGroups(5 * time.Minute)

	// Should return no groups
	assert.Len(t, staleGroups, 0)

	// Original group should remain
	assert.Len(t, hunter.groups, 1)
}

func TestHunter_SelectStaleGroups_EmptyHunter(t *testing.T) {
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()

	// No groups in hunter
	assert.Len(t, hunter.groups, 0)

	// Select stale groups from empty hunter
	staleGroups := hunter.SelectStaleGroups(5 * time.Minute)

	// Should return empty slice
	assert.Len(t, staleGroups, 0)
}

// Helper function for creating MetricCompactionMessage with varying fields
func createCompactionMessage(orgID uuid.UUID, segmentID int64, instanceNum int16, slotID, slotCount int32, records int64) *messages.MetricCompactionMessage {
	return &messages.MetricCompactionMessage{
		Version:        1,
		OrganizationID: orgID,
		DateInt:        20250909,
		FrequencyMs:    60000,
		SegmentID:      segmentID,
		InstanceNum:    instanceNum,
		SlotID:         slotID,
		SlotCount:      slotCount,
		Records:        records,
		FileSize:       records * 50, // Realistic ratio: ~50 bytes per record
		QueuedAt:       time.Now(),
	}
}

// Short helper for creating MetricCompactionMessage
func compactMessage(orgID uuid.UUID, segmentID int64, instanceNum int16, slotID, slotCount int32, records int64) *messages.MetricCompactionMessage {
	return createCompactionMessage(orgID, segmentID, instanceNum, slotID, slotCount, records)
}

// Helper function for creating Kafka metadata with varying offset
func createKafkaMetadata(offset int64) *MessageMetadata {
	return &MessageMetadata{
		Topic:         "lakerunner.segments.metrics.compact",
		Partition:     0,
		ConsumerGroup: "lakerunner.compact.metrics",
		Offset:        offset,
	}
}

// Short helper for creating Kafka metadata
func kmeta(offset int64) *MessageMetadata {
	return createKafkaMetadata(offset)
}

func TestHunter_MetricCompactionMessage_AccumulationScaffolding(t *testing.T) {
	// Create Hunter using the same types as metric compaction consumer
	hunter := NewHunter[*messages.MetricCompactionMessage, messages.CompactionKey]()
	orgID := uuid.New()
	targetRecordCount := int64(10000) // 10k threshold

	// Expected grouping key for all messages
	expectedKey := messages.CompactionKey{
		OrganizationID: orgID,
		InstanceNum:    15,
		DateInt:        20250909,
		FrequencyMs:    60000,
	}

	result1 := hunter.AddMessage(compactMessage(orgID, 123456789, 15, 1, 4, 4000), kmeta(100), targetRecordCount)
	assert.Nil(t, result1, "First message should not trigger (below threshold)")
	assert.Len(t, hunter.groups, 1, "Should have exactly one group after first message")

	group := hunter.groups[expectedKey]
	require.NotNil(t, group, "Group should exist")
	assert.Len(t, group.Messages, 1, "Group should have 1 message")
	assert.Equal(t, int64(4000), group.TotalRecordCount, "Group should have 4000 records")
	assert.Equal(t, int64(100), group.LatestOffsets[0], "Should track offset 100")

	result2 := hunter.AddMessage(compactMessage(orgID, 123456790, 15, 1, 4, 4000), kmeta(101), targetRecordCount)
	assert.Nil(t, result2, "Second message should not trigger (still below threshold)")
	assert.Len(t, hunter.groups, 1, "Should have exactly one group after second message")

	group = hunter.groups[expectedKey]
	assert.Len(t, group.Messages, 2, "Group should have 2 messages")
	assert.Equal(t, int64(8000), group.TotalRecordCount, "Group should have 8000 total records (4000+4000)")
	assert.Equal(t, int64(101), group.LatestOffsets[0], "Should track latest offset 101")

	result3 := hunter.AddMessage(compactMessage(orgID, 123456791, 15, 1, 4, 4000), kmeta(102), targetRecordCount)
	require.NotNil(t, result3, "Third message should trigger processing (exceeds threshold)")

	// Check returned group state
	returnedGroup := result3.Group
	assert.Len(t, returnedGroup.Messages, 3, "Returned group should have all 3 messages")
	assert.Equal(t, int64(12000), returnedGroup.TotalRecordCount, "Returned group should have 12000 total records")
	assert.Equal(t, int64(102), returnedGroup.LatestOffsets[0], "Returned group should have latest offset")
	assert.Equal(t, expectedKey, returnedGroup.Key, "Returned group should have correct key")
	assert.Len(t, hunter.groups, 0, "Should have exactly 0 groups after group emission")

	// Check triggering record - should be the last message added
	assert.Equal(t, int64(4000), result3.TriggeringRecord.Message.RecordCount(), "Triggering message should have 4000 records")
	assert.Equal(t, int64(102), result3.TriggeringRecord.Metadata.Offset, "Triggering metadata should have offset 102")

	// Check Hunter state after processing
	assert.Len(t, hunter.groups, 0, "Hunter should be empty after group is returned")

	// Now batch-add 15 too-small messages to not exceed threshold, and ensure that we have one group
	// at the end.
	for i := range 15 {
		result := hunter.AddMessage(compactMessage(orgID, int64(200000000+i), 15, 1, 4, 100), kmeta(int64(200+i)), targetRecordCount)
		assert.Nil(t, result, "Message %d should not trigger (%d < 10000)", i+1, (i+1)*100)
	}
	assert.Len(t, hunter.groups, 1, "Hunter should have exactly one group after batch addition of small messages")
	// now, cause our periodic flush to pick it up
	staleGroups := hunter.SelectStaleGroups(0 * time.Minute)
	assert.Len(t, staleGroups, 1, "Should have exactly one stale group after flush")
	assert.Len(t, hunter.groups, 0, "Hunter should be empty after stale group is returned")

	// Now, add the same 15 records for 4 orgs, using a different instance num for each org.  Ensure at the end we have 4 groups
	// with the proper number of messages each.  Then, flush them all out as stale and confirm we got them back correctly.
	orgIDs := []uuid.UUID{uuid.New(), uuid.New(), uuid.New(), uuid.New()}
	instanceNums := []int16{10, 20, 30, 40}

	for orgIdx, testOrgID := range orgIDs {
		for i := range 15 {
			result := hunter.AddMessage(compactMessage(testOrgID, int64(300000000+orgIdx*1000+i), instanceNums[orgIdx], 1, 4, 100), kmeta(int64(300+orgIdx*100+i)), targetRecordCount)
			assert.Nil(t, result, "Org %d message %d should not trigger", orgIdx+1, i+1)
		}
	}

	assert.Len(t, hunter.groups, 4, "Hunter should have exactly 4 groups (one per org)")

	// Verify each group has 15 messages and correct record count
	for orgIdx, testOrgID := range orgIDs {
		expectedKey := messages.CompactionKey{
			OrganizationID: testOrgID,
			InstanceNum:    instanceNums[orgIdx],
			DateInt:        20250909,
			FrequencyMs:    60000,
		}
		group := hunter.groups[expectedKey]
		require.NotNil(t, group, "Group for org %d should exist", orgIdx+1)
		assert.Len(t, group.Messages, 15, "Group for org %d should have 15 messages", orgIdx+1)
		assert.Equal(t, int64(1500), group.TotalRecordCount, "Group for org %d should have 1500 records", orgIdx+1)
	}

	// Flush all groups as stale
	allStaleGroups := hunter.SelectStaleGroups(0 * time.Minute)
	assert.Len(t, allStaleGroups, 4, "Should have exactly 4 stale groups after flush")
	assert.Len(t, hunter.groups, 0, "Hunter should be empty after all stale groups are returned")

	// Verify returned groups have correct data
	orgCounts := make(map[uuid.UUID]int)
	for _, group := range allStaleGroups {
		orgCounts[group.Key.OrganizationID]++
		assert.Len(t, group.Messages, 15, "Each returned group should have 15 messages")
		assert.Equal(t, int64(1500), group.TotalRecordCount, "Each returned group should have 1500 records")
	}
	assert.Len(t, orgCounts, 4, "Should have groups for all 4 organizations")

	t.Logf("Multi-org accumulation completed: 4 orgs × 15 messages × 100 records = %d total records", 4*15*100)
}
