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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKey for testing purposes
type TestKey struct {
	OrganizationID uuid.UUID
	InstanceNum    int16
	DateInt        int32
	FrequencyMs    int32
	SlotID         int32
	SlotCount      int32
}

// Helper function to create a TestKey from a message
func testKeyMapper(msg *messages.MetricSegmentNotificationMessage) TestKey {
	return TestKey{
		OrganizationID: msg.OrganizationID,
		InstanceNum:    msg.InstanceNum,
		DateInt:        msg.DateInt,
		FrequencyMs:    msg.FrequencyMs,
		SlotID:         msg.SlotID,
		SlotCount:      msg.SlotCount,
	}
}

func createTestMessage(orgID uuid.UUID, instanceNum int16, dateInt int32, frequencyMs int32, slotID int32, slotCount int32, recordCount int64) *messages.MetricSegmentNotificationMessage {
	return &messages.MetricSegmentNotificationMessage{
		OrganizationID: orgID,
		DateInt:        dateInt,
		FrequencyMs:    frequencyMs,
		SegmentID:      123,
		InstanceNum:    instanceNum,
		SlotID:         slotID,
		SlotCount:      slotCount,
		RecordCount:    recordCount,
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

func TestHunter_AddMessage_BelowThreshold(t *testing.T) {
	hunter := NewHunter(testKeyMapper)
	orgID := uuid.New()

	msg := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata := createTestMetadata("metrics", 0, "test-group", 100)

	result := hunter.AddMessage(msg, metadata, 100)

	assert.Nil(t, result)

	assert.Len(t, hunter.groups, 1)

	for _, group := range hunter.groups {
		assert.Len(t, group.Messages, 1)
		assert.Equal(t, int64(50), group.TotalRecordCount)
		assert.Equal(t, int64(100), group.LatestOffsets["metrics"][0])
	}
}

func TestHunter_AddMessage_ExceedsThreshold(t *testing.T) {
	hunter := NewHunter(testKeyMapper)
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
	assert.Equal(t, int64(101), group.LatestOffsets["metrics"][0])

	// Group should be removed from hunter after being returned
	assert.Len(t, hunter.groups, 0)
}

func TestHunter_AddMessage_DifferentKeys(t *testing.T) {
	hunter := NewHunter(testKeyMapper)
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
	hunter := NewHunter(testKeyMapper)
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
		assert.Equal(t, int64(100), group.LatestOffsets["metrics"][0])
		assert.Equal(t, int64(200), group.LatestOffsets["metrics"][1])
	}
}

func TestHunter_AddMessage_OffsetTracking(t *testing.T) {
	hunter := NewHunter(testKeyMapper)
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
		assert.Equal(t, int64(150), group.LatestOffsets["metrics"][0])
	}
}

func TestHunter_AddMessage_MultipleTopics(t *testing.T) {
	hunter := NewHunter(testKeyMapper)
	orgID := uuid.New()

	msg1 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata1 := createTestMetadata("metrics", 0, "group1", 100)
	hunter.AddMessage(msg1, metadata1, 200)

	msg2 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata2 := createTestMetadata("traces", 1, "group2", 200)
	hunter.AddMessage(msg2, metadata2, 200)

	assert.Len(t, hunter.groups, 1)

	for _, group := range hunter.groups {
		assert.Len(t, group.Messages, 2)
		assert.Equal(t, int64(100), group.LatestOffsets["metrics"][0])
		assert.Equal(t, int64(200), group.LatestOffsets["traces"][1])
	}
}

func TestHunter_TestKey_Equality(t *testing.T) {
	orgID := uuid.New()

	key1 := TestKey{
		OrganizationID: orgID,
		InstanceNum:    1,
		DateInt:        20250108,
		FrequencyMs:    60000,
		SlotID:         1,
		SlotCount:      4,
	}

	key2 := TestKey{
		OrganizationID: orgID,
		InstanceNum:    1,
		DateInt:        20250108,
		FrequencyMs:    60000,
		SlotID:         1,
		SlotCount:      4,
	}

	key3 := TestKey{
		OrganizationID: orgID,
		InstanceNum:    2, // Different instance
		DateInt:        20250108,
		FrequencyMs:    60000,
		SlotID:         1,
		SlotCount:      4,
	}

	assert.Equal(t, key1, key2)
	assert.NotEqual(t, key1, key3)
}

func TestHunter_FirstMessageExceedsThreshold(t *testing.T) {
	hunter := NewHunter(testKeyMapper)
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
	hunter := NewHunter(testKeyMapper)

	assert.NotNil(t, hunter)
	assert.NotNil(t, hunter.groups)
	assert.Len(t, hunter.groups, 0)
}

func TestHunter_SelectGroups(t *testing.T) {
	hunter := NewHunter(testKeyMapper)
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
	selectedGroups := hunter.SelectGroups(func(key TestKey, group *AccumulationGroup[TestKey]) bool {
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
	hunter := NewHunter(testKeyMapper)
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
	selectedGroups := hunter.SelectGroups(func(key TestKey, group *AccumulationGroup[TestKey]) bool {
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
	hunter := NewHunter(testKeyMapper)
	orgID := uuid.New()

	msg := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata := createTestMetadata("metrics", 0, "test-group", 100)
	hunter.AddMessage(msg, metadata, 200)

	// Select with criteria that matches nothing
	selectedGroups := hunter.SelectGroups(func(key TestKey, group *AccumulationGroup[TestKey]) bool {
		return group.TotalRecordCount > 1000
	})

	// Should return empty slice
	assert.Len(t, selectedGroups, 0)

	// Should leave all groups in hunter
	assert.Len(t, hunter.groups, 1)
}

func TestHunter_CustomKeyType(t *testing.T) {
	// Example using a string key instead of HunterKey
	stringKeyMapper := func(msg *messages.MetricSegmentNotificationMessage) string {
		return msg.OrganizationID.String() + "-" + fmt.Sprintf("%d", msg.InstanceNum)
	}

	hunter := NewHunter(stringKeyMapper)
	orgID := uuid.New()

	msg1 := createTestMessage(orgID, 1, 20250108, 60000, 1, 4, 50)
	metadata1 := createTestMetadata("metrics", 0, "test-group", 100)
	hunter.AddMessage(msg1, metadata1, 200)

	msg2 := createTestMessage(orgID, 2, 20250108, 60000, 1, 4, 40)
	metadata2 := createTestMetadata("metrics", 0, "test-group", 101)
	hunter.AddMessage(msg2, metadata2, 200)

	assert.Len(t, hunter.groups, 2)

	// Select using string key logic
	selectedGroups := hunter.SelectGroups(func(key string, group *AccumulationGroup[string]) bool {
		return strings.HasSuffix(key, "-1") // Select keys ending with "-1"
	})

	assert.Len(t, selectedGroups, 1)
	assert.Len(t, hunter.groups, 1)

	// Verify the selected group has the right key format
	assert.Equal(t, orgID.String()+"-1", selectedGroups[0].Key)
}

func TestHunter_SelectStaleGroups(t *testing.T) {
	keyMapper := func(msg *messages.MetricSegmentNotificationMessage) TestKey {
		return TestKey{
			OrganizationID: msg.OrganizationID,
			DateInt:        msg.DateInt,
		}
	}
	hunter := NewHunter(keyMapper)

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
	keyA := TestKey{OrganizationID: orgA, DateInt: 20250108}
	hunter.groups[keyA].CreatedAt = now.Add(-10 * time.Minute)
	hunter.groups[keyA].LastUpdatedAt = now.Add(-10 * time.Minute)

	// GroupB: 2 minutes old (fresh)
	keyB := TestKey{OrganizationID: orgB, DateInt: 20250108}
	hunter.groups[keyB].CreatedAt = now.Add(-2 * time.Minute)
	hunter.groups[keyB].LastUpdatedAt = now.Add(-2 * time.Minute)

	// GroupC: 7 minutes old (stale)
	keyC := TestKey{OrganizationID: orgC, DateInt: 20250108}
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
	keyMapper := func(msg *messages.MetricSegmentNotificationMessage) TestKey {
		return TestKey{
			OrganizationID: msg.OrganizationID,
			DateInt:        msg.DateInt,
		}
	}
	hunter := NewHunter(keyMapper)

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
	keyMapper := func(msg *messages.MetricSegmentNotificationMessage) TestKey {
		return TestKey{
			OrganizationID: msg.OrganizationID,
			DateInt:        msg.DateInt,
		}
	}
	hunter := NewHunter(keyMapper)

	// No groups in hunter
	assert.Len(t, hunter.groups, 0)

	// Select stale groups from empty hunter
	staleGroups := hunter.SelectStaleGroups(5 * time.Minute)

	// Should return empty slice
	assert.Len(t, staleGroups, 0)
}
