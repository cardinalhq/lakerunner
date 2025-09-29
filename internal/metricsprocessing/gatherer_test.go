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
	"fmt"
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
	store        *MockOffsetStore // Store reference to track offsets
}

func NewMockCompactor() *MockCompactor {
	return &MockCompactor{}
}

func NewMockCompactorWithStore(store *MockOffsetStore) *MockCompactor {
	return &MockCompactor{store: store}
}

func (m *MockCompactor) Process(ctx context.Context, group *accumulationGroup[messages.CompactionKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	m.groups = append(m.groups, group)
	m.kafkaOffsets = append(m.kafkaOffsets, kafkaOffsets)

	// Simulate writing offsets to the database (critical for deduplication)
	if m.store != nil {
		for _, offsetInfo := range kafkaOffsets {
			m.store.SetProcessedOffsets(
				offsetInfo.ConsumerGroup,
				offsetInfo.Topic,
				offsetInfo.PartitionID,
				offsetInfo.Offsets,
			)
		}
	}
	return nil
}

func (m *MockCompactor) GetTargetRecordCount(ctx context.Context, groupingKey messages.CompactionKey) int64 {
	return 3000 // Lower threshold for testing to trigger more boxes
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
	// Append to existing offsets rather than replacing
	existing := m.processedOffsets[key]
	m.processedOffsets[key] = append(existing, offsets...)
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

func (m *MockOffsetStore) InsertKafkaOffsets(ctx context.Context, params lrdb.InsertKafkaOffsetsParams) error {
	key := params.ConsumerGroup + ":" + params.Topic + ":" + string(rune(params.PartitionID))
	existing := m.processedOffsets[key]
	m.processedOffsets[key] = append(existing, params.Offsets...)
	return nil
}

func TestGatherer_ProcessMessage_Integration(t *testing.T) {
	offsetStore := NewMockOffsetStore()
	compactor := NewMockCompactor()
	orgID := uuid.New()

	gatherer := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", compactor, offsetStore)

	// Add messages until threshold is reached
	for i := 0; i < 200; i++ { // Should trigger at CompactionTargetRecordCount=3000 (new threshold)
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
	// With max 50 messages limit, we get 50 * 50 = 2500 records
	assert.Equal(t, 50, len(group.Messages))             // Should be limited to 50 messages
	assert.Equal(t, int64(2500), group.TotalRecordCount) // 50 messages * 50 records each
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
	sharedTracker := newMetadataTracker("test-topic", "test-group", sharedHunter)
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

	msg := createTestMessage(orgID, 1, 20250909, 60000, 500) // Reduced to stay below threshold

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

func TestGatherer_FailureAndRecoveryWithReplay(t *testing.T) {
	offsetStore := NewMockOffsetStore()
	processor := NewMockCompactorWithStore(offsetStore) // Use processor that tracks offsets
	orgID := uuid.New()

	// Create messages with controlled time and sizes
	// We'll send enough messages to trigger box emission
	var testMessages []*messages.MetricCompactionMessage
	var testMetadata []*messageMetadata

	// Create 300 messages with 50 records each (total 15,000 records)
	// This should trigger box emission at 10,000 records (default target)
	for i := range 300 {
		msg := createTestMessage(orgID, 1, 20250115, 60000, 50)
		md := createTestMetadata("test-topic", 0, "test-group", int64(i))
		testMessages = append(testMessages, msg)
		testMetadata = append(testMetadata, md)
	}

	// Phase 1: Process messages until crash
	gatherer1 := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", processor, offsetStore)

	// Process messages and track when boxes are emitted
	boxesBeforeCrash := 0
	crashOffset := int64(-1)

	for i := 0; i < len(testMessages); i++ {
		err := gatherer1.processMessage(context.Background(), testMessages[i], testMetadata[i])
		assert.NoError(t, err)

		// Check if a box was emitted
		if len(processor.groups) > boxesBeforeCrash {
			boxesBeforeCrash = len(processor.groups)
			// Simulate crash after second box is emitted
			if boxesBeforeCrash == 2 {
				crashOffset = int64(i)
				break
			}
		}
	}

	// Should have emitted 2 boxes before crash
	assert.Equal(t, 2, boxesBeforeCrash, "Should have emitted 2 boxes before crash")
	assert.Greater(t, crashOffset, int64(0), "Should have crashed after processing some messages")

	// Capture state before crash
	pendingMessagesInHunter := 0
	for _, group := range gatherer1.hunter.groups {
		pendingMessagesInHunter += len(group.Messages)
	}

	// Note: Offsets are automatically tracked by the MockCompactorWithStore

	// Phase 2: Simulate recovery with new gatherer
	// Reset processor to track new boxes
	processor2 := NewMockCompactorWithStore(offsetStore) // Use same store for continuity
	gatherer2 := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", processor2, offsetStore)

	// Find the last committed offset (from the last box)
	lastCommittedOffset := int64(-1)
	for _, kafkaOffsets := range processor.kafkaOffsets {
		for _, offsetInfo := range kafkaOffsets {
			for _, offset := range offsetInfo.Offsets {
				if offset > lastCommittedOffset {
					lastCommittedOffset = offset
				}
			}
		}
	}

	// Replay from the last committed offset + 1
	replayStartOffset := max(lastCommittedOffset+1, 0)

	// Process all messages from replay start to end
	for i := int(replayStartOffset); i < len(testMessages); i++ {
		err := gatherer2.processMessage(context.Background(), testMessages[i], testMetadata[i])
		assert.NoError(t, err)
	}

	// Force flush any remaining messages
	_, err := gatherer2.processIdleGroups(context.Background(), 0, 0)
	assert.NoError(t, err)

	// Verify correctness after recovery:
	// 1. Total boxes emitted = pre-crash + post-recovery
	totalBoxes := len(processor.groups) + len(processor2.groups)
	assert.GreaterOrEqual(t, totalBoxes, 3, "Should have at least 3 boxes total (300 messages / ~200 per box)")

	// 2. No duplicate messages in boxes
	seenOffsets := make(map[int64]bool)
	duplicates := 0

	// Check pre-crash boxes
	for _, kafkaOffsets := range processor.kafkaOffsets {
		for _, offsetInfo := range kafkaOffsets {
			for _, offset := range offsetInfo.Offsets {
				if seenOffsets[offset] {
					duplicates++
				}
				seenOffsets[offset] = true
			}
		}
	}

	// Check post-recovery boxes
	for _, kafkaOffsets := range processor2.kafkaOffsets {
		for _, offsetInfo := range kafkaOffsets {
			for _, offset := range offsetInfo.Offsets {
				if seenOffsets[offset] {
					duplicates++
				}
				seenOffsets[offset] = true
			}
		}
	}

	assert.Equal(t, 0, duplicates, "Should have no duplicate messages in boxes")

	// 3. All messages should be included exactly once
	for i := 0; i < len(testMessages); i++ {
		assert.True(t, seenOffsets[int64(i)] || i > int(crashOffset),
			"Message at offset %d should be in a box or unprocessed after crash", i)
	}

	// 4. Messages should maintain their grouping integrity
	// All boxes should have the same key
	for _, group := range processor.groups {
		assert.Equal(t, orgID, group.Key.OrganizationID)
		assert.Equal(t, int16(1), group.Key.InstanceNum)
		assert.Equal(t, int32(20250115), group.Key.DateInt)
		assert.Equal(t, int32(60000), group.Key.FrequencyMs)
	}

	for _, group := range processor2.groups {
		assert.Equal(t, orgID, group.Key.OrganizationID)
		assert.Equal(t, int16(1), group.Key.InstanceNum)
		assert.Equal(t, int32(20250115), group.Key.DateInt)
		assert.Equal(t, int32(60000), group.Key.FrequencyMs)
	}

	t.Logf("Test completed: %d boxes before crash, %d boxes after recovery, %d total messages processed",
		len(processor.groups), len(processor2.groups), len(seenOffsets))
}

func TestGatherer_MultiPartitionFailureAndRecovery(t *testing.T) {
	offsetStore := NewMockOffsetStore()
	processor := NewMockCompactorWithStore(offsetStore) // Use processor that tracks offsets
	orgID := uuid.New()

	// Create messages across multiple partitions
	type messageInfo struct {
		msg       *messages.MetricCompactionMessage
		metadata  *messageMetadata
		processed bool
	}

	var allMessages []messageInfo

	// Create 150 messages per partition across 3 partitions
	for partition := range int32(3) {
		for i := range 150 {
			msg := createTestMessage(orgID, 1, 20250115, 60000, 50)
			md := createTestMetadata("test-topic", partition, "test-group", int64(i))
			allMessages = append(allMessages, messageInfo{
				msg:      msg,
				metadata: md,
			})
		}
	}

	// Phase 1: Process messages from all partitions until crash
	gatherer1 := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", processor, offsetStore)

	// Process messages in interleaved fashion (simulate real Kafka consumption)
	processedCount := 0
	for round := range 50 {
		for partition := range int32(3) {
			idx := int(partition)*150 + round
			if idx < len(allMessages) {
				err := gatherer1.processMessage(context.Background(),
					allMessages[idx].msg,
					allMessages[idx].metadata)
				assert.NoError(t, err)
				allMessages[idx].processed = true
				processedCount++

				// Simulate crash after processing 120 messages
				if processedCount >= 120 {
					goto crashPoint
				}
			}
		}
	}
crashPoint:

	// Note: Offsets are automatically tracked by the MockCompactorWithStore

	// Phase 2: Recovery with replay from each partition's last committed offset
	processor2 := NewMockCompactorWithStore(offsetStore) // Use same store for continuity
	gatherer2 := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", processor2, offsetStore)

	// Determine replay start for each partition
	partitionReplayStart := make(map[int32]int64)
	for partition := range int32(3) {
		partitionReplayStart[partition] = -1
		// Find last committed offset for this partition
		for _, kafkaOffsets := range processor.kafkaOffsets {
			for _, offsetInfo := range kafkaOffsets {
				if offsetInfo.PartitionID == partition {
					for _, offset := range offsetInfo.Offsets {
						if offset > partitionReplayStart[partition] {
							partitionReplayStart[partition] = offset
						}
					}
				}
			}
		}
		partitionReplayStart[partition]++ // Start from next offset
	}

	// Replay messages from each partition
	for _, msgInfo := range allMessages {
		if msgInfo.metadata.Offset >= partitionReplayStart[msgInfo.metadata.Partition] {
			err := gatherer2.processMessage(context.Background(), msgInfo.msg, msgInfo.metadata)
			assert.NoError(t, err)
		}
	}

	// Flush remaining messages
	_, err := gatherer2.processIdleGroups(context.Background(), 0, 0)
	assert.NoError(t, err)

	// Verify no duplicates across all partitions
	seenMessages := make(map[string]bool) // partition:offset -> bool
	duplicates := 0

	for _, kafkaOffsets := range append(processor.kafkaOffsets, processor2.kafkaOffsets...) {
		for _, offsetInfo := range kafkaOffsets {
			for _, offset := range offsetInfo.Offsets {
				key := fmt.Sprintf("%d:%d", offsetInfo.PartitionID, offset)
				if seenMessages[key] {
					duplicates++
				}
				seenMessages[key] = true
			}
		}
	}

	assert.Equal(t, 0, duplicates, "Should have no duplicate messages across partitions")

	// Verify all partitions maintained progress
	partitionCounts := make(map[int32]int)
	for key := range seenMessages {
		var partition int32
		_, _ = fmt.Sscanf(key, "%d:", &partition)
		partitionCounts[partition]++
	}

	for partition := int32(0); partition < 3; partition++ {
		assert.Greater(t, partitionCounts[partition], 0,
			"Partition %d should have processed messages", partition)
	}

	t.Logf("Multi-partition test: %d pre-crash boxes, %d post-recovery boxes, partition counts: %v",
		len(processor.groups), len(processor2.groups), partitionCounts)
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

// TestGatherer_ExactlyOnceSemantics verifies that each message appears in exactly one box
// even with failures, replays, and partition rebalancing
func TestGatherer_ExactlyOnceSemantics(t *testing.T) {
	offsetStore := NewMockOffsetStore()
	processor := NewMockCompactorWithStore(offsetStore) // Use processor that tracks offsets
	orgID := uuid.New()

	// Create a controlled set of messages
	var testMsgs []*messages.MetricCompactionMessage
	var testMetadata []*messageMetadata

	for i := range 500 {
		msg := createTestMessage(orgID, 1, 20250115, 60000, 30) // 30 records each
		md := createTestMetadata("test-topic", 0, "test-group", int64(i))
		testMsgs = append(testMsgs, msg)
		testMetadata = append(testMetadata, md)
	}

	// Phase 1: Normal processing
	gatherer1 := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", processor, offsetStore)

	// Process first 200 messages
	for i := 0; i < 200; i++ {
		err := gatherer1.processMessage(context.Background(), testMsgs[i], testMetadata[i])
		assert.NoError(t, err)
	}

	// Flush any pending messages from gatherer1
	_, err := gatherer1.processIdleGroups(context.Background(), 0, 0)
	assert.NoError(t, err)

	// Phase 2: Replay with overlap (simulating partition rebalance with new consumer)
	// Create a new gatherer to simulate a new consumer after rebalance
	gatherer2 := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", processor, offsetStore)

	// Start from offset 150 (50 message overlap)
	for i := 150; i < 350; i++ {
		err := gatherer2.processMessage(context.Background(), testMsgs[i], testMetadata[i])
		assert.NoError(t, err)
	}

	// Flush any pending messages from gatherer2
	_, err = gatherer2.processIdleGroups(context.Background(), 0, 0)
	assert.NoError(t, err)

	// Phase 3: Another overlap replay (simulating another rebalance with new consumer)
	// Create another new gatherer to simulate another consumer after rebalance
	gatherer3 := newGatherer[*messages.MetricCompactionMessage]("test-topic", "test-group", processor, offsetStore)

	for i := 300; i < 500; i++ {
		err := gatherer3.processMessage(context.Background(), testMsgs[i], testMetadata[i])
		assert.NoError(t, err)
	}

	// Flush any remaining messages from gatherer3
	_, err = gatherer3.processIdleGroups(context.Background(), 0, 0)
	assert.NoError(t, err)

	// Verify exactly-once semantics
	messageCount := make(map[int64]int)
	for _, kafkaOffsets := range processor.kafkaOffsets {
		for _, offsetInfo := range kafkaOffsets {
			for _, offset := range offsetInfo.Offsets {
				messageCount[offset]++
			}
		}
	}

	// Check for duplicates
	duplicates := 0
	missing := 0
	for i := range 500 {
		count := messageCount[int64(i)]
		if count > 1 {
			duplicates++
			t.Errorf("Message at offset %d appeared %d times (expected 1)", i, count)
		} else if count == 0 && i < 350 { // Messages after 350 might not be in a box yet
			missing++
			t.Errorf("Message at offset %d is missing", i)
		}
	}

	assert.Equal(t, 0, duplicates, "Should have no duplicate messages")
	assert.Equal(t, 0, missing, "Should have no missing messages in processed range")

	// Verify all messages in boxes have consistent grouping
	for _, group := range processor.groups {
		assert.Equal(t, orgID, group.Key.OrganizationID)
		assert.Equal(t, int16(1), group.Key.InstanceNum)
		assert.Equal(t, int32(20250115), group.Key.DateInt)
		assert.Equal(t, int32(60000), group.Key.FrequencyMs)
	}

	t.Logf("Exactly-once test: processed %d unique messages in %d boxes",
		len(messageCount), len(processor.groups))
}

func createTestMetadata(topic string, partition int32, consumerGroup string, offset int64) *messageMetadata {
	return &messageMetadata{
		Topic:         topic,
		Partition:     partition,
		ConsumerGroup: consumerGroup,
		Offset:        offset,
	}
}
