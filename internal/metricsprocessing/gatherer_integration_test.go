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

//go:build integration

package metricsprocessing

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// Use lrdb.Store directly since it implements most of LogCompactionStore interface

// integrationTestProcessor simulates a processor that performs atomic DB updates
type integrationTestProcessor struct {
	store           LogCompactionStore
	processedGroups []*accumulationGroup[messages.LogCompactionKey]
	cfg             *config.Config
}

func newIntegrationTestProcessor(store LogCompactionStore, cfg *config.Config) *integrationTestProcessor {
	return &integrationTestProcessor{
		store: store,
		cfg:   cfg,
	}
}

func (p *integrationTestProcessor) Process(ctx context.Context, group *accumulationGroup[messages.LogCompactionKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	p.processedGroups = append(p.processedGroups, group)

	// Create fake input segments for the messages in the group
	var inputSegments []lrdb.LogSeg
	for _, msg := range group.Messages {
		logMsg := msg.Message.(*messages.LogCompactionMessage)
		seg := lrdb.LogSeg{
			OrganizationID: logMsg.OrganizationID,
			SegmentID:      logMsg.SegmentID,
			Dateint:        logMsg.DateInt,
			InstanceNum:    logMsg.InstanceNum,
			RecordCount:    logMsg.Records,
			FileSize:       logMsg.FileSize,
			TsRange: pgtype.Range[pgtype.Int8]{
				Lower:     pgtype.Int8{Int64: time.Now().UnixNano(), Valid: true},
				Upper:     pgtype.Int8{Int64: time.Now().UnixNano() + 1000000, Valid: true},
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Valid:     true,
			},
		}
		inputSegments = append(inputSegments, seg)
	}

	// Create a fake output segment (simulating compaction result)
	outputSegment := lrdb.LogSeg{
		OrganizationID: group.Key.OrganizationID,
		SegmentID:      int64(time.Now().UnixNano()), // Generate unique ID
		Dateint:        group.Key.DateInt,
		InstanceNum:    group.Key.InstanceNum,
		RecordCount:    group.TotalRecordCount,
		FileSize:       group.TotalRecordCount * 100, // Fake file size
		TsRange: pgtype.Range[pgtype.Int8]{
			Lower:     pgtype.Int8{Int64: time.Now().UnixNano(), Valid: true},
			Upper:     pgtype.Int8{Int64: time.Now().UnixNano() + 10000000, Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		},
		Fingerprints: []int64{}, // Empty fingerprints for test
	}

	// Perform atomic database update (marks old segments as compacted, inserts new, updates Kafka offsets)
	params := lrdb.CompactLogSegsParams{
		OrganizationID: group.Key.OrganizationID,
		Dateint:        group.Key.DateInt,
		IngestDateint:  group.Key.DateInt, // Use same as Dateint for test
		InstanceNum:    group.Key.InstanceNum,
		OldRecords:     make([]lrdb.CompactLogSegsOld, len(inputSegments)),
		NewRecords: []lrdb.CompactLogSegsNew{{
			SegmentID:    outputSegment.SegmentID,
			StartTs:      outputSegment.TsRange.Lower.Int64,
			EndTs:        outputSegment.TsRange.Upper.Int64,
			RecordCount:  outputSegment.RecordCount,
			FileSize:     outputSegment.FileSize,
			Fingerprints: outputSegment.Fingerprints,
		}},
	}

	for i, seg := range inputSegments {
		params.OldRecords[i] = lrdb.CompactLogSegsOld{
			SegmentID: seg.SegmentID,
		}
	}

	// This is the atomic operation that updates both segments and Kafka offsets
	return p.store.CompactLogSegments(ctx, params, kafkaOffsets)
}

func (p *integrationTestProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.LogCompactionKey) int64 {
	return 1000 // Lower threshold for test to trigger box emission
}

// setupIntegrationTest creates a test database and returns cleanup function
func setupIntegrationTest(t *testing.T) (*lrdb.Store, func()) {
	ctx := context.Background()

	// Create test database connection using pgxpool
	config, err := pgxpool.ParseConfig("host=localhost dbname=testing_lrdb sslmode=disable")
	require.NoError(t, err)

	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)

	// Create LRDB store which implements LogCompactionStore interface
	store := lrdb.NewStore(pool)

	// Clean up any existing test data
	_, _ = pool.Exec(ctx, "DELETE FROM kafka_offset_tracker WHERE consumer_group LIKE 'test-%'")

	cleanup := func() {
		_, _ = pool.Exec(ctx, "DELETE FROM kafka_offset_tracker WHERE consumer_group LIKE 'test-%'")
		// Drop any test partitions created during tests
		rows, _ := pool.Query(ctx, "SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename LIKE 'log_seg_%' AND tablename NOT LIKE '%_pkey' AND tablename != 'log_seg'")
		var partitions []string
		for rows.Next() {
			var partition string
			if rows.Scan(&partition) == nil {
				partitions = append(partitions, partition)
			}
		}
		rows.Close()
		for _, partition := range partitions {
			_, _ = pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", partition))
		}
		pool.Close()
	}

	return store, cleanup
}

func TestGatherer_DatabaseIntegration_ExactlyOnceSemantics(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	orgID := uuid.New()
	cfg := getTestConfig()
	processor := newIntegrationTestProcessor(store, cfg)

	// Create test messages and insert corresponding segments
	var testMsgs []*messages.LogCompactionMessage
	var testMetadata []*messageMetadata
	segmentIDs := make([]int64, 0, 100)

	for i := 0; i < 100; i++ {
		segmentID := int64(1000000 + i) // Use predictable IDs for testing
		msg := &messages.LogCompactionMessage{
			OrganizationID: orgID,
			DateInt:        20250115,
			InstanceNum:    1,
			SegmentID:      segmentID,
			Records:        100, // Increased to trigger box emission faster
			FileSize:       10000,
			QueuedAt:       time.Now(),
		}
		md := createTestMetadata("test-topic", 0, "test-group", int64(i))
		testMsgs = append(testMsgs, msg)
		testMetadata = append(testMetadata, md)
		segmentIDs = append(segmentIDs, segmentID)
	}

	// Insert segments into database so they can be marked as compacted
	err := insertTestSegments(ctx, store, orgID, 20250115, 1, segmentIDs, false, false)
	require.NoError(t, err, "Failed to insert test segments")

	// Phase 1: Process first batch with gatherer1
	gatherer1 := newGatherer[*messages.LogCompactionMessage, messages.LogCompactionKey](
		"test-topic", "test-group", processor, store)

	for i := 0; i < 50; i++ {
		err := gatherer1.processMessage(ctx, testMsgs[i], testMetadata[i])
		require.NoError(t, err)
	}

	// Flush pending messages
	_, err = gatherer1.processIdleGroups(ctx, 0, 0)
	require.NoError(t, err)

	groupsAfterPhase1 := len(processor.processedGroups)

	// Phase 2: Simulate restart with overlap (messages 30-80)
	processor2 := newIntegrationTestProcessor(store, cfg)
	gatherer2 := newGatherer[*messages.LogCompactionMessage, messages.LogCompactionKey](
		"test-topic", "test-group", processor2, store)

	for i := 30; i < 80; i++ {
		err := gatherer2.processMessage(ctx, testMsgs[i], testMetadata[i])
		require.NoError(t, err)
	}

	// Flush pending messages
	_, err = gatherer2.processIdleGroups(ctx, 0, 0)
	require.NoError(t, err)

	// Phase 3: Process remaining messages with another overlap (60-100)
	processor3 := newIntegrationTestProcessor(store, cfg)
	gatherer3 := newGatherer[*messages.LogCompactionMessage, messages.LogCompactionKey](
		"test-topic", "test-group", processor3, store)

	for i := 60; i < 100; i++ {
		err := gatherer3.processMessage(ctx, testMsgs[i], testMetadata[i])
		require.NoError(t, err)
	}

	// Flush pending messages
	_, err = gatherer3.processIdleGroups(ctx, 0, 0)
	require.NoError(t, err)

	// Verify no duplicates in the database
	// Count all Kafka offsets that were tracked
	offsets, err := store.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: "test-group",
		Topic:         "test-topic",
		PartitionID:   0,
		MinOffset:     0,
	})
	require.NoError(t, err)

	// Check for duplicates
	offsetMap := make(map[int64]int)
	for _, offset := range offsets {
		offsetMap[offset]++
	}

	duplicates := 0
	for offset, count := range offsetMap {
		if count > 1 {
			duplicates++
			t.Errorf("Offset %d appears %d times (expected 1)", offset, count)
		}
	}

	assert.Equal(t, 0, duplicates, "Should have no duplicate offsets in database")

	// Verify segments were marked as compacted
	query := `SELECT COUNT(*) FROM log_seg WHERE organization_id = $1 AND dateint = $2 AND compacted = true`
	var compactedCount int
	err = store.Pool().QueryRow(ctx, query, orgID, int32(20250115)).Scan(&compactedCount)
	require.NoError(t, err)

	query = `SELECT COUNT(*) FROM log_seg WHERE organization_id = $1 AND dateint = $2 AND compacted = false`
	var activeCount int
	err = store.Pool().QueryRow(ctx, query, orgID, int32(20250115)).Scan(&activeCount)
	require.NoError(t, err)

	t.Logf("Database integration test complete: %d groups phase 1, %d total groups processed, %d compacted segments, %d active segments",
		groupsAfterPhase1, len(processor.processedGroups)+len(processor2.processedGroups)+len(processor3.processedGroups),
		compactedCount, activeCount)
}

func TestGatherer_DatabaseIntegration_FailureAndRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	orgID := uuid.New()
	cfg := getTestConfig()
	processor := newIntegrationTestProcessor(store, cfg)

	// Create messages and insert corresponding segments
	var testMsgs []*messages.LogCompactionMessage
	var testMetadata []*messageMetadata
	segmentIDs := make([]int64, 0, 150)

	for i := 0; i < 150; i++ {
		segmentID := int64(2000000 + i) // Use different range from other test
		msg := &messages.LogCompactionMessage{
			OrganizationID: orgID,
			DateInt:        20250115,
			InstanceNum:    1,
			SegmentID:      segmentID,
			Records:        50, // Increased for consistent box emission
			FileSize:       5000,
			QueuedAt:       time.Now(),
		}
		md := createTestMetadata("test-topic", 0, "test-group", int64(i))
		testMsgs = append(testMsgs, msg)
		testMetadata = append(testMetadata, md)
		segmentIDs = append(segmentIDs, segmentID)
	}

	// Insert segments into database so they can be marked as compacted
	err := insertTestSegments(ctx, store, orgID, 20250115, 1, segmentIDs, false, false)
	require.NoError(t, err, "Failed to insert test segments")

	// Phase 1: Process messages until "crash"
	gatherer1 := newGatherer[*messages.LogCompactionMessage, messages.LogCompactionKey](
		"test-topic", "test-group", processor, store)

	crashAt := -1
	for i := 0; i < len(testMsgs); i++ {
		err := gatherer1.processMessage(ctx, testMsgs[i], testMetadata[i])
		require.NoError(t, err)

		// Check for boxes emitted (need to flush to force emission)
		if i > 0 && i%50 == 0 { // Periodically check for boxes
			_, _ = gatherer1.processIdleGroups(ctx, 0, 0)
		}

		// Simulate crash after 2 boxes are emitted
		if len(processor.processedGroups) >= 2 {
			crashAt = i
			break
		}
	}

	require.Greater(t, crashAt, 0, "Should have crashed after processing some messages")
	boxesBeforeCrash := len(processor.processedGroups)

	// Get last committed offset from database
	offsets, err := store.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: "test-group",
		Topic:         "test-topic",
		PartitionID:   0,
		MinOffset:     0,
	})
	require.NoError(t, err)

	lastCommittedOffset := int64(-1)
	for _, offset := range offsets {
		if offset > lastCommittedOffset {
			lastCommittedOffset = offset
		}
	}

	// Phase 2: Recovery - replay from last committed offset
	processor2 := newIntegrationTestProcessor(store, cfg)
	gatherer2 := newGatherer[*messages.LogCompactionMessage, messages.LogCompactionKey](
		"test-topic", "test-group", processor2, store)

	replayStart := int(lastCommittedOffset + 1)
	if replayStart < 0 {
		replayStart = 0
	}

	for i := replayStart; i < len(testMsgs); i++ {
		err := gatherer2.processMessage(ctx, testMsgs[i], testMetadata[i])
		require.NoError(t, err)
	}

	// Flush remaining
	_, err = gatherer2.processIdleGroups(ctx, 0, 0)
	require.NoError(t, err)

	// Verify all messages were processed exactly once
	allOffsets, err := store.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
		ConsumerGroup: "test-group",
		Topic:         "test-topic",
		PartitionID:   0,
		MinOffset:     0,
	})
	require.NoError(t, err)

	// Check each message appears exactly once
	offsetCount := make(map[int64]int)
	for _, offset := range allOffsets {
		offsetCount[offset]++
	}

	for i := 0; i < len(testMsgs); i++ {
		count := offsetCount[int64(i)]
		assert.Equal(t, 1, count, "Message at offset %d should appear exactly once, got %d", i, count)
	}

	t.Logf("Failure/recovery test: %d boxes before crash, %d boxes after recovery, %d total messages",
		boxesBeforeCrash, len(processor2.processedGroups), len(testMsgs))
}

func TestGatherer_DatabaseIntegration_MultiPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	ctx := context.Background()
	store, cleanup := setupIntegrationTest(t)
	defer cleanup()

	orgID := uuid.New()
	cfg := getTestConfig()
	processor := newIntegrationTestProcessor(store, cfg)

	// Create messages across 3 partitions
	type msgInfo struct {
		msg      *messages.LogCompactionMessage
		metadata *messageMetadata
	}

	var allMessages []msgInfo
	var allSegmentIDs []int64

	for partition := int32(0); partition < 3; partition++ {
		for i := 0; i < 50; i++ {
			segmentID := int64(3000000 + int(partition)*1000 + i) // Unique range per partition
			msg := &messages.LogCompactionMessage{
				OrganizationID: orgID,
				DateInt:        20250115,
				InstanceNum:    1,
				SegmentID:      segmentID,
				Records:        50, // Increased for consistent box emission
				FileSize:       5000,
				QueuedAt:       time.Now(),
			}
			md := createTestMetadata("test-topic", partition, "test-group", int64(i))

			allMessages = append(allMessages, msgInfo{msg: msg, metadata: md})
			allSegmentIDs = append(allSegmentIDs, segmentID)
		}
	}

	// Insert all segments into database
	err := insertTestSegments(ctx, store, orgID, 20250115, 1, allSegmentIDs, false, false)
	require.NoError(t, err, "Failed to insert test segments")

	// Process messages in interleaved fashion
	gatherer := newGatherer[*messages.LogCompactionMessage, messages.LogCompactionKey](
		"test-topic", "test-group", processor, store)

	// Process in rounds to simulate real Kafka consumption
	for round := 0; round < 50; round++ {
		for partition := int32(0); partition < 3; partition++ {
			idx := int(partition)*50 + round
			if idx < len(allMessages) {
				err := gatherer.processMessage(ctx, allMessages[idx].msg, allMessages[idx].metadata)
				require.NoError(t, err)
			}
		}
	}

	// Flush remaining
	_, err = gatherer.processIdleGroups(ctx, 0, 0)
	require.NoError(t, err)

	// Verify each partition's offsets are tracked correctly
	for partition := int32(0); partition < 3; partition++ {
		offsets, err := store.KafkaOffsetsAfter(ctx, lrdb.KafkaOffsetsAfterParams{
			ConsumerGroup: "test-group",
			Topic:         "test-topic",
			PartitionID:   partition,
			MinOffset:     0,
		})
		require.NoError(t, err)

		// Each partition should have its messages tracked
		assert.Greater(t, len(offsets), 0, "Partition %d should have offsets tracked", partition)

		// Check no duplicates
		offsetSet := make(map[int64]bool)
		for _, offset := range offsets {
			assert.False(t, offsetSet[offset], "Duplicate offset %d in partition %d", offset, partition)
			offsetSet[offset] = true
		}
	}

	t.Logf("Multi-partition test complete: %d total messages processed across 3 partitions",
		len(allMessages))
}

// Helper to create log compaction message
func createTestLogMessage(orgID uuid.UUID, instanceNum int16, dateInt int32, recordCount int64) *messages.LogCompactionMessage {
	return &messages.LogCompactionMessage{
		OrganizationID: orgID,
		DateInt:        dateInt,
		SegmentID:      int64(time.Now().UnixNano()), // Generate unique ID
		InstanceNum:    instanceNum,
		Records:        recordCount,
		FileSize:       recordCount * 100,
		QueuedAt:       time.Now(),
	}
}

// Helper to insert test segments into database
func insertTestSegments(ctx context.Context, store *lrdb.Store, orgID uuid.UUID, dateInt int32, instanceNum int16, segmentIDs []int64, compacted bool, published bool) error {
	// Use InsertLogSegment which handles partition creation automatically
	for _, segmentID := range segmentIDs {
		startTs := time.Now().UnixNano() / 1e6 // Convert to milliseconds
		endTs := startTs + 1000                // 1 second range

		params := lrdb.InsertLogSegmentParams{
			OrganizationID: orgID,
			Dateint:        dateInt,
			SegmentID:      segmentID,
			InstanceNum:    instanceNum,
			StartTs:        startTs,
			EndTs:          endTs,
			RecordCount:    100,
			FileSize:       10000,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   []int64{}, // Empty fingerprints for test
			Published:      published,
			Compacted:      compacted,
		}

		err := store.InsertLogSegment(ctx, params)
		if err != nil {
			return fmt.Errorf("failed to insert segment %d: %w", segmentID, err)
		}
	}
	return nil
}
