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
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// mockBoxerStore implements BoxerStore for testing
type mockBoxerStore struct {
	addedBundles    []addedBundle
	insertedOffsets []lrdb.KafkaOffsetInfo
	metricEstimate  int64
	logEstimate     int64
	traceEstimate   int64
}

type addedBundle struct {
	taskName       string
	organizationID uuid.UUID
	instanceNum    int16
	spec           json.RawMessage
}

func (m *mockBoxerStore) WorkQueueAdd(ctx context.Context, arg lrdb.WorkQueueAddParams) (lrdb.WorkQueue, error) {
	m.addedBundles = append(m.addedBundles, addedBundle{
		taskName:       arg.TaskName,
		organizationID: arg.OrganizationID,
		instanceNum:    arg.InstanceNum,
		spec:           arg.Spec,
	})
	return lrdb.WorkQueue{ID: int64(len(m.addedBundles))}, nil
}

func (m *mockBoxerStore) WorkQueueDepth(ctx context.Context, taskName string) (int64, error) {
	return 0, nil
}

func (m *mockBoxerStore) WorkQueueCleanup(ctx context.Context, heartbeatTimeout time.Duration) error {
	return nil
}

func (m *mockBoxerStore) InsertKafkaOffsets(ctx context.Context, params lrdb.InsertKafkaOffsetsParams) error {
	m.insertedOffsets = append(m.insertedOffsets, lrdb.KafkaOffsetInfo{
		ConsumerGroup: params.ConsumerGroup,
		Topic:         params.Topic,
		PartitionID:   params.PartitionID,
		Offsets:       params.Offsets,
	})
	return nil
}

func (m *mockBoxerStore) GetMetricEstimate(ctx context.Context, organizationID uuid.UUID, frequencyMs int32) int64 {
	return m.metricEstimate
}

func (m *mockBoxerStore) GetLogEstimate(ctx context.Context, orgID uuid.UUID) int64 {
	return m.logEstimate
}

func (m *mockBoxerStore) GetTraceEstimate(ctx context.Context, orgID uuid.UUID) int64 {
	return m.traceEstimate
}

func (m *mockBoxerStore) KafkaOffsetsAfter(ctx context.Context, params lrdb.KafkaOffsetsAfterParams) ([]int64, error) {
	return nil, nil
}

func (m *mockBoxerStore) CleanupKafkaOffsets(ctx context.Context, params lrdb.CleanupKafkaOffsetsParams) (int64, error) {
	return 0, nil
}

func TestLogIngestBoxerProcessor_Process(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	store := &mockBoxerStore{}

	processor := newLogIngestBoxerProcessor(cfg, store)

	orgID := uuid.New()
	instanceNum := int16(1)
	key := messages.IngestKey{
		OrganizationID: orgID,
		InstanceNum:    instanceNum,
	}

	msgs := []*messages.ObjStoreNotificationMessage{
		{
			OrganizationID: orgID,
			InstanceNum:    instanceNum,
			Bucket:         "test-bucket",
			ObjectID:       "test-object-1",
			FileSize:       1024,
		},
		{
			OrganizationID: orgID,
			InstanceNum:    instanceNum,
			Bucket:         "test-bucket",
			ObjectID:       "test-object-2",
			FileSize:       2048,
		},
	}

	group := &accumulationGroup[messages.IngestKey]{
		Key:      key,
		Messages: make([]*accumulatedMessage, 0, len(msgs)),
	}
	for _, msg := range msgs {
		group.Messages = append(group.Messages, &accumulatedMessage{Message: msg})
	}

	kafkaOffsets := []lrdb.KafkaOffsetInfo{
		{
			ConsumerGroup: "test-group",
			Topic:         "test-topic",
			PartitionID:   0,
			Offsets:       []int64{100, 101},
		},
	}

	err := processor.Process(ctx, group, kafkaOffsets)
	require.NoError(t, err)

	// Verify bundle was added
	assert.Len(t, store.addedBundles, 1)
	assert.Equal(t, config.BoxerTaskIngestLogs, store.addedBundles[0].taskName)
	assert.Equal(t, orgID, store.addedBundles[0].organizationID)
	assert.Equal(t, instanceNum, store.addedBundles[0].instanceNum)

	// Verify Kafka offsets were inserted
	assert.Len(t, store.insertedOffsets, 1)
	assert.Equal(t, "test-group", store.insertedOffsets[0].ConsumerGroup)
	assert.Equal(t, "test-topic", store.insertedOffsets[0].Topic)
	assert.Equal(t, int32(0), store.insertedOffsets[0].PartitionID)
	assert.Equal(t, []int64{100, 101}, store.insertedOffsets[0].Offsets)
}

func TestMetricIngestBoxerProcessor_Process(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	store := &mockBoxerStore{}

	processor := newMetricIngestBoxerProcessor(cfg, store)

	orgID := uuid.New()
	instanceNum := int16(2)
	key := messages.IngestKey{
		OrganizationID: orgID,
		InstanceNum:    instanceNum,
	}

	msgs := []*messages.ObjStoreNotificationMessage{
		{
			OrganizationID: orgID,
			InstanceNum:    instanceNum,
			Bucket:         "metrics-bucket",
			ObjectID:       "metrics-1",
			FileSize:       4096,
		},
	}

	group := &accumulationGroup[messages.IngestKey]{
		Key:      key,
		Messages: []*accumulatedMessage{{Message: msgs[0]}},
	}

	kafkaOffsets := []lrdb.KafkaOffsetInfo{
		{
			ConsumerGroup: "metrics-group",
			Topic:         "metrics-topic",
			PartitionID:   1,
			Offsets:       []int64{200},
		},
	}

	err := processor.Process(ctx, group, kafkaOffsets)
	require.NoError(t, err)

	assert.Len(t, store.addedBundles, 1)
	assert.Equal(t, config.BoxerTaskIngestMetrics, store.addedBundles[0].taskName)
	assert.Equal(t, orgID, store.addedBundles[0].organizationID)
	assert.Equal(t, instanceNum, store.addedBundles[0].instanceNum)

	assert.Len(t, store.insertedOffsets, 1)
}

func TestTraceIngestBoxerProcessor_Process(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	store := &mockBoxerStore{}

	processor := newTraceIngestBoxerProcessor(cfg, store)

	orgID := uuid.New()
	instanceNum := int16(3)
	key := messages.IngestKey{
		OrganizationID: orgID,
		InstanceNum:    instanceNum,
	}

	msgs := []*messages.ObjStoreNotificationMessage{
		{
			OrganizationID: orgID,
			InstanceNum:    instanceNum,
			Bucket:         "traces-bucket",
			ObjectID:       "trace-1",
			FileSize:       8192,
		},
	}

	group := &accumulationGroup[messages.IngestKey]{
		Key:      key,
		Messages: []*accumulatedMessage{{Message: msgs[0]}},
	}

	kafkaOffsets := []lrdb.KafkaOffsetInfo{}

	err := processor.Process(ctx, group, kafkaOffsets)
	require.NoError(t, err)

	assert.Len(t, store.addedBundles, 1)
	assert.Equal(t, config.BoxerTaskIngestTraces, store.addedBundles[0].taskName)

	// No offsets should be inserted when kafkaOffsets is empty
	assert.Len(t, store.insertedOffsets, 0)
}

func TestLogCompactionBoxerProcessor_Process(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	store := &mockBoxerStore{logEstimate: 5000}

	processor := newLogCompactionBoxerProcessor(cfg, store)

	orgID := uuid.New()
	instanceNum := int16(1)
	dateint := int32(20250101)

	key := messages.LogCompactionKey{
		OrganizationID: orgID,
		DateInt:        dateint,
		InstanceNum:    instanceNum,
	}

	msg := &messages.LogCompactionMessage{
		OrganizationID: orgID,
		DateInt:        dateint,
		InstanceNum:    instanceNum,
		SegmentID:      12345,
	}

	group := &accumulationGroup[messages.LogCompactionKey]{
		Key:      key,
		Messages: []*accumulatedMessage{{Message: msg}},
	}

	kafkaOffsets := []lrdb.KafkaOffsetInfo{}

	err := processor.Process(ctx, group, kafkaOffsets)
	require.NoError(t, err)

	assert.Len(t, store.addedBundles, 1)
	assert.Equal(t, config.BoxerTaskCompactLogs, store.addedBundles[0].taskName)
	assert.Equal(t, orgID, store.addedBundles[0].organizationID)
	assert.Equal(t, instanceNum, store.addedBundles[0].instanceNum)
}

func TestMetricCompactionBoxerProcessor_Process(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	store := &mockBoxerStore{metricEstimate: 10000}

	processor := newMetricCompactionBoxerProcessor(cfg, store)

	orgID := uuid.New()
	instanceNum := int16(2)
	dateint := int32(20250102)

	key := messages.CompactionKey{
		OrganizationID: orgID,
		DateInt:        dateint,
		FrequencyMs:    10000,
		InstanceNum:    instanceNum,
	}

	msg := &messages.MetricCompactionMessage{
		OrganizationID: orgID,
		DateInt:        dateint,
		FrequencyMs:    10000,
		InstanceNum:    instanceNum,
		SegmentID:      54321,
	}

	group := &accumulationGroup[messages.CompactionKey]{
		Key:      key,
		Messages: []*accumulatedMessage{{Message: msg}},
	}

	kafkaOffsets := []lrdb.KafkaOffsetInfo{}

	err := processor.Process(ctx, group, kafkaOffsets)
	require.NoError(t, err)

	assert.Len(t, store.addedBundles, 1)
	assert.Equal(t, config.BoxerTaskCompactMetrics, store.addedBundles[0].taskName)
}

func TestTraceCompactionBoxerProcessor_Process(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	store := &mockBoxerStore{}

	processor := newTraceCompactionBoxerProcessor(cfg, store)

	orgID := uuid.New()
	instanceNum := int16(3)
	dateint := int32(20250103)

	key := messages.TraceCompactionKey{
		OrganizationID: orgID,
		DateInt:        dateint,
		InstanceNum:    instanceNum,
	}

	msg := &messages.TraceCompactionMessage{
		OrganizationID: orgID,
		DateInt:        dateint,
		InstanceNum:    instanceNum,
		SegmentID:      99999,
	}

	group := &accumulationGroup[messages.TraceCompactionKey]{
		Key:      key,
		Messages: []*accumulatedMessage{{Message: msg}},
	}

	kafkaOffsets := []lrdb.KafkaOffsetInfo{}

	err := processor.Process(ctx, group, kafkaOffsets)
	require.NoError(t, err)

	assert.Len(t, store.addedBundles, 1)
	assert.Equal(t, config.BoxerTaskCompactTraces, store.addedBundles[0].taskName)
}

func TestMetricRollupBoxerProcessor_Process(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	store := &mockBoxerStore{metricEstimate: 15000}

	processor := newMetricBoxerProcessor(cfg, store)

	orgID := uuid.New()
	instanceNum := int16(1)
	dateint := int32(20250104)
	sourceFreqMs := int32(10000)
	targetFreqMs := int32(60000)
	truncatedTimebox := time.Now().Unix()

	key := messages.RollupKey{
		OrganizationID:    orgID,
		DateInt:           dateint,
		SourceFrequencyMs: sourceFreqMs,
		TargetFrequencyMs: targetFreqMs,
		InstanceNum:       instanceNum,
		TruncatedTimebox:  truncatedTimebox,
	}

	msg := &messages.MetricRollupMessage{
		OrganizationID:    orgID,
		DateInt:           dateint,
		SourceFrequencyMs: sourceFreqMs,
		TargetFrequencyMs: targetFreqMs,
		InstanceNum:       instanceNum,
		SegmentID:         77777,
	}

	group := &accumulationGroup[messages.RollupKey]{
		Key:      key,
		Messages: []*accumulatedMessage{{Message: msg}},
	}

	kafkaOffsets := []lrdb.KafkaOffsetInfo{}

	err := processor.Process(ctx, group, kafkaOffsets)
	require.NoError(t, err)

	assert.Len(t, store.addedBundles, 1)
	assert.Equal(t, config.BoxerTaskRollupMetrics, store.addedBundles[0].taskName)
	assert.Equal(t, orgID, store.addedBundles[0].organizationID)
	assert.Equal(t, instanceNum, store.addedBundles[0].instanceNum)
}

func TestBoxerProcessor_EmptyOffsets(t *testing.T) {
	// Test that empty offset arrays are skipped
	ctx := context.Background()
	cfg := &config.Config{}
	store := &mockBoxerStore{}

	processor := newLogIngestBoxerProcessor(cfg, store)

	orgID := uuid.New()
	key := messages.IngestKey{
		OrganizationID: orgID,
		InstanceNum:    1,
	}

	msg := &messages.ObjStoreNotificationMessage{
		OrganizationID: orgID,
		InstanceNum:    1,
		Bucket:         "test-bucket",
		ObjectID:       "test-object",
		FileSize:       1024,
	}

	group := &accumulationGroup[messages.IngestKey]{
		Key:      key,
		Messages: []*accumulatedMessage{{Message: msg}},
	}

	// Include both empty and non-empty offset arrays
	kafkaOffsets := []lrdb.KafkaOffsetInfo{
		{
			ConsumerGroup: "empty-group",
			Topic:         "empty-topic",
			PartitionID:   0,
			Offsets:       []int64{}, // Empty - should be skipped
		},
		{
			ConsumerGroup: "valid-group",
			Topic:         "valid-topic",
			PartitionID:   1,
			Offsets:       []int64{100},
		},
	}

	err := processor.Process(ctx, group, kafkaOffsets)
	require.NoError(t, err)

	// Only the non-empty offset should be inserted
	assert.Len(t, store.insertedOffsets, 1)
	assert.Equal(t, "valid-group", store.insertedOffsets[0].ConsumerGroup)
}

func TestBoxerProcessor_GetTargetRecordCount(t *testing.T) {
	ctx := context.Background()
	cfg := &config.Config{}
	orgID := uuid.New()

	t.Run("LogIngestBoxerProcessor", func(t *testing.T) {
		store := &mockBoxerStore{}
		processor := newLogIngestBoxerProcessor(cfg, store)
		key := messages.IngestKey{OrganizationID: orgID}
		count := processor.GetTargetRecordCount(ctx, key)
		assert.Equal(t, config.TargetFileSize, count)
	})

	t.Run("MetricIngestBoxerProcessor", func(t *testing.T) {
		store := &mockBoxerStore{}
		processor := newMetricIngestBoxerProcessor(cfg, store)
		key := messages.IngestKey{OrganizationID: orgID}
		count := processor.GetTargetRecordCount(ctx, key)
		assert.Equal(t, config.TargetFileSize, count)
	})

	t.Run("TraceIngestBoxerProcessor", func(t *testing.T) {
		store := &mockBoxerStore{}
		processor := newTraceIngestBoxerProcessor(cfg, store)
		key := messages.IngestKey{OrganizationID: orgID}
		count := processor.GetTargetRecordCount(ctx, key)
		assert.Equal(t, config.TargetFileSize, count)
	})

	t.Run("MetricRollupBoxerProcessor", func(t *testing.T) {
		expectedEstimate := int64(12345)
		store := &mockBoxerStore{metricEstimate: expectedEstimate}
		processor := newMetricBoxerProcessor(cfg, store)
		key := messages.RollupKey{
			OrganizationID:    orgID,
			TargetFrequencyMs: 60000,
		}
		count := processor.GetTargetRecordCount(ctx, key)
		assert.Equal(t, expectedEstimate, count)
	})

	t.Run("LogCompactionBoxerProcessor", func(t *testing.T) {
		expectedEstimate := int64(54321)
		store := &mockBoxerStore{logEstimate: expectedEstimate}
		processor := newLogCompactionBoxerProcessor(cfg, store)
		key := messages.LogCompactionKey{OrganizationID: orgID}
		count := processor.GetTargetRecordCount(ctx, key)
		assert.Equal(t, expectedEstimate, count)
	})
}
