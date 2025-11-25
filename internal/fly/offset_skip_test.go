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

package fly

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// mockStore implements the subset of lrdb.Store methods needed for testing
type mockStore struct {
	skips        []lrdb.GetKafkaOffsetSkipsRow
	deletedSkips []lrdb.DeleteKafkaOffsetSkipParams
	getError     error
	deleteError  error
	getCalled    int
	deleteCalled int
}

func (m *mockStore) GetKafkaOffsetSkips(ctx context.Context, params lrdb.GetKafkaOffsetSkipsParams) ([]lrdb.GetKafkaOffsetSkipsRow, error) {
	m.getCalled++
	if m.getError != nil {
		return nil, m.getError
	}
	return m.skips, nil
}

func (m *mockStore) DeleteKafkaOffsetSkip(ctx context.Context, params lrdb.DeleteKafkaOffsetSkipParams) error {
	m.deleteCalled++
	if m.deleteError != nil {
		return m.deleteError
	}
	m.deletedSkips = append(m.deletedSkips, params)
	return nil
}

// mockAdminClient implements the subset of AdminClient methods needed for testing
type mockAdminClient struct {
	committedOffsets []struct {
		groupID string
		topic   string
		offsets []PartitionOffset
	}
	commitError  error
	commitCalled int
}

func (m *mockAdminClient) CommitConsumerGroupOffsets(ctx context.Context, groupID, topic string, offsets []PartitionOffset) error {
	m.commitCalled++
	if m.commitError != nil {
		return m.commitError
	}
	m.committedOffsets = append(m.committedOffsets, struct {
		groupID string
		topic   string
		offsets []PartitionOffset
	}{groupID, topic, offsets})
	return nil
}

// testableOffsetSkipChecker wraps OffsetSkipChecker with mock components
type testableOffsetSkipChecker struct {
	store       *mockStore
	adminClient *mockAdminClient
	checker     *OffsetSkipChecker
}

func newTestableChecker(consumerGroup, topic string) *testableOffsetSkipChecker {
	store := &mockStore{}
	adminClient := &mockAdminClient{}

	// Create checker with nil store/admin - we'll inject our mocks
	checker := &OffsetSkipChecker{
		consumerGroup: consumerGroup,
		topic:         topic,
		checkInterval: 100 * time.Millisecond,
		stopCh:        make(chan struct{}),
	}

	return &testableOffsetSkipChecker{
		store:       store,
		adminClient: adminClient,
		checker:     checker,
	}
}

func TestOffsetSkipChecker_NoSkips(t *testing.T) {
	tc := newTestableChecker("test-group", "test-topic")
	tc.store.skips = []lrdb.GetKafkaOffsetSkipsRow{}

	// Create a checker that uses our mocks
	checker := &testableSkipChecker{
		store:         tc.store,
		adminClient:   tc.adminClient,
		consumerGroup: "test-group",
		topic:         "test-topic",
	}

	ctx := context.Background()
	applied := checker.checkAndApplySkips(ctx)

	assert.False(t, applied, "should return false when no skips found")
	assert.Equal(t, 1, tc.store.getCalled, "should query for skips")
	assert.Equal(t, 0, tc.adminClient.commitCalled, "should not commit when no skips")
	assert.Equal(t, 0, tc.store.deleteCalled, "should not delete when no skips")
}

func TestOffsetSkipChecker_WithSkips(t *testing.T) {
	tc := newTestableChecker("test-group", "test-topic")
	tc.store.skips = []lrdb.GetKafkaOffsetSkipsRow{
		{ConsumerGroup: "test-group", Topic: "test-topic", PartitionID: 0, SkipToOffset: 100},
		{ConsumerGroup: "test-group", Topic: "test-topic", PartitionID: 1, SkipToOffset: 200},
	}

	checker := &testableSkipChecker{
		store:         tc.store,
		adminClient:   tc.adminClient,
		consumerGroup: "test-group",
		topic:         "test-topic",
	}

	ctx := context.Background()
	applied := checker.checkAndApplySkips(ctx)

	assert.True(t, applied, "should return true when skips applied")
	assert.Equal(t, 1, tc.store.getCalled, "should query for skips")
	assert.Equal(t, 1, tc.adminClient.commitCalled, "should commit offsets")
	assert.Equal(t, 2, tc.store.deleteCalled, "should delete both skip entries")

	// Verify committed offsets
	require.Len(t, tc.adminClient.committedOffsets, 1)
	committed := tc.adminClient.committedOffsets[0]
	assert.Equal(t, "test-group", committed.groupID)
	assert.Equal(t, "test-topic", committed.topic)
	assert.Len(t, committed.offsets, 2)

	// Verify deleted entries
	assert.Len(t, tc.store.deletedSkips, 2)
}

func TestOffsetSkipChecker_CommitError(t *testing.T) {
	tc := newTestableChecker("test-group", "test-topic")
	tc.store.skips = []lrdb.GetKafkaOffsetSkipsRow{
		{ConsumerGroup: "test-group", Topic: "test-topic", PartitionID: 0, SkipToOffset: 100},
	}
	tc.adminClient.commitError = assert.AnError

	checker := &testableSkipChecker{
		store:         tc.store,
		adminClient:   tc.adminClient,
		consumerGroup: "test-group",
		topic:         "test-topic",
	}

	ctx := context.Background()
	applied := checker.checkAndApplySkips(ctx)

	assert.False(t, applied, "should return false on commit error")
	assert.Equal(t, 1, tc.adminClient.commitCalled, "should attempt commit")
	assert.Equal(t, 0, tc.store.deleteCalled, "should not delete on commit error")
}

// testableSkipChecker is a testable version that uses interfaces
type testableSkipChecker struct {
	store         skipStore
	adminClient   skipAdminClient
	consumerGroup string
	topic         string
}

type skipStore interface {
	GetKafkaOffsetSkips(ctx context.Context, params lrdb.GetKafkaOffsetSkipsParams) ([]lrdb.GetKafkaOffsetSkipsRow, error)
	DeleteKafkaOffsetSkip(ctx context.Context, params lrdb.DeleteKafkaOffsetSkipParams) error
}

type skipAdminClient interface {
	CommitConsumerGroupOffsets(ctx context.Context, groupID, topic string, offsets []PartitionOffset) error
}

func (c *testableSkipChecker) checkAndApplySkips(ctx context.Context) bool {
	skips, err := c.store.GetKafkaOffsetSkips(ctx, lrdb.GetKafkaOffsetSkipsParams{
		ConsumerGroup: c.consumerGroup,
		Topic:         c.topic,
	})
	if err != nil {
		return false
	}

	if len(skips) == 0 {
		return false
	}

	// Build offset list for committing
	offsets := make([]PartitionOffset, 0, len(skips))
	for _, skip := range skips {
		offsets = append(offsets, PartitionOffset{
			Partition: int(skip.PartitionID),
			Offset:    skip.SkipToOffset,
		})
	}

	// Commit the offsets to Kafka
	if err := c.adminClient.CommitConsumerGroupOffsets(ctx, c.consumerGroup, c.topic, offsets); err != nil {
		return false
	}

	// Delete the skip entries after successful commit
	for _, skip := range skips {
		_ = c.store.DeleteKafkaOffsetSkip(ctx, lrdb.DeleteKafkaOffsetSkipParams{
			ConsumerGroup: c.consumerGroup,
			Topic:         c.topic,
			PartitionID:   skip.PartitionID,
		})
	}

	return true
}
