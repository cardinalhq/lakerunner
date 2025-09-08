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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestOffsetTracker_FirstMessage(t *testing.T) {
	store := NewMockOffsetStore()
	orgID := uuid.New()
	instanceNum := int16(1)

	tracker := NewOffsetTracker(store)

	metadata := &MessageMetadata{
		Topic:         "test-topic",
		Partition:     0,
		ConsumerGroup: "test-group",
		Offset:        100,
	}

	// First message - should process since no DB entry exists
	shouldProcess, err := tracker.ShouldProcessMessage(context.Background(), metadata, orgID, instanceNum)
	assert.NoError(t, err)
	assert.True(t, shouldProcess)
}

func TestOffsetTracker_SequentialMessages(t *testing.T) {
	store := NewMockOffsetStore()
	orgID := uuid.New()
	instanceNum := int16(1)

	tracker := NewOffsetTracker(store)

	// First message
	metadata1 := &MessageMetadata{
		Topic:         "test-topic",
		Partition:     0,
		ConsumerGroup: "test-group",
		Offset:        100,
	}

	shouldProcess1, err := tracker.ShouldProcessMessage(context.Background(), metadata1, orgID, instanceNum)
	assert.NoError(t, err)
	assert.True(t, shouldProcess1)

	// Next sequential message (+1)
	metadata2 := &MessageMetadata{
		Topic:         "test-topic",
		Partition:     0,
		ConsumerGroup: "test-group",
		Offset:        101,
	}

	shouldProcess2, err := tracker.ShouldProcessMessage(context.Background(), metadata2, orgID, instanceNum)
	assert.NoError(t, err)
	assert.True(t, shouldProcess2)
}

func TestOffsetTracker_DuplicateMessage(t *testing.T) {
	store := NewMockOffsetStore()
	orgID := uuid.New()
	instanceNum := int16(1)

	tracker := NewOffsetTracker(store)

	metadata := &MessageMetadata{
		Topic:         "test-topic",
		Partition:     0,
		ConsumerGroup: "test-group",
		Offset:        100,
	}

	// Process first message
	shouldProcess1, err := tracker.ShouldProcessMessage(context.Background(), metadata, orgID, instanceNum)
	assert.NoError(t, err)
	assert.True(t, shouldProcess1)

	// Try to process same message again
	shouldProcess2, err := tracker.ShouldProcessMessage(context.Background(), metadata, orgID, instanceNum)
	assert.NoError(t, err)
	assert.False(t, shouldProcess2) // Should be dropped
}

func TestOffsetTracker_WithExistingDBOffset(t *testing.T) {
	store := NewMockOffsetStore()
	orgID := uuid.New()
	instanceNum := int16(1)

	// Set existing offset in "DB"
	store.SetOffset("test-topic", 0, "test-group", orgID, instanceNum, 105)

	tracker := NewOffsetTracker(store)

	// Try to process older message
	metadata1 := &MessageMetadata{
		Topic:         "test-topic",
		Partition:     0,
		ConsumerGroup: "test-group",
		Offset:        100,
	}

	shouldProcess1, err := tracker.ShouldProcessMessage(context.Background(), metadata1, orgID, instanceNum)
	assert.NoError(t, err)
	assert.False(t, shouldProcess1) // Should be dropped - already processed

	// Process newer message
	metadata2 := &MessageMetadata{
		Topic:         "test-topic",
		Partition:     0,
		ConsumerGroup: "test-group",
		Offset:        106,
	}

	shouldProcess2, err := tracker.ShouldProcessMessage(context.Background(), metadata2, orgID, instanceNum)
	assert.NoError(t, err)
	assert.True(t, shouldProcess2) // Should be processed
}
