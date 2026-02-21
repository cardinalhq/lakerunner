// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package workcoord

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkTracker_AddAndGet(t *testing.T) {
	tr := NewWorkTracker()
	item := &WorkItem{
		QueryID:  "q1",
		LeafID:   "l1",
		WorkID:   "w1",
		WorkerID: "worker-1",
		State:    WorkStateAssigned,
	}
	require.NoError(t, tr.Add(item))

	got, err := tr.Get("w1")
	require.NoError(t, err)
	assert.Equal(t, "q1", got.QueryID)
	assert.Equal(t, WorkStateAssigned, got.State)
}

func TestWorkTracker_AddDuplicate(t *testing.T) {
	tr := NewWorkTracker()
	item := &WorkItem{WorkID: "w1", QueryID: "q1", WorkerID: "worker-1", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item))

	err := tr.Add(&WorkItem{WorkID: "w1", QueryID: "q2", WorkerID: "worker-2", State: WorkStateAssigned})
	var dupErr *ErrDuplicateWork
	require.ErrorAs(t, err, &dupErr)
	assert.Equal(t, "w1", dupErr.WorkID)
}

func TestWorkTracker_GetNotFound(t *testing.T) {
	tr := NewWorkTracker()
	_, err := tr.Get("nonexistent")
	var notFound *ErrWorkNotFound
	require.ErrorAs(t, err, &notFound)
}

func TestWorkTracker_Transition(t *testing.T) {
	tr := NewWorkTracker()
	item := &WorkItem{WorkID: "w1", QueryID: "q1", WorkerID: "worker-1", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item))

	require.NoError(t, tr.Transition("w1", WorkStateAccepted))
	got, _ := tr.Get("w1")
	assert.Equal(t, WorkStateAccepted, got.State)
}

func TestWorkTracker_TransitionNotFound(t *testing.T) {
	tr := NewWorkTracker()
	err := tr.Transition("nonexistent", WorkStateAccepted)
	var notFound *ErrWorkNotFound
	require.ErrorAs(t, err, &notFound)
}

func TestWorkTracker_TransitionInvalid(t *testing.T) {
	tr := NewWorkTracker()
	item := &WorkItem{WorkID: "w1", QueryID: "q1", WorkerID: "worker-1", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item))

	err := tr.Transition("w1", WorkStateReady) // assigned->ready is invalid
	var invalidErr *ErrInvalidTransition
	require.ErrorAs(t, err, &invalidErr)
}

func TestWorkTracker_Complete(t *testing.T) {
	tr := NewWorkTracker()
	item := &WorkItem{WorkID: "w1", QueryID: "q1", LeafID: "l1", WorkerID: "worker-1", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item))
	require.NoError(t, tr.Transition("w1", WorkStateAccepted))

	artifact := ArtifactInfo{
		ArtifactURL:      "http://worker-1/artifacts/w1",
		ArtifactChecksum: "abc123",
		RowCount:         100,
	}
	require.NoError(t, tr.Complete("w1", artifact))

	got, _ := tr.Get("w1")
	assert.Equal(t, WorkStateReady, got.State)
	require.NotNil(t, got.ArtifactInfo)
	assert.Equal(t, "abc123", got.ArtifactInfo.ArtifactChecksum)
}

func TestWorkTracker_CompleteSameWorkIDTwice(t *testing.T) {
	tr := NewWorkTracker()

	// Complete a work item normally.
	item := &WorkItem{WorkID: "w1", QueryID: "q1", LeafID: "l1", WorkerID: "worker-1", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item))
	require.NoError(t, tr.Transition("w1", WorkStateAccepted))
	artifact := ArtifactInfo{ArtifactChecksum: "abc123"}
	require.NoError(t, tr.Complete("w1", artifact))

	// A duplicate message for the same work_id+checksum should be caught.
	// (The work is already in Ready state, so the transition itself will fail.)
	err := tr.Complete("w1", artifact)
	require.Error(t, err)
}

func TestWorkTracker_DistinctWorkIDsSameLeafNotDuplicate(t *testing.T) {
	tr := NewWorkTracker()

	// Two different work units in the same leaf producing the same checksum
	// are distinct — both completions must succeed.
	item1 := &WorkItem{WorkID: "w1", QueryID: "q1", LeafID: "l1", WorkerID: "worker-1", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item1))
	require.NoError(t, tr.Transition("w1", WorkStateAccepted))
	require.NoError(t, tr.Complete("w1", ArtifactInfo{ArtifactChecksum: "abc123"}))

	item2 := &WorkItem{WorkID: "w2", QueryID: "q1", LeafID: "l1", WorkerID: "worker-2", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item2))
	require.NoError(t, tr.Transition("w2", WorkStateAccepted))
	require.NoError(t, tr.Complete("w2", ArtifactInfo{ArtifactChecksum: "abc123"}),
		"different work_id should not be treated as duplicate even with same checksum")
}

func TestWorkTracker_DifferentChecksum(t *testing.T) {
	tr := NewWorkTracker()

	item1 := &WorkItem{WorkID: "w1", QueryID: "q1", LeafID: "l1", WorkerID: "worker-1", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item1))
	require.NoError(t, tr.Transition("w1", WorkStateAccepted))
	require.NoError(t, tr.Complete("w1", ArtifactInfo{ArtifactChecksum: "abc123"}))

	item2 := &WorkItem{WorkID: "w2", QueryID: "q1", LeafID: "l1", WorkerID: "worker-2", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item2))
	require.NoError(t, tr.Transition("w2", WorkStateAccepted))
	require.NoError(t, tr.Complete("w2", ArtifactInfo{ArtifactChecksum: "def456"}))
}

func TestWorkTracker_Reassign(t *testing.T) {
	tr := NewWorkTracker()
	item := &WorkItem{
		WorkID: "w1", QueryID: "q1", LeafID: "l1",
		WorkerID: "worker-1", State: WorkStateAssigned,
		AffinityKey: "seg-1",
	}
	require.NoError(t, tr.Add(item))

	newItem, err := tr.Reassign("w1", "w2", "worker-2")
	require.NoError(t, err)
	assert.Equal(t, "w2", newItem.WorkID)
	assert.Equal(t, "worker-2", newItem.WorkerID)
	assert.Equal(t, WorkStateAssigned, newItem.State)
	assert.Equal(t, "seg-1", newItem.AffinityKey)
	assert.Equal(t, "q1", newItem.QueryID)

	// Old item should be canceled.
	old, _ := tr.Get("w1")
	assert.Equal(t, WorkStateCanceled, old.State)
}

func TestWorkTracker_ReassignIDCollision(t *testing.T) {
	tr := NewWorkTracker()
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w1", QueryID: "q1", LeafID: "l1", WorkerID: "worker-1", State: WorkStateAssigned}))
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w2", QueryID: "q1", LeafID: "l2", WorkerID: "worker-1", State: WorkStateAssigned}))

	// Reassign w1 with new ID "w2" — collides with existing work.
	_, err := tr.Reassign("w1", "w2", "worker-2")
	var dupErr *ErrDuplicateWork
	require.ErrorAs(t, err, &dupErr)
	assert.Equal(t, "w2", dupErr.WorkID)

	// w1 should NOT be canceled since the reassignment failed.
	got, _ := tr.Get("w1")
	assert.Equal(t, WorkStateAssigned, got.State)
}

func TestWorkTracker_ReassignTerminalFails(t *testing.T) {
	tr := NewWorkTracker()
	item := &WorkItem{WorkID: "w1", QueryID: "q1", LeafID: "l1", WorkerID: "worker-1", State: WorkStateAssigned}
	require.NoError(t, tr.Add(item))
	require.NoError(t, tr.Transition("w1", WorkStateAccepted))
	require.NoError(t, tr.Transition("w1", WorkStateFailed))

	_, err := tr.Reassign("w1", "w2", "worker-2")
	require.Error(t, err)
}

func TestWorkTracker_WorkForQuery(t *testing.T) {
	tr := NewWorkTracker()
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w1", QueryID: "q1", WorkerID: "worker-1", State: WorkStateAssigned}))
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w2", QueryID: "q1", WorkerID: "worker-2", State: WorkStateAssigned}))
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w3", QueryID: "q2", WorkerID: "worker-1", State: WorkStateAssigned}))

	items := tr.WorkForQuery("q1")
	assert.Len(t, items, 2)

	items = tr.WorkForQuery("q2")
	assert.Len(t, items, 1)

	items = tr.WorkForQuery("nonexistent")
	assert.Empty(t, items)
}

func TestWorkTracker_WorkForWorker(t *testing.T) {
	tr := NewWorkTracker()
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w1", QueryID: "q1", WorkerID: "worker-1", State: WorkStateAssigned}))
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w2", QueryID: "q1", WorkerID: "worker-1", State: WorkStateAssigned}))
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w3", QueryID: "q1", WorkerID: "worker-2", State: WorkStateAssigned}))

	items := tr.WorkForWorker("worker-1")
	assert.Len(t, items, 2)

	// Transition w1 to terminal — should no longer appear.
	require.NoError(t, tr.Transition("w1", WorkStateRejected))
	items = tr.WorkForWorker("worker-1")
	assert.Len(t, items, 1)
	assert.Equal(t, "w2", items[0].WorkID)
}

func TestWorkTracker_RemoveQuery(t *testing.T) {
	tr := NewWorkTracker()
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w1", QueryID: "q1", LeafID: "l1", WorkerID: "worker-1", State: WorkStateAssigned}))
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w2", QueryID: "q1", LeafID: "l2", WorkerID: "worker-2", State: WorkStateAssigned}))
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w3", QueryID: "q2", LeafID: "l1", WorkerID: "worker-1", State: WorkStateAssigned}))

	tr.RemoveQuery("q1")
	assert.Equal(t, 1, tr.TotalCount())
	assert.Empty(t, tr.WorkForQuery("q1"))
	assert.Len(t, tr.WorkForQuery("q2"), 1)

	// w1 and w2 should be gone.
	_, err := tr.Get("w1")
	require.Error(t, err)
	_, err = tr.Get("w2")
	require.Error(t, err)
}

func TestWorkTracker_PendingCount(t *testing.T) {
	tr := NewWorkTracker()
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w1", QueryID: "q1", WorkerID: "worker-1", State: WorkStateAssigned}))
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w2", QueryID: "q1", WorkerID: "worker-1", State: WorkStateAssigned}))

	assert.Equal(t, 2, tr.PendingCount())

	require.NoError(t, tr.Transition("w1", WorkStateRejected))
	assert.Equal(t, 1, tr.PendingCount())
}

func TestWorkTracker_GetReturnsSnapshot(t *testing.T) {
	tr := NewWorkTracker()
	require.NoError(t, tr.Add(&WorkItem{WorkID: "w1", QueryID: "q1", WorkerID: "worker-1", State: WorkStateAssigned}))

	got, err := tr.Get("w1")
	require.NoError(t, err)
	got.State = WorkStateFailed                 // mutate the copy
	assert.Equal(t, WorkStateFailed, got.State) // sanity check mutation happened

	got2, err := tr.Get("w1")
	require.NoError(t, err)
	assert.Equal(t, WorkStateAssigned, got2.State, "original should be unchanged")
}
