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
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type seqIDGen struct {
	counter atomic.Int64
}

func (g *seqIDGen) NewWorkID() string {
	return fmt.Sprintf("reassigned-%d", g.counter.Add(1))
}

func newTestCoordinator() *Coordinator {
	return NewCoordinator(&seqIDGen{})
}

// registerAvailable registers workers and marks them as accepting work.
func registerAvailable(t *testing.T, c *Coordinator, workerIDs ...string) {
	t.Helper()
	for _, id := range workerIDs {
		require.NoError(t, c.RegisterWorker(id))
		require.NoError(t, c.Workers.SetAcceptingWork(id, true))
	}
}

func TestCoordinator_RegisterAndRemoveWorker(t *testing.T) {
	c := newTestCoordinator()
	require.NoError(t, c.RegisterWorker("w1"))
	assert.Equal(t, 1, c.Workers.Count())

	_, err := c.RemoveWorker("w1")
	require.NoError(t, err)
	assert.Equal(t, 0, c.Workers.Count())
}

func TestCoordinator_AssignWork(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2")

	item, err := c.AssignWork("q1", "l1", "work-1", "seg-100", nil)
	require.NoError(t, err)
	assert.Equal(t, "q1", item.QueryID)
	assert.Equal(t, "work-1", item.WorkID)
	assert.Equal(t, WorkStateAssigned, item.State)
	assert.Contains(t, []string{"w1", "w2"}, item.WorkerID)
}

func TestCoordinator_AssignWork_NoWorkers(t *testing.T) {
	c := newTestCoordinator()
	_, err := c.AssignWork("q1", "l1", "work-1", "seg-100", nil)
	require.ErrorIs(t, err, ErrNoAvailableWorkers)
}

func TestCoordinator_AssignWorkToWorker(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")

	item, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)
	assert.Equal(t, "w1", item.WorkerID)
}

func TestCoordinator_AssignWorkToWorker_Unavailable(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")

	// Disconnect makes worker unavailable.
	require.NoError(t, c.Workers.Disconnect("w1"))
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	var unavail *ErrWorkerUnavailable
	require.ErrorAs(t, err, &unavail)

	// Draining makes worker unavailable.
	require.NoError(t, c.Workers.Reconnect("w1"))
	require.NoError(t, c.Workers.BeginDrain("w1"))
	_, err = c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.ErrorAs(t, err, &unavail)

	// Not accepting makes worker unavailable.
	require.NoError(t, c.Workers.Reconnect("w1"))
	require.NoError(t, c.Workers.SetAcceptingWork("w1", false))
	_, err = c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.ErrorAs(t, err, &unavail)
}

func TestCoordinator_AssignWorkToWorker_NotFound(t *testing.T) {
	c := newTestCoordinator()
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "nonexistent", nil)
	var notFound *ErrWorkerNotFound
	require.ErrorAs(t, err, &notFound)
}

func TestCoordinator_FullWorkLifecycle(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")

	item, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)
	assert.Equal(t, WorkStateAssigned, item.State)

	require.NoError(t, c.HandleWorkAccepted("work-1"))
	require.NoError(t, c.HandleWorkReady("work-1", ArtifactInfo{
		ArtifactURL:      "http://w1/artifacts/work-1",
		ArtifactChecksum: "sha256-abc",
		RowCount:         500,
		MinTs:            1000,
		MaxTs:            2000,
	}))

	got, err := c.Work.Get("work-1")
	require.NoError(t, err)
	assert.Equal(t, WorkStateReady, got.State)
	require.NotNil(t, got.ArtifactInfo)
	assert.Equal(t, int64(500), got.ArtifactInfo.RowCount)
}

func TestCoordinator_WorkRejected_Reassigns(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2")
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)

	// w1 rejects — should reassign to w2.
	require.NoError(t, c.Workers.BeginDrain("w1"))
	reassigned, err := c.HandleWorkRejected("work-1")
	require.NoError(t, err)
	require.NotNil(t, reassigned)
	assert.Equal(t, "w2", reassigned.NewWorkerID)
	assert.Equal(t, "work-1", reassigned.OldWorkID)

	// Old item should be rejected.
	got, _ := c.Work.Get("work-1")
	assert.Equal(t, WorkStateRejected, got.State)

	// New item should be assigned on w2.
	newGot, err := c.Work.Get(reassigned.NewWorkID)
	require.NoError(t, err)
	assert.Equal(t, WorkStateAssigned, newGot.State)
	assert.Equal(t, "w2", newGot.WorkerID)
}

func TestCoordinator_WorkRejected_NoOtherWorkers(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)

	// w1 rejects and is the only worker — no reassignment possible.
	require.NoError(t, c.Workers.BeginDrain("w1"))
	reassigned, err := c.HandleWorkRejected("work-1")
	require.NoError(t, err)
	assert.Nil(t, reassigned, "no reassignment when no workers available")

	got, _ := c.Work.Get("work-1")
	assert.Equal(t, WorkStateRejected, got.State)
}

func TestCoordinator_WorkRejected_ExcludesRejectingWorker(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2")

	// Assign to w1 (w1 is still available/healthy).
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)

	// w1 rejects but is still technically available — reassignment must
	// still exclude w1 to avoid bounce loops.
	reassigned, err := c.HandleWorkRejected("work-1")
	require.NoError(t, err)
	require.NotNil(t, reassigned)
	assert.Equal(t, "w2", reassigned.NewWorkerID,
		"must not reassign back to the rejecting worker")
}

func TestCoordinator_WorkRejected_DuplicateIsNoop(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2")
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)

	// First reject succeeds.
	_, err = c.HandleWorkRejected("work-1")
	require.NoError(t, err)

	// Duplicate reject is a no-op, not an error.
	reassigned, err := c.HandleWorkRejected("work-1")
	require.NoError(t, err)
	assert.Nil(t, reassigned)
}

func TestCoordinator_WorkFailed(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)
	require.NoError(t, c.HandleWorkAccepted("work-1"))

	require.NoError(t, c.HandleWorkFailed("work-1"))

	got, _ := c.Work.Get("work-1")
	assert.Equal(t, WorkStateFailed, got.State)
}

func TestCoordinator_CancelWork(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)

	require.NoError(t, c.CancelWork("work-1"))

	got, _ := c.Work.Get("work-1")
	assert.Equal(t, WorkStateCanceled, got.State)
}

func TestCoordinator_CancelQueryWork(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)
	_, err = c.AssignWorkToWorker("q1", "l2", "work-2", "seg-2", "w1", nil)
	require.NoError(t, err)
	_, err = c.AssignWorkToWorker("q2", "l1", "work-3", "seg-3", "w1", nil)
	require.NoError(t, err)

	c.CancelQueryWork("q1")

	// q1 work should be gone.
	assert.Empty(t, c.Work.WorkForQuery("q1"))
	// q2 work should be unaffected.
	assert.Len(t, c.Work.WorkForQuery("q2"), 1)
}

func TestCoordinator_DisconnectWorker_ReassignsWork(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2")

	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)
	_, err = c.AssignWorkToWorker("q1", "l2", "work-2", "seg-2", "w1", nil)
	require.NoError(t, err)

	reassigned, err := c.DisconnectWorker("w1")
	require.NoError(t, err)
	assert.Len(t, reassigned, 2)

	// Each reassigned work should be on w2 (only available worker).
	for _, r := range reassigned {
		assert.Equal(t, "w2", r.NewWorkerID)
		got, err := c.Work.Get(r.NewWorkID)
		require.NoError(t, err)
		assert.Equal(t, WorkStateAssigned, got.State)
		assert.Equal(t, "w2", got.WorkerID)
	}

	// Original work items should be canceled.
	old1, _ := c.Work.Get("work-1")
	assert.Equal(t, WorkStateCanceled, old1.State)
	old2, _ := c.Work.Get("work-2")
	assert.Equal(t, WorkStateCanceled, old2.State)
}

func TestCoordinator_DisconnectWorker_NoOtherWorkers(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")

	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)

	// Disconnect w1 — no other workers to reassign to.
	reassigned, err := c.DisconnectWorker("w1")
	require.NoError(t, err)
	assert.Empty(t, reassigned)

	// Work should be canceled since no workers are available.
	got, _ := c.Work.Get("work-1")
	assert.Equal(t, WorkStateCanceled, got.State)
}

func TestCoordinator_DrainWorker_ReassignsUnstarted(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2")

	// work-1: assigned (not yet accepted) — should be reassigned.
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)

	// work-2: accepted (in-flight) — should NOT be reassigned.
	_, err = c.AssignWorkToWorker("q1", "l2", "work-2", "seg-2", "w1", nil)
	require.NoError(t, err)
	require.NoError(t, c.HandleWorkAccepted("work-2"))

	reassigned, err := c.DrainWorker("w1")
	require.NoError(t, err)
	assert.Len(t, reassigned, 1)
	assert.Equal(t, "work-1", reassigned[0].OldWorkID)
	assert.Equal(t, "w2", reassigned[0].NewWorkerID)

	// work-2 should still be accepted on w1.
	got, _ := c.Work.Get("work-2")
	assert.Equal(t, WorkStateAccepted, got.State)
	assert.Equal(t, "w1", got.WorkerID)
}

func TestCoordinator_RemoveWorker_CancelsWork(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)

	canceled, err := c.RemoveWorker("w1")
	require.NoError(t, err)
	assert.Len(t, canceled, 1)
	assert.Equal(t, "work-1", canceled[0].WorkID)

	got, _ := c.Work.Get("work-1")
	assert.Equal(t, WorkStateCanceled, got.State)
}

func TestCoordinator_QueryWorkStatus(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")

	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)
	_, err = c.AssignWorkToWorker("q1", "l2", "work-2", "seg-2", "w1", nil)
	require.NoError(t, err)
	require.NoError(t, c.HandleWorkAccepted("work-2"))

	status := c.QueryWorkStatus("q1")
	assert.Equal(t, 1, status[WorkStateAssigned])
	assert.Equal(t, 1, status[WorkStateAccepted])
}

func TestCoordinator_IdempotencyDedup_SameWorkID(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1")

	// Complete work-1.
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)
	require.NoError(t, c.HandleWorkAccepted("work-1"))
	require.NoError(t, c.HandleWorkReady("work-1", ArtifactInfo{ArtifactChecksum: "checksum-1"}))

	// A duplicate completion message for the same work_id is caught
	// (already in terminal Ready state).
	err = c.HandleWorkReady("work-1", ArtifactInfo{ArtifactChecksum: "checksum-1"})
	require.Error(t, err)
}

func TestCoordinator_DistinctWorkUnits_SameLeaf_NotDuplicate(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2")

	// Two distinct work units for the same leaf (different segment groups).
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", nil)
	require.NoError(t, err)
	require.NoError(t, c.HandleWorkAccepted("work-1"))
	require.NoError(t, c.HandleWorkReady("work-1", ArtifactInfo{ArtifactChecksum: "checksum-1"}))

	_, err = c.AssignWorkToWorker("q1", "l1", "work-2", "seg-2", "w2", nil)
	require.NoError(t, err)
	require.NoError(t, c.HandleWorkAccepted("work-2"))
	require.NoError(t, c.HandleWorkReady("work-2", ArtifactInfo{ArtifactChecksum: "checksum-1"}),
		"different work_id must not be treated as duplicate")
}

func TestCoordinator_AffinityStability(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2", "w3")

	// Same affinity key should always go to the same worker.
	item1, err := c.AssignWork("q1", "l1", "work-1", "seg-42", nil)
	require.NoError(t, err)

	// Clean up and re-assign with same key.
	c.CancelQueryWork("q1")

	item2, err := c.AssignWork("q2", "l1", "work-2", "seg-42", nil)
	require.NoError(t, err)
	assert.Equal(t, item1.WorkerID, item2.WorkerID,
		"same affinity key should map to same worker")
}

func TestCoordinator_SpecPropagatedOnReject(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2")

	spec := []byte(`{"table":"logs","filter":"severity>3"}`)
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", spec)
	require.NoError(t, err)

	// w1 rejects — spec should propagate to the reassigned work item.
	require.NoError(t, c.Workers.BeginDrain("w1"))
	reassigned, err := c.HandleWorkRejected("work-1")
	require.NoError(t, err)
	require.NotNil(t, reassigned)

	newItem, err := c.Work.Get(reassigned.NewWorkID)
	require.NoError(t, err)
	assert.Equal(t, spec, newItem.Spec)
}

func TestCoordinator_SpecPropagatedOnDisconnect(t *testing.T) {
	c := newTestCoordinator()
	registerAvailable(t, c, "w1", "w2")

	spec := []byte(`{"table":"metrics","rollup":"5m"}`)
	_, err := c.AssignWorkToWorker("q1", "l1", "work-1", "seg-1", "w1", spec)
	require.NoError(t, err)

	reassigned, err := c.DisconnectWorker("w1")
	require.NoError(t, err)
	require.Len(t, reassigned, 1)

	newItem, err := c.Work.Get(reassigned[0].NewWorkID)
	require.NoError(t, err)
	assert.Equal(t, spec, newItem.Spec)
}
