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

package workmanager

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/core/workcoordpb"
)

// mockStreamSender records sent messages.
type mockStreamSender struct {
	mu       sync.Mutex
	messages map[string][]*workcoordpb.APIMessage // workerID → messages
	failNext bool
}

func newMockStreamSender() *mockStreamSender {
	return &mockStreamSender{
		messages: make(map[string][]*workcoordpb.APIMessage),
	}
}

func (m *mockStreamSender) Send(workerID string, msg *workcoordpb.APIMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failNext {
		m.failNext = false
		return assert.AnError
	}
	m.messages[workerID] = append(m.messages[workerID], msg)
	return nil
}

func (m *mockStreamSender) messagesFor(workerID string) []*workcoordpb.APIMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.messages[workerID]
}

// mockArtifactFetcher returns predetermined data.
type mockArtifactFetcher struct {
	data []byte
	err  error
}

func (m *mockArtifactFetcher) FetchArtifact(_ context.Context, _ string, _ string) ([]byte, error) {
	return m.data, m.err
}

func setupManager(t *testing.T) (*Manager, *mockStreamSender) {
	t.Helper()
	sender := newMockStreamSender()
	mgr := NewManager(sender, &mockArtifactFetcher{data: []byte("parquet-data")})
	// Register a worker and mark it as accepting work.
	require.NoError(t, mgr.Coordinator().RegisterWorker("worker-1"))
	require.NoError(t, mgr.Coordinator().Workers.SetAcceptingWork("worker-1", true))
	return mgr, sender
}

func TestManager_DispatchWork(t *testing.T) {
	mgr, sender := setupManager(t)

	workID, err := mgr.DispatchWork("q1", "l1", "org:seg1", []byte("spec"))
	require.NoError(t, err)
	assert.NotEmpty(t, workID)

	// Verify message was sent.
	msgs := sender.messagesFor("worker-1")
	require.Len(t, msgs, 1)

	assign := msgs[0].GetAssignWork()
	require.NotNil(t, assign)
	assert.Equal(t, "q1", assign.QueryId)
	assert.Equal(t, "l1", assign.LeafId)
	assert.Equal(t, workID, assign.WorkId)
}

func TestManager_DispatchWork_NoWorkers(t *testing.T) {
	sender := newMockStreamSender()
	mgr := NewManager(sender, nil)

	_, err := mgr.DispatchWork("q1", "l1", "org:seg1", []byte("spec"))
	assert.Error(t, err)
}

func TestManager_DispatchWork_SendFails(t *testing.T) {
	mgr, sender := setupManager(t)

	sender.mu.Lock()
	sender.failNext = true
	sender.mu.Unlock()

	_, err := mgr.DispatchWork("q1", "l1", "org:seg1", []byte("spec"))
	assert.Error(t, err)
}

func TestManager_HandleWorkAccepted(t *testing.T) {
	mgr, _ := setupManager(t)

	workID, err := mgr.DispatchWork("q1", "l1", "org:seg1", []byte("spec"))
	require.NoError(t, err)

	mgr.HandleWorkAccepted("worker-1", &workcoordpb.WorkAccepted{WorkId: workID})

	item, err := mgr.Coordinator().Work.Get(workID)
	require.NoError(t, err)
	assert.Equal(t, "accepted", item.State.String())
}

func TestManager_HandleWorkReady_DeliversResult(t *testing.T) {
	mgr, sender := setupManager(t)

	resultsCh, _ := mgr.StartQuery(t.Context(), "q1", 16)

	workID, err := mgr.DispatchWork("q1", "l1", "org:seg1", []byte("spec"))
	require.NoError(t, err)

	// Accept then complete.
	mgr.HandleWorkAccepted("worker-1", &workcoordpb.WorkAccepted{WorkId: workID})
	mgr.HandleWorkReady("worker-1", &workcoordpb.WorkReady{
		WorkId:            workID,
		ArtifactUrl:       "http://worker-1:8081/artifacts/" + workID,
		ArtifactSizeBytes: 1024,
		ArtifactChecksum:  "sha256:abc",
		RowCount:          100,
		MinTs:             1000,
		MaxTs:             2000,
	})

	// Should receive result on the channel.
	select {
	case result := <-resultsCh:
		assert.Equal(t, workID, result.WorkID)
		assert.Equal(t, "q1", result.QueryID)
		assert.Equal(t, "l1", result.LeafID)
		assert.Equal(t, []byte("parquet-data"), result.ArtifactData)
		assert.Equal(t, int64(100), result.RowCount)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for result")
	}

	// Should have sent ArtifactAck.
	msgs := sender.messagesFor("worker-1")
	var ackFound bool
	for _, m := range msgs {
		if ack := m.GetArtifactAck(); ack != nil && ack.WorkId == workID {
			ackFound = true
		}
	}
	assert.True(t, ackFound)
}

func TestManager_HandleWorkFailed_DeliversError(t *testing.T) {
	mgr, _ := setupManager(t)

	_, errCh := mgr.StartQuery(t.Context(), "q1", 16)

	workID, err := mgr.DispatchWork("q1", "l1", "org:seg1", []byte("spec"))
	require.NoError(t, err)

	mgr.HandleWorkAccepted("worker-1", &workcoordpb.WorkAccepted{WorkId: workID})
	mgr.HandleWorkFailed("worker-1", &workcoordpb.WorkFailed{WorkId: workID, Reason: "OOM"})

	select {
	case workErr := <-errCh:
		assert.Contains(t, workErr.Error(), "OOM")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for error")
	}
}

func TestManager_CancelQuery(t *testing.T) {
	mgr, _ := setupManager(t)

	resultsCh, _ := mgr.StartQuery(t.Context(), "q1", 16)

	workID, err := mgr.DispatchWork("q1", "l1", "org:seg1", []byte("spec"))
	require.NoError(t, err)
	assert.NotEmpty(t, workID)

	mgr.CancelQuery("q1")

	// Results channel should eventually close.
	select {
	case _, ok := <-resultsCh:
		assert.False(t, ok)
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for results channel to close")
	}
}

func TestManager_CancelQuery_SendsCancelWork(t *testing.T) {
	mgr, sender := setupManager(t)

	_, _ = mgr.StartQuery(t.Context(), "q1", 16)

	workID, err := mgr.DispatchWork("q1", "l1", "org:seg1", []byte("spec"))
	require.NoError(t, err)

	// Accept the work so it's in a non-terminal state.
	mgr.HandleWorkAccepted("worker-1", &workcoordpb.WorkAccepted{WorkId: workID})

	mgr.CancelQuery("q1")

	// Verify a CancelWork message was sent for the in-flight work.
	msgs := sender.messagesFor("worker-1")
	var cancelFound bool
	for _, m := range msgs {
		if cw := m.GetCancelWork(); cw != nil && cw.WorkId == workID {
			cancelFound = true
		}
	}
	assert.True(t, cancelFound, "expected CancelWork message for work %s", workID)
}

func TestManager_HandleWorkerStatus_RegistersAndDrains(t *testing.T) {
	sender := newMockStreamSender()
	mgr := NewManager(sender, nil)

	// Status with accepting_work=true should register the worker.
	mgr.HandleWorkerStatus("worker-new", &workcoordpb.WorkerStatus{
		WorkerId:      "worker-new",
		AcceptingWork: true,
		Draining:      false,
	})

	w, err := mgr.Coordinator().Workers.Get("worker-new")
	require.NoError(t, err)
	assert.True(t, w.AcceptingWork)
	assert.False(t, w.Draining)

	// Drain status should set draining.
	mgr.HandleWorkerStatus("worker-new", &workcoordpb.WorkerStatus{
		WorkerId:      "worker-new",
		AcceptingWork: false,
		Draining:      true,
	})

	w, err = mgr.Coordinator().Workers.Get("worker-new")
	require.NoError(t, err)
	assert.True(t, w.Draining)
}

func TestManager_HandleWorkerDisconnected_ReassignsWork(t *testing.T) {
	sender := newMockStreamSender()
	mgr := NewManager(sender, nil)

	// Register two workers and mark them as accepting work.
	require.NoError(t, mgr.Coordinator().RegisterWorker("worker-1"))
	require.NoError(t, mgr.Coordinator().Workers.SetAcceptingWork("worker-1", true))
	require.NoError(t, mgr.Coordinator().RegisterWorker("worker-2"))
	require.NoError(t, mgr.Coordinator().Workers.SetAcceptingWork("worker-2", true))

	// Dispatch work (will go to one of them via rendezvous hashing).
	workID, err := mgr.DispatchWork("q1", "l1", "org:seg1", []byte("spec"))
	require.NoError(t, err)

	item, err := mgr.Coordinator().Work.Get(workID)
	require.NoError(t, err)
	originalWorker := item.WorkerID

	// Disconnect the worker that has the work.
	mgr.HandleWorkerDisconnected(originalWorker)

	// The work should have been reassigned — verify by checking that an
	// AssignWork message was sent to the other worker.
	otherWorker := "worker-1"
	if originalWorker == "worker-1" {
		otherWorker = "worker-2"
	}

	require.Eventually(t, func() bool {
		msgs := sender.messagesFor(otherWorker)
		for _, m := range msgs {
			if a := m.GetAssignWork(); a != nil && a.QueryId == "q1" {
				return true
			}
		}
		return false
	}, 3*time.Second, 50*time.Millisecond)
}

func TestManager_HandleWorkerStatus_ReconnectsDisconnectedWorker(t *testing.T) {
	sender := newMockStreamSender()
	mgr := NewManager(sender, nil)

	// First status registers the worker.
	mgr.HandleWorkerStatus("worker-1", &workcoordpb.WorkerStatus{
		WorkerId:      "worker-1",
		AcceptingWork: true,
	})
	w, err := mgr.Coordinator().Workers.Get("worker-1")
	require.NoError(t, err)
	assert.True(t, w.Alive)
	assert.True(t, w.AcceptingWork)

	// Simulate disconnect.
	require.NoError(t, mgr.Coordinator().Workers.Disconnect("worker-1"))
	w, err = mgr.Coordinator().Workers.Get("worker-1")
	require.NoError(t, err)
	assert.False(t, w.Alive)

	// Status after reconnect should restore Alive=true.
	mgr.HandleWorkerStatus("worker-1", &workcoordpb.WorkerStatus{
		WorkerId:      "worker-1",
		AcceptingWork: true,
	})
	w, err = mgr.Coordinator().Workers.Get("worker-1")
	require.NoError(t, err)
	assert.True(t, w.Alive)
	assert.True(t, w.AcceptingWork)
}

func TestManager_DispatchLeafWork(t *testing.T) {
	mgr, sender := setupManager(t)

	type testReq struct {
		Table string `json:"table"`
		Limit int    `json:"limit"`
	}

	workID, err := mgr.DispatchLeafWork("q1", "l1", "org:seg1", &testReq{Table: "logs", Limit: 100})
	require.NoError(t, err)
	assert.NotEmpty(t, workID)

	// Verify an AssignWork message was sent with JSON spec.
	msgs := sender.messagesFor("worker-1")
	require.NotEmpty(t, msgs)
	assign := msgs[0].GetAssignWork()
	require.NotNil(t, assign)
	assert.Equal(t, "q1", assign.QueryId)
	assert.Contains(t, string(assign.Spec), `"table":"logs"`)
}

func TestManager_DispatchLeafWork_MarshalError(t *testing.T) {
	mgr, _ := setupManager(t)

	// Channels can't be marshaled to JSON.
	_, err := mgr.DispatchLeafWork("q1", "l1", "org:seg1", make(chan int))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "marshal spec")
}

func TestManager_MakeStreamHandler(t *testing.T) {
	mgr, _ := setupManager(t)
	handler := mgr.MakeStreamHandler()
	assert.NotNil(t, handler)

	// Verify it implements the interface by calling a method.
	handler.OnWorkerStatus("worker-1", &workcoordpb.WorkerStatus{
		WorkerId:      "worker-1",
		AcceptingWork: true,
	})
}
