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

package streammanager

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/cardinalhq/lakerunner/core/workcoordpb"
)

// mockHandler records incoming messages for assertions.
type mockHandler struct {
	mu           sync.Mutex
	statuses     []*workcoordpb.WorkerStatus
	accepted     []*workcoordpb.WorkAccepted
	rejected     []*workcoordpb.WorkRejected
	ready        []*workcoordpb.WorkReady
	failed       []*workcoordpb.WorkFailed
	disconnected []string
}

func (m *mockHandler) OnWorkerStatus(_ string, msg *workcoordpb.WorkerStatus) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.statuses = append(m.statuses, msg)
}

func (m *mockHandler) OnWorkAccepted(_ string, msg *workcoordpb.WorkAccepted) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.accepted = append(m.accepted, msg)
}

func (m *mockHandler) OnWorkRejected(_ string, msg *workcoordpb.WorkRejected) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rejected = append(m.rejected, msg)
}

func (m *mockHandler) OnWorkReady(_ string, msg *workcoordpb.WorkReady) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ready = append(m.ready, msg)
}

func (m *mockHandler) OnWorkFailed(_ string, msg *workcoordpb.WorkFailed) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed = append(m.failed, msg)
}

func (m *mockHandler) OnWorkerDisconnected(workerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.disconnected = append(m.disconnected, workerID)
}

// echoWorkHandler responds to AssignWork with WorkAccepted.
type echoWorkHandler struct{}

func (e *echoWorkHandler) OnAssignWork(session sessionSender, msg *workcoordpb.AssignWork) {
	session.Send(&workcoordpb.WorkerMessage{
		Msg: &workcoordpb.WorkerMessage_WorkAccepted{
			WorkAccepted: &workcoordpb.WorkAccepted{WorkId: msg.WorkId},
		},
	})
}

func (e *echoWorkHandler) OnCancelWork(_ sessionSender, _ *workcoordpb.CancelWork)   {}
func (e *echoWorkHandler) OnArtifactAck(_ sessionSender, _ *workcoordpb.ArtifactAck) {}
func (e *echoWorkHandler) OnSessionClosed(_ string)                                  {}

// sessionSender is the interface the echo handler needs. Matches Session.Send signature.
type sessionSender interface {
	Send(msg *workcoordpb.WorkerMessage) bool
}

// testWorkerServer is a minimal WorkControlService server for testing.
type testWorkerServer struct {
	workcoordpb.UnimplementedWorkControlServiceServer
	workerID string
	handler  *echoWorkHandler
}

func (s *testWorkerServer) ControlStream(stream workcoordpb.WorkControlService_ControlStreamServer) error {
	// Send initial WorkerStatus.
	if err := stream.Send(&workcoordpb.WorkerMessage{
		Msg: &workcoordpb.WorkerMessage_WorkerStatus{
			WorkerStatus: &workcoordpb.WorkerStatus{
				WorkerId:      s.workerID,
				AcceptingWork: true,
				Draining:      false,
			},
		},
	}); err != nil {
		return err
	}

	// Send heartbeats.
	ctx := stream.Context()
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = stream.Send(&workcoordpb.WorkerMessage{
					Msg: &workcoordpb.WorkerMessage_Heartbeat{
						Heartbeat: &workcoordpb.Heartbeat{TimestampNs: time.Now().UnixNano()},
					},
				})
			}
		}
	}()

	// Read and handle messages.
	for {
		msg, err := stream.Recv()
		if err != nil {
			return nil
		}
		switch m := msg.Msg.(type) {
		case *workcoordpb.APIMessage_AssignWork:
			_ = stream.Send(&workcoordpb.WorkerMessage{
				Msg: &workcoordpb.WorkerMessage_WorkAccepted{
					WorkAccepted: &workcoordpb.WorkAccepted{WorkId: m.AssignWork.WorkId},
				},
			})
		}
	}
}

func startTestWorkerServer(t *testing.T, workerID string) (int, func()) {
	t.Helper()

	lis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port

	srv := grpc.NewServer()
	workcoordpb.RegisterWorkControlServiceServer(srv, &testWorkerServer{
		workerID: workerID,
		handler:  &echoWorkHandler{},
	})

	go func() {
		_ = srv.Serve(lis)
	}()

	// Use Stop() (not GracefulStop) so active streams are force-closed immediately.
	return port, func() { srv.Stop() }
}

func TestManager_ConnectAndReceiveStatus(t *testing.T) {
	handler := &mockHandler{}
	port, cleanup := startTestWorkerServer(t, "worker-1")
	defer cleanup()

	mgr := NewManager(handler)
	mgr.Start(t.Context())
	defer mgr.Stop()

	err := mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port),
	})
	require.NoError(t, err)

	// Should receive initial WorkerStatus.
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.statuses) >= 1
	}, 3*time.Second, 50*time.Millisecond)

	handler.mu.Lock()
	assert.Equal(t, "worker-1", handler.statuses[0].WorkerId)
	assert.True(t, handler.statuses[0].AcceptingWork)
	handler.mu.Unlock()

	assert.True(t, mgr.IsConnected("worker-1"))
}

func TestManager_SendAssignWork(t *testing.T) {
	handler := &mockHandler{}
	port, cleanup := startTestWorkerServer(t, "worker-1")
	defer cleanup()

	mgr := NewManager(handler)
	mgr.Start(t.Context())
	defer mgr.Stop()

	err := mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port),
	})
	require.NoError(t, err)

	// Wait for connection to be ready.
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.statuses) >= 1
	}, 3*time.Second, 50*time.Millisecond)

	// Send AssignWork.
	err = mgr.Send("worker-1", &workcoordpb.APIMessage{
		Msg: &workcoordpb.APIMessage_AssignWork{
			AssignWork: &workcoordpb.AssignWork{
				QueryId: "q1",
				LeafId:  "l1",
				WorkId:  "w1",
			},
		},
	})
	require.NoError(t, err)

	// Should receive WorkAccepted from the echo handler.
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.accepted) >= 1
	}, 3*time.Second, 50*time.Millisecond)

	handler.mu.Lock()
	assert.Equal(t, "w1", handler.accepted[0].WorkId)
	handler.mu.Unlock()
}

func TestManager_SendToUnknownWorker(t *testing.T) {
	mgr := NewManager(&mockHandler{})
	mgr.Start(t.Context())
	defer mgr.Stop()

	err := mgr.Send("nonexistent", &workcoordpb.APIMessage{
		Msg: &workcoordpb.APIMessage_Heartbeat{
			Heartbeat: &workcoordpb.Heartbeat{TimestampNs: 1},
		},
	})
	assert.Error(t, err)
}

func TestManager_DisconnectWorker(t *testing.T) {
	handler := &mockHandler{}
	port, cleanup := startTestWorkerServer(t, "worker-1")
	defer cleanup()

	mgr := NewManager(handler)
	mgr.Start(t.Context())
	defer mgr.Stop()

	err := mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return mgr.IsConnected("worker-1")
	}, 3*time.Second, 50*time.Millisecond)

	mgr.DisconnectWorker("worker-1")
	assert.False(t, mgr.IsConnected("worker-1"))
}

func TestManager_DuplicateConnect(t *testing.T) {
	handler := &mockHandler{}
	port, cleanup := startTestWorkerServer(t, "worker-1")
	defer cleanup()

	mgr := NewManager(handler)
	mgr.Start(t.Context())
	defer mgr.Stop()

	err := mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port),
	})
	require.NoError(t, err)

	// Second connect should be a no-op.
	err = mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port),
	})
	require.NoError(t, err)

	assert.Equal(t, 1, len(mgr.ConnectedWorkers()))
}

func TestManager_ConnectedWorkers(t *testing.T) {
	handler := &mockHandler{}
	port1, cleanup1 := startTestWorkerServer(t, "worker-1")
	defer cleanup1()
	port2, cleanup2 := startTestWorkerServer(t, "worker-2")
	defer cleanup2()

	mgr := NewManager(handler)
	mgr.Start(t.Context())
	defer mgr.Stop()

	require.NoError(t, mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port1),
	}))
	require.NoError(t, mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-2",
		Address:  "localhost:" + strconv.Itoa(port2),
	}))

	workers := mgr.ConnectedWorkers()
	assert.Len(t, workers, 2)
	assert.Contains(t, workers, "worker-1")
	assert.Contains(t, workers, "worker-2")
}

func TestManager_WorkerServerShutdown_NotifiesDisconnect(t *testing.T) {
	handler := &mockHandler{}
	port, cleanup := startTestWorkerServer(t, "worker-1")

	mgr := NewManager(handler)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	mgr.Start(ctx)
	defer mgr.Stop()

	err := mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port),
	})
	require.NoError(t, err)

	// Wait for connection.
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.statuses) >= 1
	}, 3*time.Second, 50*time.Millisecond)

	// Kill the worker server.
	cleanup()

	// Should get disconnect notification.
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.disconnected) >= 1
	}, 10*time.Second, 100*time.Millisecond)

	handler.mu.Lock()
	assert.Contains(t, handler.disconnected, "worker-1")
	handler.mu.Unlock()
}

func TestManager_ReconnectsAfterDisconnect(t *testing.T) {
	handler := &mockHandler{}

	// Start the "new" server first so it's ready for reconnect.
	port2, cleanup2 := startTestWorkerServer(t, "worker-1")
	defer cleanup2()

	// Start the "initial" server.
	port, cleanup := startTestWorkerServer(t, "worker-1")

	mgr := NewManager(handler)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	mgr.Start(ctx)
	defer mgr.Stop()

	err := mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port),
	})
	require.NoError(t, err)

	// Wait for initial connection.
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.statuses) >= 1
	}, 3*time.Second, 50*time.Millisecond)

	// Update the known endpoint to the new port BEFORE killing the old server.
	// This simulates service discovery updating the address. The reconnect loop
	// re-reads knownEndpoints on each attempt, so it will use the new address.
	mgr.mu.Lock()
	mgr.knownEndpoints["worker-1"] = WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port2),
	}
	mgr.mu.Unlock()

	// Kill the original server.
	cleanup()

	// Wait for disconnect.
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.disconnected) >= 1
	}, 10*time.Second, 100*time.Millisecond)

	// Should eventually reconnect to the new server and receive a new WorkerStatus.
	initialStatuses := func() int {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.statuses)
	}()

	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.statuses) > initialStatuses
	}, 15*time.Second, 100*time.Millisecond)

	assert.True(t, mgr.IsConnected("worker-1"))
}

func TestManager_ConnectWorkerBeforeStart(t *testing.T) {
	mgr := NewManager(&mockHandler{})
	// ConnectWorker should return error if Start() was not called.
	err := mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:1234",
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not started")
}

func TestManager_DisconnectPreventsReconnect(t *testing.T) {
	handler := &mockHandler{}
	port, cleanup := startTestWorkerServer(t, "worker-1")
	defer cleanup()

	mgr := NewManager(handler)
	mgr.Start(t.Context())
	defer mgr.Stop()

	err := mgr.ConnectWorker(WorkerEndpoint{
		WorkerID: "worker-1",
		Address:  "localhost:" + strconv.Itoa(port),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return mgr.IsConnected("worker-1")
	}, 3*time.Second, 50*time.Millisecond)

	// Explicit disconnect removes from known endpoints.
	mgr.DisconnectWorker("worker-1")
	assert.False(t, mgr.IsConnected("worker-1"))

	// Verify no reconnect attempt after a delay.
	time.Sleep(1 * time.Second)
	assert.False(t, mgr.IsConnected("worker-1"))
}
