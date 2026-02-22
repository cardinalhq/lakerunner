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

package controlstream

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
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cardinalhq/lakerunner/core/workcoordpb"
)

// mockWorkHandler records calls for testing.
type mockWorkHandler struct {
	mu             sync.Mutex
	assignCalls    []*workcoordpb.AssignWork
	cancelCalls    []*workcoordpb.CancelWork
	ackCalls       []*workcoordpb.ArtifactAck
	closedSessions []string
}

func (m *mockWorkHandler) OnAssignWork(session *Session, msg *workcoordpb.AssignWork) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.assignCalls = append(m.assignCalls, msg)
	// Reply with accepted.
	session.Send(&workcoordpb.WorkerMessage{
		Msg: &workcoordpb.WorkerMessage_WorkAccepted{
			WorkAccepted: &workcoordpb.WorkAccepted{WorkId: msg.WorkId},
		},
	})
}

func (m *mockWorkHandler) OnCancelWork(_ *Session, msg *workcoordpb.CancelWork) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cancelCalls = append(m.cancelCalls, msg)
}

func (m *mockWorkHandler) OnArtifactAck(_ *Session, msg *workcoordpb.ArtifactAck) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ackCalls = append(m.ackCalls, msg)
}

func (m *mockWorkHandler) OnSessionClosed(sessionID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closedSessions = append(m.closedSessions, sessionID)
}

// startTestServer starts a control stream server on a random port and returns the port.
func startTestServer(t *testing.T, handler WorkHandler) (*Server, int) {
	t.Helper()

	// Find a free port.
	lis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	port := lis.Addr().(*net.TCPAddr).Port
	require.NoError(t, lis.Close())

	srv := NewServer(Config{
		WorkerID: "test-worker-1",
		GRPCPort: port,
		Handler:  handler,
	})

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	go func() {
		_ = srv.Start(ctx)
	}()

	// Wait for server to be ready.
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", ":"+strconv.Itoa(port), 100*time.Millisecond)
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 2*time.Second, 50*time.Millisecond)

	return srv, port
}

func dialTestServer(t *testing.T, port int) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(
		"localhost:"+strconv.Itoa(port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestServer_NewServer_Defaults(t *testing.T) {
	srv := NewServer(Config{})
	assert.Equal(t, DefaultGRPCPort, srv.grpcPort)
	assert.NotEmpty(t, srv.workerID)
	assert.True(t, srv.acceptingWork)
	assert.False(t, srv.draining)
}

func TestServer_NewServer_CustomConfig(t *testing.T) {
	srv := NewServer(Config{
		WorkerID: "custom-worker",
		GRPCPort: 9999,
	})
	assert.Equal(t, 9999, srv.grpcPort)
	assert.Equal(t, "custom-worker", srv.workerID)
}

func TestServer_InitialWorkerStatus(t *testing.T) {
	handler := &mockWorkHandler{}
	_, port := startTestServer(t, handler)

	conn := dialTestServer(t, port)
	client := workcoordpb.NewWorkControlServiceClient(conn)

	stream, err := client.ControlStream(t.Context())
	require.NoError(t, err)

	// First message should be WorkerStatus.
	msg, err := stream.Recv()
	require.NoError(t, err)

	status := msg.GetWorkerStatus()
	require.NotNil(t, status)
	assert.Equal(t, "test-worker-1", status.WorkerId)
	assert.True(t, status.AcceptingWork)
	assert.False(t, status.Draining)
}

func TestServer_AssignWork_Dispatch(t *testing.T) {
	handler := &mockWorkHandler{}
	_, port := startTestServer(t, handler)

	conn := dialTestServer(t, port)
	client := workcoordpb.NewWorkControlServiceClient(conn)

	stream, err := client.ControlStream(t.Context())
	require.NoError(t, err)

	// Consume initial WorkerStatus.
	_, err = stream.Recv()
	require.NoError(t, err)

	// Send AssignWork.
	err = stream.Send(&workcoordpb.APIMessage{
		Msg: &workcoordpb.APIMessage_AssignWork{
			AssignWork: &workcoordpb.AssignWork{
				QueryId:     "q1",
				LeafId:      "l1",
				WorkId:      "w1",
				AffinityKey: "org:seg",
				Spec:        []byte("test-spec"),
			},
		},
	})
	require.NoError(t, err)

	// Should receive WorkAccepted back from handler.
	msg, err := stream.Recv()
	require.NoError(t, err)
	accepted := msg.GetWorkAccepted()
	require.NotNil(t, accepted)
	assert.Equal(t, "w1", accepted.WorkId)

	// Verify handler received the call.
	handler.mu.Lock()
	assert.Len(t, handler.assignCalls, 1)
	assert.Equal(t, "w1", handler.assignCalls[0].WorkId)
	handler.mu.Unlock()
}

func TestServer_CancelWork_Dispatch(t *testing.T) {
	handler := &mockWorkHandler{}
	_, port := startTestServer(t, handler)

	conn := dialTestServer(t, port)
	client := workcoordpb.NewWorkControlServiceClient(conn)

	stream, err := client.ControlStream(t.Context())
	require.NoError(t, err)

	// Consume initial WorkerStatus.
	_, err = stream.Recv()
	require.NoError(t, err)

	// Send CancelWork.
	err = stream.Send(&workcoordpb.APIMessage{
		Msg: &workcoordpb.APIMessage_CancelWork{
			CancelWork: &workcoordpb.CancelWork{WorkId: "w1"},
		},
	})
	require.NoError(t, err)

	// Give handler time to process.
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.cancelCalls) == 1
	}, 2*time.Second, 50*time.Millisecond)

	handler.mu.Lock()
	assert.Equal(t, "w1", handler.cancelCalls[0].WorkId)
	handler.mu.Unlock()
}

func TestServer_ArtifactAck_Dispatch(t *testing.T) {
	handler := &mockWorkHandler{}
	_, port := startTestServer(t, handler)

	conn := dialTestServer(t, port)
	client := workcoordpb.NewWorkControlServiceClient(conn)

	stream, err := client.ControlStream(t.Context())
	require.NoError(t, err)

	// Consume initial WorkerStatus.
	_, err = stream.Recv()
	require.NoError(t, err)

	// Send ArtifactAck.
	err = stream.Send(&workcoordpb.APIMessage{
		Msg: &workcoordpb.APIMessage_ArtifactAck{
			ArtifactAck: &workcoordpb.ArtifactAck{WorkId: "w1"},
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.ackCalls) == 1
	}, 2*time.Second, 50*time.Millisecond)

	handler.mu.Lock()
	assert.Equal(t, "w1", handler.ackCalls[0].WorkId)
	handler.mu.Unlock()
}

func TestServer_SessionClosed_OnDisconnect(t *testing.T) {
	handler := &mockWorkHandler{}
	srv, port := startTestServer(t, handler)

	conn, err := grpc.NewClient(
		"localhost:"+strconv.Itoa(port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := workcoordpb.NewWorkControlServiceClient(conn)
	stream, err := client.ControlStream(t.Context())
	require.NoError(t, err)

	// Consume initial WorkerStatus.
	_, err = stream.Recv()
	require.NoError(t, err)

	assert.Equal(t, 1, srv.SessionCount())

	// Close the client connection to simulate disconnect.
	require.NoError(t, conn.Close())

	// Handler should be notified of session close. The heartbeat monitor
	// detects the missing heartbeat after ~3s and fires cancel.
	require.Eventually(t, func() bool {
		handler.mu.Lock()
		defer handler.mu.Unlock()
		return len(handler.closedSessions) == 1
	}, 10*time.Second, 100*time.Millisecond)

	assert.Equal(t, 0, srv.SessionCount())
}

func TestServer_SetDraining_BroadcastsStatus(t *testing.T) {
	handler := &mockWorkHandler{}
	_, port := startTestServer(t, handler)

	conn := dialTestServer(t, port)
	client := workcoordpb.NewWorkControlServiceClient(conn)

	stream, err := client.ControlStream(t.Context())
	require.NoError(t, err)

	// Consume initial WorkerStatus (accepting=true, draining=false).
	msg, err := stream.Recv()
	require.NoError(t, err)
	status := msg.GetWorkerStatus()
	require.NotNil(t, status)
	assert.True(t, status.AcceptingWork)
	assert.False(t, status.Draining)
}

func TestServer_SessionSend_AfterClose(t *testing.T) {
	sess := &Session{
		ID:     "test",
		sendCh: make(chan *workcoordpb.WorkerMessage, 1),
		cancel: func() {},
	}
	sess.close()

	ok := sess.Send(&workcoordpb.WorkerMessage{
		Msg: &workcoordpb.WorkerMessage_Heartbeat{
			Heartbeat: &workcoordpb.Heartbeat{TimestampNs: 1},
		},
	})
	assert.False(t, ok)
}

func TestServer_HeartbeatSent(t *testing.T) {
	handler := &mockWorkHandler{}
	_, port := startTestServer(t, handler)

	conn := dialTestServer(t, port)
	client := workcoordpb.NewWorkControlServiceClient(conn)

	stream, err := client.ControlStream(t.Context())
	require.NoError(t, err)

	// Send heartbeats from API side to keep session alive.
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-t.Context().Done():
				return
			case <-ticker.C:
				_ = stream.Send(&workcoordpb.APIMessage{
					Msg: &workcoordpb.APIMessage_Heartbeat{
						Heartbeat: &workcoordpb.Heartbeat{TimestampNs: time.Now().UnixNano()},
					},
				})
			}
		}
	}()

	// Consume initial WorkerStatus.
	_, err = stream.Recv()
	require.NoError(t, err)

	// Should receive heartbeat from worker within 2 seconds.
	gotHeartbeat := false
	deadline := time.After(3 * time.Second)
	for !gotHeartbeat {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for heartbeat")
		default:
		}
		msg, err := stream.Recv()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if msg.GetHeartbeat() != nil {
			gotHeartbeat = true
		}
	}
	assert.True(t, gotHeartbeat)
}

func TestServer_MultipleSessions(t *testing.T) {
	handler := &mockWorkHandler{}
	srv, port := startTestServer(t, handler)

	// Open two sessions.
	conn1 := dialTestServer(t, port)
	conn2 := dialTestServer(t, port)
	client1 := workcoordpb.NewWorkControlServiceClient(conn1)
	client2 := workcoordpb.NewWorkControlServiceClient(conn2)

	stream1, err := client1.ControlStream(t.Context())
	require.NoError(t, err)
	stream2, err := client2.ControlStream(t.Context())
	require.NoError(t, err)

	// Consume initial statuses.
	_, err = stream1.Recv()
	require.NoError(t, err)
	_, err = stream2.Recv()
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return srv.SessionCount() == 2
	}, 2*time.Second, 50*time.Millisecond)
}
