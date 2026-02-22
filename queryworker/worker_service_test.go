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

package queryworker

import (
	"context"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cardinalhq/lakerunner/core/workcoordpb"
	"github.com/cardinalhq/lakerunner/queryworker/controlstream"
	"github.com/cardinalhq/lakerunner/queryworker/workmanager"
)

type drainTestExecutor struct {
	blockCh   chan struct{}
	canceled  atomic.Bool
	completed atomic.Bool
}

func (e *drainTestExecutor) Execute(ctx context.Context, _ string, _ []byte) (*workmanager.ArtifactResult, error) {
	select {
	case <-e.blockCh:
		e.completed.Store(true)
		return &workmanager.ArtifactResult{
			ArtifactURL: "http://localhost/test",
			RowCount:    1,
		}, nil
	case <-ctx.Done():
		e.canceled.Store(true)
		return nil, ctx.Err()
	}
}

func TestWorkerService_Run_DrainBeforeSessionClose(t *testing.T) {
	// Find free ports for gRPC and HTTP.
	grpcLis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	grpcPort := grpcLis.Addr().(*net.TCPAddr).Port
	require.NoError(t, grpcLis.Close())

	httpLis, err := net.Listen("tcp", ":0")
	require.NoError(t, err)
	httpAddr := httpLis.Addr().String()
	require.NoError(t, httpLis.Close())

	exec := &drainTestExecutor{blockCh: make(chan struct{})}
	workMgr := workmanager.NewManager(exec, 4, nil)

	csServer := controlstream.NewServer(controlstream.Config{
		WorkerID: "test-drain-worker",
		GRPCPort: grpcPort,
		Handler:  workMgr,
	})

	ws := &WorkerService{
		ControlStreamServer: csServer,
		WorkMgr:             workMgr,
		httpAddr:            httpAddr,
	}

	doneCtx, doneCancel := context.WithCancel(context.Background())
	defer doneCancel()

	runDone := make(chan error, 1)
	go func() {
		runDone <- ws.Run(doneCtx)
	}()

	// Wait for gRPC server to be ready.
	require.Eventually(t, func() bool {
		conn, dialErr := net.DialTimeout("tcp", ":"+strconv.Itoa(grpcPort), 100*time.Millisecond)
		if dialErr != nil {
			return false
		}
		_ = conn.Close()
		return true
	}, 5*time.Second, 50*time.Millisecond)

	// Connect gRPC client.
	conn, err := grpc.NewClient(
		"localhost:"+strconv.Itoa(grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	client := workcoordpb.NewWorkControlServiceClient(conn)
	stream, err := client.ControlStream(context.Background())
	require.NoError(t, err)

	// Send heartbeats to keep session alive.
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stream.Context().Done():
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
	msg, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, msg.GetWorkerStatus())

	// Assign work.
	err = stream.Send(&workcoordpb.APIMessage{
		Msg: &workcoordpb.APIMessage_AssignWork{
			AssignWork: &workcoordpb.AssignWork{
				WorkId: "drain-w1",
				Spec:   []byte("test"),
			},
		},
	})
	require.NoError(t, err)

	// Consume WorkAccepted (skip heartbeats).
	require.Eventually(t, func() bool {
		return workMgr.InFlightCount() == 1
	}, 2*time.Second, 50*time.Millisecond)

	// Trigger shutdown.
	doneCancel()

	// Give shutdown sequence time to set draining and start WaitForDrain.
	time.Sleep(500 * time.Millisecond)

	// Work should still be in-flight (not canceled by premature session close).
	assert.Equal(t, 1, workMgr.InFlightCount(), "work should still be in-flight during drain")
	assert.False(t, exec.canceled.Load(), "executor should not be canceled during drain")

	// Unblock the executor so work completes.
	close(exec.blockCh)

	// Run should complete after drain + server shutdown.
	select {
	case err := <-runDone:
		assert.NoError(t, err)
	case <-time.After(60 * time.Second):
		t.Fatal("Run did not return")
	}

	assert.True(t, exec.completed.Load(), "executor should have completed normally")
	assert.False(t, exec.canceled.Load(), "executor should not have been canceled")
}
