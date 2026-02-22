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
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"

	"github.com/cardinalhq/lakerunner/core/workcoordpb"
)

const (
	DefaultGRPCPort         = 8082
	HeartbeatInterval       = 1 * time.Second
	HeartbeatFailureTimeout = 3 * HeartbeatInterval
	SendBufferSize          = 256
)

// WorkHandler processes work-related messages received on a control stream session.
type WorkHandler interface {
	OnAssignWork(session *Session, msg *workcoordpb.AssignWork)
	OnCancelWork(session *Session, msg *workcoordpb.CancelWork)
	OnArtifactAck(session *Session, msg *workcoordpb.ArtifactAck)
	OnSessionClosed(sessionID string)
}

// Session represents a single API→Worker control stream connection.
type Session struct {
	ID     string
	sendCh chan *workcoordpb.WorkerMessage
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
}

// Send enqueues a message to be sent on this session's stream.
// Returns false if the session is closed.
func (s *Session) Send(msg *workcoordpb.WorkerMessage) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	select {
	case s.sendCh <- msg:
		return true
	default:
		slog.Warn("Control stream send buffer full, dropping message",
			slog.String("session_id", s.ID))
		return false
	}
}

func (s *Session) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.closed {
		s.closed = true
		close(s.sendCh)
		s.cancel()
	}
}

// Server hosts the gRPC WorkControlService on the worker side.
type Server struct {
	workcoordpb.UnimplementedWorkControlServiceServer
	workerID   string
	grpcPort   int
	handler    WorkHandler
	grpcServer *grpc.Server

	sessions sync.Map // sessionID → *Session

	// Admission state — shared with WorkHandler.
	mu            sync.RWMutex
	acceptingWork bool
	draining      bool
}

// Config holds configuration for the control stream server.
type Config struct {
	WorkerID string
	GRPCPort int
	Handler  WorkHandler
}

// NewServer creates a new control stream server.
func NewServer(cfg Config) *Server {
	port := cfg.GRPCPort
	if port == 0 {
		port = DefaultGRPCPort
	}
	workerID := cfg.WorkerID
	if workerID == "" {
		workerID = uuid.New().String()
	}
	return &Server{
		workerID:      workerID,
		grpcPort:      port,
		handler:       cfg.Handler,
		acceptingWork: true,
	}
}

// WorkerID returns the server's worker identity.
func (s *Server) WorkerID() string {
	return s.workerID
}

// SetAcceptingWork updates the admission state and broadcasts to all sessions.
func (s *Server) SetAcceptingWork(accepting bool) {
	s.mu.Lock()
	s.acceptingWork = accepting
	s.mu.Unlock()
	s.broadcastStatus()
}

// SetDraining updates the drain state and broadcasts to all sessions.
func (s *Server) SetDraining(draining bool) {
	s.mu.Lock()
	s.draining = draining
	if draining {
		s.acceptingWork = false
	}
	s.mu.Unlock()
	s.broadcastStatus()
}

func (s *Server) broadcastStatus() {
	s.mu.RLock()
	status := &workcoordpb.WorkerMessage{
		Msg: &workcoordpb.WorkerMessage_WorkerStatus{
			WorkerStatus: &workcoordpb.WorkerStatus{
				WorkerId:      s.workerID,
				AcceptingWork: s.acceptingWork,
				Draining:      s.draining,
			},
		},
	}
	s.mu.RUnlock()

	s.sessions.Range(func(_, value any) bool {
		sess := value.(*Session)
		sess.Send(status)
		return true
	})
}

// Start starts the gRPC server and blocks until ctx is canceled.
func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(s.grpcPort))
	if err != nil {
		return fmt.Errorf("failed to listen on port %d: %w", s.grpcPort, err)
	}

	s.grpcServer = grpc.NewServer()
	workcoordpb.RegisterWorkControlServiceServer(s.grpcServer, s)

	go func() {
		<-ctx.Done()
		// Close all sessions first.
		s.sessions.Range(func(_, value any) bool {
			sess := value.(*Session)
			sess.close()
			return true
		})
		done := make(chan struct{})
		go func() {
			s.grpcServer.GracefulStop()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			slog.Warn("Control stream gRPC server graceful stop timed out, forcing stop")
			s.grpcServer.Stop()
		}
	}()

	slog.Info("Starting control stream gRPC server", slog.Int("port", s.grpcPort))
	if err := s.grpcServer.Serve(lis); err != nil {
		return fmt.Errorf("control stream gRPC server failed: %w", err)
	}
	return nil
}

// ControlStream implements the WorkControlService bidirectional stream RPC.
func (s *Server) ControlStream(stream workcoordpb.WorkControlService_ControlStreamServer) error {
	sessionID := uuid.New().String()
	ctx, cancel := context.WithCancel(stream.Context())

	sess := &Session{
		ID:     sessionID,
		sendCh: make(chan *workcoordpb.WorkerMessage, SendBufferSize),
		cancel: cancel,
	}
	s.sessions.Store(sessionID, sess)
	defer func() {
		sess.close()
		s.sessions.Delete(sessionID)
		if s.handler != nil {
			s.handler.OnSessionClosed(sessionID)
		}
		slog.Info("Control stream session closed", slog.String("session_id", sessionID))
	}()

	slog.Info("Control stream session opened", slog.String("session_id", sessionID))

	// Send initial worker status.
	s.mu.RLock()
	initStatus := &workcoordpb.WorkerMessage{
		Msg: &workcoordpb.WorkerMessage_WorkerStatus{
			WorkerStatus: &workcoordpb.WorkerStatus{
				WorkerId:      s.workerID,
				AcceptingWork: s.acceptingWork,
				Draining:      s.draining,
			},
		},
	}
	s.mu.RUnlock()
	sess.Send(initStatus)

	// Writer goroutine: drain sendCh into the gRPC stream.
	writerDone := make(chan error, 1)
	go func() {
		for msg := range sess.sendCh {
			if err := stream.Send(msg); err != nil {
				writerDone <- err
				return
			}
		}
		writerDone <- nil
	}()

	// Heartbeat sender goroutine.
	go func() {
		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				sess.Send(&workcoordpb.WorkerMessage{
					Msg: &workcoordpb.WorkerMessage_Heartbeat{
						Heartbeat: &workcoordpb.Heartbeat{
							TimestampNs: time.Now().UnixNano(),
						},
					},
				})
			}
		}
	}()

	// Heartbeat monitor: track the last heartbeat from API.
	lastHeartbeat := time.Now()
	var hbMu sync.Mutex
	go func() {
		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				hbMu.Lock()
				elapsed := time.Since(lastHeartbeat)
				hbMu.Unlock()
				if elapsed > HeartbeatFailureTimeout {
					slog.Warn("API heartbeat timeout, closing session",
						slog.String("session_id", sessionID),
						slog.Duration("elapsed", elapsed))
					cancel()
					return
				}
			}
		}
	}()

	// Reader loop: read APIMessages and dispatch.
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF || ctx.Err() != nil {
				break
			}
			slog.Error("Control stream recv error",
				slog.String("session_id", sessionID),
				slog.Any("error", err))
			break
		}

		switch m := msg.Msg.(type) {
		case *workcoordpb.APIMessage_Heartbeat:
			hbMu.Lock()
			lastHeartbeat = time.Now()
			hbMu.Unlock()

		case *workcoordpb.APIMessage_AssignWork:
			if s.handler != nil {
				s.handler.OnAssignWork(sess, m.AssignWork)
			}

		case *workcoordpb.APIMessage_CancelWork:
			if s.handler != nil {
				s.handler.OnCancelWork(sess, m.CancelWork)
			}

		case *workcoordpb.APIMessage_ArtifactAck:
			if s.handler != nil {
				s.handler.OnArtifactAck(sess, m.ArtifactAck)
			}
		}
	}

	// Wait for writer to finish.
	cancel()
	select {
	case <-writerDone:
	case <-time.After(5 * time.Second):
	}

	return nil
}

// SessionCount returns the number of active sessions.
func (s *Server) SessionCount() int {
	count := 0
	s.sessions.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
