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
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/cardinalhq/lakerunner/core/workcoordpb"
)

const (
	HeartbeatInterval       = 1 * time.Second
	HeartbeatFailureTimeout = 3 * HeartbeatInterval
	SendBufferSize          = 256
	ReconnectBaseDelay      = 500 * time.Millisecond
	ReconnectMaxDelay       = 30 * time.Second
)

// WorkerEndpoint describes a worker's network address.
type WorkerEndpoint struct {
	WorkerID string
	Address  string // host:port for gRPC
}

// MessageHandler handles incoming WorkerMessages from streams.
type MessageHandler interface {
	OnWorkerStatus(workerID string, msg *workcoordpb.WorkerStatus)
	OnWorkAccepted(workerID string, msg *workcoordpb.WorkAccepted)
	OnWorkRejected(workerID string, msg *workcoordpb.WorkRejected)
	OnWorkReady(workerID string, msg *workcoordpb.WorkReady)
	OnWorkFailed(workerID string, msg *workcoordpb.WorkFailed)
	OnWorkerDisconnected(workerID string)
}

// workerConn represents a single persistent connection to a worker.
type workerConn struct {
	workerID string
	address  string

	mu     sync.Mutex
	conn   *grpc.ClientConn
	stream workcoordpb.WorkControlService_ControlStreamClient
	sendCh chan *workcoordpb.APIMessage
	cancel context.CancelFunc
	closed bool
}

// Manager maintains gRPC control stream connections to all known workers.
type Manager struct {
	handler MessageHandler

	mu             sync.RWMutex
	workers        map[string]*workerConn    // workerID → conn (currently connected)
	knownEndpoints map[string]WorkerEndpoint // workerID → endpoint (for reconnect)
	ctx            context.Context
	cancel         context.CancelFunc
}

// NewManager creates a new stream manager.
func NewManager(handler MessageHandler) *Manager {
	return &Manager{
		handler:        handler,
		workers:        make(map[string]*workerConn),
		knownEndpoints: make(map[string]WorkerEndpoint),
	}
}

// Start begins the manager's lifecycle.
func (m *Manager) Start(ctx context.Context) {
	m.ctx, m.cancel = context.WithCancel(ctx)
}

// Stop gracefully closes all worker connections and prevents reconnect.
func (m *Manager) Stop() {
	if m.cancel != nil {
		m.cancel()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	clear(m.knownEndpoints)
	for _, wc := range m.workers {
		wc.close()
	}
}

// ConnectWorker establishes a control stream to a worker.
// If already connected, this is a no-op. The endpoint is remembered for reconnect.
// Start() must be called before ConnectWorker.
func (m *Manager) ConnectWorker(endpoint WorkerEndpoint) error {
	if m.ctx == nil {
		return fmt.Errorf("manager not started: call Start() before ConnectWorker")
	}
	m.mu.Lock()
	m.knownEndpoints[endpoint.WorkerID] = endpoint
	if _, exists := m.workers[endpoint.WorkerID]; exists {
		m.mu.Unlock()
		return nil
	}
	m.mu.Unlock()

	wc, err := m.dial(endpoint)
	if err != nil {
		return err
	}

	m.mu.Lock()
	// Double-check after acquiring write lock.
	if _, exists := m.workers[endpoint.WorkerID]; exists {
		m.mu.Unlock()
		wc.close()
		return nil
	}
	m.workers[endpoint.WorkerID] = wc
	m.mu.Unlock()

	go m.runWorkerStream(wc)
	return nil
}

// DisconnectWorker closes the connection to a specific worker and prevents reconnect.
func (m *Manager) DisconnectWorker(workerID string) {
	m.mu.Lock()
	delete(m.knownEndpoints, workerID)
	wc, exists := m.workers[workerID]
	if exists {
		delete(m.workers, workerID)
	}
	m.mu.Unlock()

	if exists {
		wc.close()
	}
}

// Send sends an APIMessage to a specific worker.
// Returns an error if the worker is not connected.
func (m *Manager) Send(workerID string, msg *workcoordpb.APIMessage) error {
	m.mu.RLock()
	wc, exists := m.workers[workerID]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("worker %s not connected", workerID)
	}

	wc.mu.Lock()
	defer wc.mu.Unlock()
	if wc.closed {
		return fmt.Errorf("worker %s connection closed", workerID)
	}

	select {
	case wc.sendCh <- msg:
		return nil
	default:
		return fmt.Errorf("worker %s send buffer full", workerID)
	}
}

// ConnectedWorkers returns the IDs of all connected workers.
func (m *Manager) ConnectedWorkers() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	ids := make([]string, 0, len(m.workers))
	for id := range m.workers {
		ids = append(ids, id)
	}
	return ids
}

// IsConnected returns true if the given worker has an active connection.
func (m *Manager) IsConnected(workerID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, exists := m.workers[workerID]
	return exists
}

func (m *Manager) dial(endpoint WorkerEndpoint) (*workerConn, error) {
	conn, err := grpc.NewClient(
		endpoint.Address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to dial worker %s at %s: %w", endpoint.WorkerID, endpoint.Address, err)
	}

	ctx, cancel := context.WithCancel(m.ctx)

	client := workcoordpb.NewWorkControlServiceClient(conn)
	stream, err := client.ControlStream(ctx)
	if err != nil {
		cancel()
		_ = conn.Close()
		return nil, fmt.Errorf("failed to open control stream to worker %s: %w", endpoint.WorkerID, err)
	}

	wc := &workerConn{
		workerID: endpoint.WorkerID,
		address:  endpoint.Address,
		conn:     conn,
		stream:   stream,
		sendCh:   make(chan *workcoordpb.APIMessage, SendBufferSize),
		cancel:   cancel,
	}

	// Start writer goroutine. On send failure, cancel the connection context
	// so the recv loop exits and reconnect can trigger.
	go func() {
		for msg := range wc.sendCh {
			if err := stream.Send(msg); err != nil {
				slog.Warn("Stream send failed, tearing down connection",
					slog.String("worker_id", wc.workerID), slog.Any("error", err))
				wc.cancel()
				return
			}
		}
	}()

	// Start heartbeat sender.
	go func() {
		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				wc.mu.Lock()
				if wc.closed {
					wc.mu.Unlock()
					return
				}
				select {
				case wc.sendCh <- &workcoordpb.APIMessage{
					Msg: &workcoordpb.APIMessage_Heartbeat{
						Heartbeat: &workcoordpb.Heartbeat{
							TimestampNs: time.Now().UnixNano(),
						},
					},
				}:
				default:
				}
				wc.mu.Unlock()
			}
		}
	}()

	return wc, nil
}

func (m *Manager) runWorkerStream(wc *workerConn) {
	defer func() {
		workerID := wc.workerID
		m.mu.Lock()
		delete(m.workers, workerID)
		endpoint, shouldReconnect := m.knownEndpoints[workerID]
		m.mu.Unlock()
		wc.close()

		if m.handler != nil {
			m.handler.OnWorkerDisconnected(workerID)
		}
		slog.Info("Worker stream closed", slog.String("worker_id", workerID))

		if shouldReconnect && m.ctx.Err() == nil {
			go m.reconnectWorker(endpoint)
		}
	}()

	slog.Info("Worker stream opened", slog.String("worker_id", wc.workerID))

	var lastHeartbeat = time.Now()
	var hbMu sync.Mutex

	// Heartbeat monitor goroutine.
	go func() {
		ticker := time.NewTicker(HeartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-m.ctx.Done():
				return
			case <-ticker.C:
				hbMu.Lock()
				elapsed := time.Since(lastHeartbeat)
				hbMu.Unlock()
				if elapsed > HeartbeatFailureTimeout {
					slog.Warn("Worker heartbeat timeout",
						slog.String("worker_id", wc.workerID),
						slog.Duration("elapsed", elapsed))
					wc.cancel()
					return
				}
			}
		}
	}()

	for {
		msg, err := wc.stream.Recv()
		if err != nil {
			if err != io.EOF && m.ctx.Err() == nil {
				slog.Error("Worker stream recv error",
					slog.String("worker_id", wc.workerID),
					slog.Any("error", err))
			}
			return
		}

		if m.handler == nil {
			continue
		}

		switch v := msg.Msg.(type) {
		case *workcoordpb.WorkerMessage_Heartbeat:
			hbMu.Lock()
			lastHeartbeat = time.Now()
			hbMu.Unlock()

		case *workcoordpb.WorkerMessage_WorkerStatus:
			m.handler.OnWorkerStatus(wc.workerID, v.WorkerStatus)

		case *workcoordpb.WorkerMessage_WorkAccepted:
			m.handler.OnWorkAccepted(wc.workerID, v.WorkAccepted)

		case *workcoordpb.WorkerMessage_WorkRejected:
			m.handler.OnWorkRejected(wc.workerID, v.WorkRejected)

		case *workcoordpb.WorkerMessage_WorkReady:
			m.handler.OnWorkReady(wc.workerID, v.WorkReady)

		case *workcoordpb.WorkerMessage_WorkFailed:
			m.handler.OnWorkFailed(wc.workerID, v.WorkFailed)
		}
	}
}

// reconnectWorker attempts to re-establish a connection to a worker with
// exponential backoff and jitter. The workerID is used to look up the
// current endpoint from knownEndpoints on each attempt (in case the address
// has been updated by service discovery).
func (m *Manager) reconnectWorker(initialEndpoint WorkerEndpoint) {
	workerID := initialEndpoint.WorkerID
	delay := ReconnectBaseDelay
	for {
		// Re-read endpoint from knownEndpoints (address may have changed).
		m.mu.RLock()
		endpoint, stillKnown := m.knownEndpoints[workerID]
		_, alreadyConnected := m.workers[workerID]
		m.mu.RUnlock()

		if !stillKnown || alreadyConnected || m.ctx.Err() != nil {
			return
		}

		slog.Info("Attempting reconnect to worker",
			slog.String("worker_id", workerID),
			slog.Duration("delay", delay))

		select {
		case <-time.After(delay):
		case <-m.ctx.Done():
			return
		}

		// Add jitter: ±25% of the delay.
		jitter := time.Duration(float64(delay) * (0.5*rand.Float64() - 0.25))
		delay = delay*2 + jitter
		if delay > ReconnectMaxDelay {
			delay = ReconnectMaxDelay
		}

		wc, err := m.dial(endpoint)
		if err != nil {
			slog.Warn("Reconnect failed",
				slog.String("worker_id", workerID),
				slog.Any("error", err))
			continue
		}

		m.mu.Lock()
		// Check again under write lock.
		if _, stillKnown := m.knownEndpoints[workerID]; !stillKnown {
			m.mu.Unlock()
			wc.close()
			return
		}
		if _, exists := m.workers[workerID]; exists {
			m.mu.Unlock()
			wc.close()
			return
		}
		m.workers[workerID] = wc
		m.mu.Unlock()

		slog.Info("Reconnected to worker", slog.String("worker_id", endpoint.WorkerID))
		go m.runWorkerStream(wc)
		return
	}
}

func (wc *workerConn) close() {
	wc.mu.Lock()
	defer wc.mu.Unlock()
	if wc.closed {
		return
	}
	wc.closed = true
	close(wc.sendCh)
	wc.cancel()
	if wc.conn != nil {
		_ = wc.conn.Close()
	}
}
