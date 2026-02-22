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

package queryapi

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/cardinalhq/lakerunner/queryapi/streammanager"
)

const (
	DefaultDiscoveryPollInterval = 5 * time.Second
)

// StreamConnector abstracts the connect/disconnect operations on a stream manager.
type StreamConnector interface {
	ConnectWorker(endpoint streammanager.WorkerEndpoint) error
	DisconnectWorker(workerID string)
}

// DiscoveryBridge periodically polls WorkerDiscovery and syncs the
// discovered worker set into a StreamConnector, connecting new workers
// and disconnecting removed ones.
type DiscoveryBridge struct {
	discovery    WorkerDiscovery
	streams      StreamConnector
	controlPort  int
	pollInterval time.Duration

	mu     sync.Mutex
	known  map[string]streammanager.WorkerEndpoint // workerID → endpoint
	cancel context.CancelFunc
}

// NewDiscoveryBridge creates a bridge between worker discovery and stream management.
// controlPort is the gRPC control stream port on workers (default 8082).
func NewDiscoveryBridge(discovery WorkerDiscovery, streams StreamConnector, controlPort int) *DiscoveryBridge {
	return &DiscoveryBridge{
		discovery:    discovery,
		streams:      streams,
		controlPort:  controlPort,
		pollInterval: DefaultDiscoveryPollInterval,
		known:        make(map[string]streammanager.WorkerEndpoint),
	}
}

// Start begins periodic polling. It blocks until the context is canceled.
func (b *DiscoveryBridge) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	b.mu.Lock()
	b.cancel = cancel
	b.mu.Unlock()

	// Initial sync.
	b.sync()

	ticker := time.NewTicker(b.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.sync()
		}
	}
}

// Stop stops the bridge's polling loop.
func (b *DiscoveryBridge) Stop() {
	b.mu.Lock()
	cancel := b.cancel
	b.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (b *DiscoveryBridge) sync() {
	workers, err := b.discovery.GetAllWorkers()
	if err != nil {
		slog.Error("Discovery bridge: failed to get workers", slog.Any("error", err))
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Build current set from discovery.
	current := make(map[string]streammanager.WorkerEndpoint, len(workers))
	for _, w := range workers {
		workerID := fmt.Sprintf("%s:%d", w.IP, w.Port)
		endpoint := streammanager.WorkerEndpoint{
			WorkerID: workerID,
			Address:  fmt.Sprintf("%s:%d", w.IP, b.controlPort),
		}
		current[workerID] = endpoint
	}

	// Connect new workers.
	for id, ep := range current {
		if _, exists := b.known[id]; !exists {
			slog.Info("Discovery bridge: connecting worker",
				slog.String("worker_id", id),
				slog.String("address", ep.Address))
			if err := b.streams.ConnectWorker(ep); err != nil {
				slog.Error("Discovery bridge: failed to connect worker",
					slog.String("worker_id", id),
					slog.Any("error", err))
			}
		}
	}

	// Disconnect removed workers.
	for id := range b.known {
		if _, exists := current[id]; !exists {
			slog.Info("Discovery bridge: disconnecting worker",
				slog.String("worker_id", id))
			b.streams.DisconnectWorker(id)
		}
	}

	b.known = current
}
