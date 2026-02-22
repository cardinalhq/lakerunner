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
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/queryapi/streammanager"
)

// fakeDiscovery implements WorkerDiscovery for testing.
type fakeDiscovery struct {
	mu      sync.Mutex
	workers []Worker
}

func (f *fakeDiscovery) Start(context.Context) error { return nil }
func (f *fakeDiscovery) Stop() error                 { return nil }
func (f *fakeDiscovery) GetWorkersForSegments(_ uuid.UUID, _ []int64) ([]SegmentWorkerMapping, error) {
	return nil, nil
}
func (f *fakeDiscovery) GetAllWorkers() ([]Worker, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Worker, len(f.workers))
	copy(out, f.workers)
	return out, nil
}
func (f *fakeDiscovery) setWorkers(w []Worker) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.workers = w
}

// fakeStreamConnector tracks ConnectWorker/DisconnectWorker calls.
type fakeStreamConnector struct {
	mu           sync.Mutex
	connected    map[string]streammanager.WorkerEndpoint
	disconnected []string
}

func newFakeStreamConnector() *fakeStreamConnector {
	return &fakeStreamConnector{
		connected: make(map[string]streammanager.WorkerEndpoint),
	}
}

func (f *fakeStreamConnector) ConnectWorker(ep streammanager.WorkerEndpoint) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.connected[ep.WorkerID] = ep
	return nil
}

func (f *fakeStreamConnector) DisconnectWorker(workerID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.connected, workerID)
	f.disconnected = append(f.disconnected, workerID)
}

func (f *fakeStreamConnector) connectedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := make([]string, 0, len(f.connected))
	for id := range f.connected {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func TestDiscoveryBridgeSync(t *testing.T) {
	disc := &fakeDiscovery{
		workers: []Worker{
			{IP: "10.0.0.1", Port: 8081},
			{IP: "10.0.0.2", Port: 8081},
		},
	}

	fsc := newFakeStreamConnector()

	bridge := NewDiscoveryBridge(disc, fsc, 8082)

	// First sync should connect both workers.
	bridge.sync()

	ids := fsc.connectedIDs()
	require.Len(t, ids, 2)
	assert.Equal(t, []string{"10.0.0.1:8081", "10.0.0.2:8081"}, ids)
	assert.Equal(t, "10.0.0.1:8082", fsc.connected["10.0.0.1:8081"].Address)
	assert.Equal(t, "10.0.0.2:8082", fsc.connected["10.0.0.2:8081"].Address)

	// Remove one worker, add another.
	disc.setWorkers([]Worker{
		{IP: "10.0.0.2", Port: 8081},
		{IP: "10.0.0.3", Port: 8081},
	})
	bridge.sync()

	ids = fsc.connectedIDs()
	require.Len(t, ids, 2)
	assert.Equal(t, []string{"10.0.0.2:8081", "10.0.0.3:8081"}, ids)
	assert.Contains(t, fsc.disconnected, "10.0.0.1:8081")
}

func TestDiscoveryBridgeNoChange(t *testing.T) {
	disc := &fakeDiscovery{
		workers: []Worker{{IP: "10.0.0.1", Port: 8081}},
	}

	fsc := newFakeStreamConnector()
	bridge := NewDiscoveryBridge(disc, fsc, 8082)

	bridge.sync()
	require.Len(t, fsc.connectedIDs(), 1)

	// Sync again with same workers should not re-connect.
	bridge.sync()
	require.Len(t, fsc.connectedIDs(), 1)
	assert.Empty(t, fsc.disconnected)
}

func TestDiscoveryBridgeStartStop(t *testing.T) {
	disc := &fakeDiscovery{
		workers: []Worker{{IP: "10.0.0.1", Port: 8081}},
	}

	fsc := newFakeStreamConnector()
	bridge := NewDiscoveryBridge(disc, fsc, 8082)
	bridge.pollInterval = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		bridge.Start(ctx)
		close(done)
	}()

	// Let it run a couple poll cycles.
	time.Sleep(200 * time.Millisecond)
	bridge.Stop()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("bridge.Start did not return after Stop")
	}

	// Should have connected the worker.
	require.Len(t, fsc.connectedIDs(), 1)
}
