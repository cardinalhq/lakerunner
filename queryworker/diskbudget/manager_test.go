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

package diskbudget

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

// mockEvictor tracks eviction requests.
type mockEvictor struct {
	totalEvicted atomic.Int64
	maxEvict     int64 // max bytes this evictor can free per call; 0 = unlimited
}

func (m *mockEvictor) EvictBytes(bytes int64) int64 {
	evict := bytes
	if m.maxEvict > 0 && evict > m.maxEvict {
		evict = m.maxEvict
	}
	m.totalEvicted.Add(evict)
	return evict
}

func fixedDiskUsage(used, total uint64) DiskUsageFunc {
	return func(_ string) (uint64, uint64, error) {
		return used, total, nil
	}
}

func TestCheckAndEvict_NoPressure(t *testing.T) {
	ev := &mockEvictor{}
	// 50% utilization, below 80% watermark.
	mgr := NewManager("/tmp", fixedDiskUsage(50, 100), ev)
	mgr.CheckAndEvict()
	assert.Equal(t, int64(0), ev.totalEvicted.Load())
}

func TestCheckAndEvict_AboveHighWatermark(t *testing.T) {
	ev := &mockEvictor{}
	// 90% utilization, need to get to 70%.
	// Used=90, Total=100, target=70, need to free 20.
	mgr := NewManager("/tmp", fixedDiskUsage(90, 100), ev)
	mgr.CheckAndEvict()
	assert.Equal(t, int64(20), ev.totalEvicted.Load())
}

func TestCheckAndEvict_MultipleEvictors(t *testing.T) {
	ev1 := &mockEvictor{maxEvict: 5}
	ev2 := &mockEvictor{}
	// Need to free 20, ev1 can only free 5, ev2 handles the rest.
	mgr := NewManager("/tmp", fixedDiskUsage(90, 100), ev1, ev2)
	mgr.CheckAndEvict()
	assert.Equal(t, int64(5), ev1.totalEvicted.Load())
	assert.Equal(t, int64(15), ev2.totalEvicted.Load())
}

func TestCheckAndEvict_AtExactWatermark(t *testing.T) {
	ev := &mockEvictor{}
	// Exactly at 80% triggers eviction (uses < not <=).
	mgr := NewManager("/tmp", fixedDiskUsage(80, 100), ev)
	mgr.CheckAndEvict()
	assert.Equal(t, int64(10), ev.totalEvicted.Load())
}

func TestCheckAndEvict_JustAboveWatermark(t *testing.T) {
	ev := &mockEvictor{}
	// 81% utilization, target 70%, need to free 11.
	mgr := NewManager("/tmp", fixedDiskUsage(81, 100), ev)
	mgr.CheckAndEvict()
	assert.Equal(t, int64(11), ev.totalEvicted.Load())
}

func TestStartStop(t *testing.T) {
	mgr := NewManager("/tmp", fixedDiskUsage(50, 100))
	mgr.Start()
	mgr.Stop()
}
