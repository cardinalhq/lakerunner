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
	"log/slog"
	"sync"
	"time"
)

const (
	HighWatermark    = 0.80
	LowWatermark     = 0.70
	CheckInterval    = 30 * time.Second
	MinArtifactFloor = 512 * 1024 * 1024 // 512 MB reserved for artifacts
)

// DiskUsageFunc returns disk usage for a path.
type DiskUsageFunc func(path string) (usedBytes, totalBytes uint64, err error)

// Evictor can evict data to free disk space.
// EvictBytes attempts to free at least the given number of bytes.
// Returns the number of bytes actually freed.
type Evictor interface {
	EvictBytes(bytes int64) int64
}

// Manager monitors shared disk usage and triggers eviction across multiple
// evictors (parquet cache + artifact spool) in priority order.
type Manager struct {
	diskPath     string
	getDiskUsage DiskUsageFunc
	evictors     []Evictor // in eviction priority order

	mu     sync.Mutex
	stopCh chan struct{}
	done   chan struct{}
}

// NewManager creates a disk budget manager.
// evictors are tried in order (first = lowest cost to evict).
func NewManager(diskPath string, getDiskUsage DiskUsageFunc, evictors ...Evictor) *Manager {
	return &Manager{
		diskPath:     diskPath,
		getDiskUsage: getDiskUsage,
		evictors:     evictors,
		stopCh:       make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// Start begins periodic disk usage monitoring.
func (m *Manager) Start() {
	go m.monitorLoop()
}

// Stop halts monitoring.
func (m *Manager) Stop() {
	close(m.stopCh)
	<-m.done
}

// CheckAndEvict checks disk usage and triggers eviction if above the high watermark.
func (m *Manager) CheckAndEvict() {
	m.mu.Lock()
	defer m.mu.Unlock()

	used, total, err := m.getDiskUsage(m.diskPath)
	if err != nil {
		slog.Warn("Disk budget check failed", slog.Any("error", err))
		return
	}

	if total == 0 {
		return
	}

	utilization := float64(used) / float64(total)
	if utilization < HighWatermark {
		return
	}

	targetUsed := uint64(float64(total) * LowWatermark)
	bytesToFree := int64(used - targetUsed)
	if bytesToFree <= 0 {
		return
	}

	slog.Info("Disk pressure detected, starting eviction",
		slog.Float64("utilization", utilization),
		slog.Int64("bytes_to_free", bytesToFree))

	var totalFreed int64
	for _, evictor := range m.evictors {
		remaining := bytesToFree - totalFreed
		if remaining <= 0 {
			break
		}
		freed := evictor.EvictBytes(remaining)
		totalFreed += freed
	}

	slog.Info("Eviction complete", slog.Int64("bytes_freed", totalFreed))
}

func (m *Manager) monitorLoop() {
	defer close(m.done)
	ticker := time.NewTicker(CheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.CheckAndEvict()
		}
	}
}
