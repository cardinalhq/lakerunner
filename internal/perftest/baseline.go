// Copyright (C) 2025 CardinalHQ, Inc
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

package perftest

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"time"
)

// Metrics captures performance measurements for a test run
type Metrics struct {
	Duration       time.Duration
	LogsProcessed  int64
	BytesProcessed int64
	PeakMemoryMB   int64
	StartMemoryMB  int64
	AllocsMB       int64
	NumGC          uint32
}

// Report generates a human-readable summary
func (m *Metrics) Report(testName string) string {
	logsPerSec := float64(m.LogsProcessed) / m.Duration.Seconds()
	bytesPerSec := float64(m.BytesProcessed) / m.Duration.Seconds()
	logsPerSecPerCore := logsPerSec / float64(runtime.GOMAXPROCS(0))
	bytesPerSecPerCore := bytesPerSec / float64(runtime.GOMAXPROCS(0))

	return fmt.Sprintf(`%s Results:
  Duration:        %v
  Logs:            %d (%.0f/sec, %.0f/sec/core)
  Bytes:           %d (%.2f MB/s, %.2f MB/s/core)
  Memory:          %d MB start, %d MB peak, %d MB allocs
  GC:              %d collections
  Cores:           %d`,
		testName,
		m.Duration,
		m.LogsProcessed, logsPerSec, logsPerSecPerCore,
		m.BytesProcessed, bytesPerSec/1e6, bytesPerSecPerCore/1e6,
		m.StartMemoryMB, m.PeakMemoryMB, m.AllocsMB,
		m.NumGC,
		runtime.GOMAXPROCS(0))
}

// Timer helps measure performance of operations
type Timer struct {
	startTime      time.Time
	startMem       runtime.MemStats
	logsProcessed  atomic.Int64
	bytesProcessed atomic.Int64
	peakMemoryMB   atomic.Int64
}

// NewTimer creates a new performance timer
func NewTimer() *Timer {
	t := &Timer{
		startTime: time.Now(),
	}
	runtime.ReadMemStats(&t.startMem)
	return t
}

// AddLogs increments the log counter (thread-safe)
func (t *Timer) AddLogs(count int64) {
	t.logsProcessed.Add(count)
}

// AddBytes increments the byte counter (thread-safe)
func (t *Timer) AddBytes(bytes int64) {
	t.bytesProcessed.Add(bytes)
}

// UpdateMemory samples current memory usage
func (t *Timer) UpdateMemory() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	currentMB := int64(m.Alloc / 1024 / 1024)
	for {
		peak := t.peakMemoryMB.Load()
		if currentMB <= peak {
			break
		}
		if t.peakMemoryMB.CompareAndSwap(peak, currentMB) {
			break
		}
	}
}

// Stop finalizes measurements and returns metrics
func (t *Timer) Stop() *Metrics {
	duration := time.Since(t.startTime)

	var endMem runtime.MemStats
	runtime.ReadMemStats(&endMem)

	return &Metrics{
		Duration:       duration,
		LogsProcessed:  t.logsProcessed.Load(),
		BytesProcessed: t.bytesProcessed.Load(),
		PeakMemoryMB:   t.peakMemoryMB.Load(),
		StartMemoryMB:  int64(t.startMem.Alloc / 1024 / 1024),
		AllocsMB:       int64((endMem.TotalAlloc - t.startMem.TotalAlloc) / 1024 / 1024),
		NumGC:          endMem.NumGC - t.startMem.NumGC,
	}
}

// StageTimer tracks performance across pipeline stages
type StageTimer struct {
	stages     map[string]*stageMeasurement
	TotalTimer *Timer
}

type stageMeasurement struct {
	duration       time.Duration
	logsProcessed  int64
	bytesProcessed int64
	active         bool
	startTime      time.Time
}

// NewStageTimer creates a timer for tracking multiple stages
func NewStageTimer() *StageTimer {
	return &StageTimer{
		stages:     make(map[string]*stageMeasurement),
		TotalTimer: NewTimer(),
	}
}

// StartStage begins timing a named stage
func (st *StageTimer) StartStage(name string) {
	if _, exists := st.stages[name]; !exists {
		st.stages[name] = &stageMeasurement{}
	}
	st.stages[name].active = true
	st.stages[name].startTime = time.Now()
}

// EndStage completes timing for a stage
func (st *StageTimer) EndStage(name string, logs, bytes int64) {
	stage, exists := st.stages[name]
	if !exists || !stage.active {
		return
	}
	stage.duration += time.Since(stage.startTime)
	stage.logsProcessed += logs
	stage.bytesProcessed += bytes
	stage.active = false

	st.TotalTimer.AddLogs(logs)
	st.TotalTimer.AddBytes(bytes)
	st.TotalTimer.UpdateMemory()
}

// StageReport provides a breakdown of time spent in each stage
func (st *StageTimer) StageReport() string {
	totalMetrics := st.TotalTimer.Stop()
	report := totalMetrics.Report("Total Pipeline")
	report += "\n\nStage Breakdown:\n"

	totalDuration := totalMetrics.Duration
	for name, stage := range st.stages {
		pct := float64(stage.duration) / float64(totalDuration) * 100
		logsPerSec := float64(stage.logsProcessed) / stage.duration.Seconds()
		bytesPerSec := float64(stage.bytesProcessed) / stage.duration.Seconds()

		report += fmt.Sprintf("  %-20s %8v (%5.1f%%) - %.0f logs/s, %.2f MB/s\n",
			name+":",
			stage.duration,
			pct,
			logsPerSec,
			bytesPerSec/1e6)
	}

	return report
}

// MemorySampler periodically samples memory usage during a test
type MemorySampler struct {
	ctx       context.Context
	cancel    context.CancelFunc
	timer     *Timer
	interval  time.Duration
	doneChan  chan struct{}
	isRunning atomic.Bool
}

// NewMemorySampler creates a background memory sampler
func NewMemorySampler(timer *Timer, interval time.Duration) *MemorySampler {
	ctx, cancel := context.WithCancel(context.Background())
	return &MemorySampler{
		ctx:      ctx,
		cancel:   cancel,
		timer:    timer,
		interval: interval,
		doneChan: make(chan struct{}),
	}
}

// Start begins memory sampling in the background
func (ms *MemorySampler) Start() {
	if !ms.isRunning.CompareAndSwap(false, true) {
		return // Already running
	}

	go func() {
		defer close(ms.doneChan)
		ticker := time.NewTicker(ms.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ms.ctx.Done():
				return
			case <-ticker.C:
				ms.timer.UpdateMemory()
			}
		}
	}()
}

// Stop halts memory sampling
func (ms *MemorySampler) Stop() {
	if !ms.isRunning.CompareAndSwap(true, false) {
		return // Not running
	}
	ms.cancel()
	<-ms.doneChan
}
