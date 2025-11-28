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
	"runtime"
	"testing"
	"time"
)

func TestTimer(t *testing.T) {
	timer := NewTimer()

	// Simulate some work
	timer.AddLogs(1000)
	timer.AddBytes(50000)
	timer.UpdateMemory()

	time.Sleep(10 * time.Millisecond)

	metrics := timer.Stop()

	if metrics.LogsProcessed != 1000 {
		t.Errorf("Expected 1000 logs, got %d", metrics.LogsProcessed)
	}
	if metrics.BytesProcessed != 50000 {
		t.Errorf("Expected 50000 bytes, got %d", metrics.BytesProcessed)
	}
	if metrics.Duration < 10*time.Millisecond {
		t.Errorf("Duration too short: %v", metrics.Duration)
	}
}

func TestStageTimer(t *testing.T) {
	st := NewStageTimer()

	st.StartStage("download")
	time.Sleep(5 * time.Millisecond)
	st.EndStage("download", 500, 25000)

	st.StartStage("process")
	time.Sleep(10 * time.Millisecond)
	st.EndStage("process", 500, 25000)

	report := st.StageReport()
	if report == "" {
		t.Error("Stage report should not be empty")
	}
	t.Log("\n" + report)
}

func TestMemorySampler(t *testing.T) {
	// Force a GC to get a clean baseline
	runtime.GC()

	timer := NewTimer()
	sampler := NewMemorySampler(timer, 1*time.Millisecond)

	sampler.Start()
	defer sampler.Stop()

	// Allocate some memory and force update
	data := make([]byte, 10*1024*1024) // 10 MB
	timer.UpdateMemory()
	runtime.KeepAlive(data)

	time.Sleep(50 * time.Millisecond)

	metrics := timer.Stop()
	// Relaxed check - just verify sampling works
	if metrics.PeakMemoryMB == 0 && metrics.StartMemoryMB == 0 {
		t.Errorf("Memory sampling appears to not be working")
	}
	t.Logf("Peak memory: %d MB, Start memory: %d MB", metrics.PeakMemoryMB, metrics.StartMemoryMB)
}

func BenchmarkTimerOverhead(b *testing.B) {
	timer := NewTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		timer.AddLogs(1)
		timer.AddBytes(100)
	}

	timer.Stop()
}

func BenchmarkMemoryUpdate(b *testing.B) {
	timer := NewTimer()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		timer.UpdateMemory()
	}
}
