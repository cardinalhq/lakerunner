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

package promql

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- helpers ---

type tsItem struct{ ts int64 }

func (t tsItem) GetTimestamp() int64 { return t.ts }

func chFromSlice[T Timestamped](ctx context.Context, buf int, xs []T) <-chan T {
	out := make(chan T, buf)
	go func() {
		defer close(out)
		for _, v := range xs {
			select {
			case <-ctx.Done():
				return
			case out <- v:
			}
		}
	}()
	return out
}

func toSlice[T any](in <-chan T) []T {
	var out []T
	for v := range in {
		out = append(out, v)
	}
	return out
}

func isSortedAsc(xs []tsItem) bool {
	for i := 1; i < len(xs); i++ {
		if xs[i-1].ts > xs[i].ts {
			return false
		}
	}
	return true
}

// func isSortedDesc(xs []tsItem) bool {
// 	for i := 1; i < len(xs); i++ {
// 		if xs[i-1].ts < xs[i].ts {
// 			return false
// 		}
// 	}
// 	return true
// }

// --- tests ---

func TestMergeSorted_Ascending_TwoInputs(t *testing.T) {
	ctx := context.Background()

	a := []tsItem{{1}, {3}, {5}, {7}}
	b := []tsItem{{2}, {4}, {6}, {8}}
	ch1 := chFromSlice(ctx, 0, a)
	ch2 := chFromSlice(ctx, 0, b)

	out := MergeSorted(ctx, nil /*ascending*/, 8, false, 0, ch1, ch2)
	got := toSlice(out)

	if len(got) != len(a)+len(b) {
		t.Fatalf("len mismatch: got=%d want=%d", len(got), len(a)+len(b))
	}
	if !isSortedAsc(got) {
		t.Fatalf("not sorted ascending: %#v", got)
	}
}

func TestMergeSorted_Ascending_WithDuplicates(t *testing.T) {
	ctx := context.Background()

	// Duplicates across inputs; only require global non-decreasing order.
	a := []tsItem{{1}, {2}, {2}, {5}}
	b := []tsItem{{2}, {3}, {4}}
	ch1 := chFromSlice(ctx, 0, a)
	ch2 := chFromSlice(ctx, 0, b)

	out := MergeSorted(ctx, nil, 2, false, 0, ch1, ch2)
	got := toSlice(out)

	if !isSortedAsc(got) {
		t.Fatalf("not sorted ascending (duplicates): %#v", got)
	}
}

func TestMergeSorted_HandlesEmptyInputs(t *testing.T) {
	ctx := context.Background()

	empty := []tsItem{}
	full := []tsItem{{1}, {2}, {3}}
	c1 := chFromSlice(ctx, 0, empty)
	c2 := chFromSlice(ctx, 0, full)
	c3 := chFromSlice(ctx, 0, empty)

	out := MergeSorted(ctx, nil, 1, false, 0, c1, c2, c3)
	got := toSlice(out)

	if len(got) != len(full) {
		t.Fatalf("len mismatch: got=%d want=%d", len(got), len(full))
	}
	if !isSortedAsc(got) {
		t.Fatalf("not sorted ascending: %#v", got)
	}
}

func TestMergeSorted_NoInputs(t *testing.T) {
	ctx := context.Background()
	out := MergeSorted[tsItem](ctx, nil, 1, false, 0)
	got := toSlice(out)
	if len(got) != 0 {
		t.Fatalf("expected empty, got: %#v", got)
	}
}

func TestMergeSorted_Cancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Long stream; we’ll cancel early and ensure out closes soon.
	long := make([]tsItem, 0, 1000)
	for i := 0; i < 1000; i++ {
		long = append(long, tsItem{int64(i)})
	}
	ch := chFromSlice(ctx, 0, long)

	out := MergeSorted(ctx, nil, 32, false, 0, ch)

	// Consume a few, then cancel and ensure the channel closes.
	for i := 0; i < 10; i++ {
		_, ok := <-out
		if !ok {
			t.Fatalf("output closed too early")
		}
	}
	cancel()

	// Wait for closure with a timeout to avoid hanging the test.
	timeout := time.After(2 * time.Second)
	for {
		select {
		case _, ok := <-out:
			if !ok {
				return // success
			}
			// keep draining until closed or timeout
		case <-timeout:
			t.Fatal("timeout waiting for output channel to close after cancel")
		}
	}
}

func isSortedDesc(xs []tsItem) bool {
	for i := 1; i < len(xs); i++ {
		if xs[i-1].ts < xs[i].ts {
			return false
		}
	}
	return true
}

// --- tests ---

func TestMergeSorted_Descending_TwoInputs(t *testing.T) {
	ctx := context.Background()

	// Each input must be non-increasing when reverse=true.
	a := []tsItem{{8}, {6}, {4}, {2}}
	b := []tsItem{{7}, {5}, {3}, {1}}
	ch1 := chFromSlice(ctx, 0, a)
	ch2 := chFromSlice(ctx, 0, b)

	out := MergeSorted(ctx, nil, 8, true, 0, ch1, ch2)
	got := toSlice(out)

	if len(got) != len(a)+len(b) {
		t.Fatalf("len mismatch: got=%d want=%d", len(got), len(a)+len(b))
	}
	if !isSortedDesc(got) {
		t.Fatalf("not sorted descending: %#v", got)
	}
}

func TestMergeSorted_Descending_WithDuplicates(t *testing.T) {
	ctx := context.Background()

	// Duplicates across inputs; only require global non-increasing order.
	a := []tsItem{{9}, {7}, {7}, {3}}
	b := []tsItem{{8}, {7}, {4}}
	ch1 := chFromSlice(ctx, 0, a)
	ch2 := chFromSlice(ctx, 0, b)

	out := MergeSorted(ctx, nil, 2, true, 0, ch1, ch2)
	got := toSlice(out)

	if !isSortedDesc(got) {
		t.Fatalf("not sorted descending (duplicates): %#v", got)
	}
}

func TestMergeSorted_Descending_HandlesEmptyInputs(t *testing.T) {
	ctx := context.Background()

	var empty []tsItem
	full := []tsItem{{5}, {3}, {1}}
	c1 := chFromSlice(ctx, 0, empty)
	c2 := chFromSlice(ctx, 0, full)
	c3 := chFromSlice(ctx, 0, empty)

	out := MergeSorted(ctx, nil, 1, true, 0, c1, c2, c3)
	got := toSlice(out)

	if len(got) != len(full) {
		t.Fatalf("len mismatch: got=%d want=%d", len(got), len(full))
	}
	if !isSortedDesc(got) {
		t.Fatalf("not sorted descending: %#v", got)
	}
}

func TestMergeSorted_Limit_Ascending(t *testing.T) {
	ctx := context.Background()
	a := []tsItem{{1}, {3}, {5}, {7}}
	b := []tsItem{{2}, {4}, {6}, {8}}

	// Test that cancel is called when limit is reached
	cancelCalled := false
	cancel := func() {
		cancelCalled = true
	}

	out := MergeSorted(ctx, cancel, 8, false /*asc*/, 5 /*limit*/, chFromSlice(ctx, 0, a), chFromSlice(ctx, 0, b))
	got := toSlice(out)
	if len(got) != 5 {
		t.Fatalf("len mismatch: got=%d want=5", len(got))
	}
	if !isSortedAsc(got) {
		t.Fatalf("not sorted asc: %#v", got)
	}
	if !cancelCalled {
		t.Fatal("producerCancel was not called when limit was reached")
	}
}

func TestMergeSorted_Limit_Descending(t *testing.T) {
	ctx := context.Background()
	a := []tsItem{{8}, {6}, {4}, {2}}
	b := []tsItem{{7}, {5}, {3}, {1}}

	// Test that cancel is called when limit is reached
	cancelCalled := false
	cancel := func() {
		cancelCalled = true
	}

	out := MergeSorted(ctx, cancel, 8, true /*desc*/, 3 /*limit*/, chFromSlice(ctx, 0, a), chFromSlice(ctx, 0, b))
	got := toSlice(out)
	if len(got) != 3 {
		t.Fatalf("len mismatch: got=%d want=3", len(got))
	}
	if !isSortedDesc(got) {
		t.Fatalf("not sorted desc: %#v", got)
	}
	if !cancelCalled {
		t.Fatal("producerCancel was not called when limit was reached")
	}
}

// TestMergeSorted_LimitDrainsPreventsBlocking verifies that when a limit is reached,
// the drain mechanism prevents producers from blocking on channel sends.
func TestMergeSorted_LimitDrainsPreventsBlocking(t *testing.T) {
	ctx := context.Background()

	// Create producers that will send more data than the limit
	// Use unbuffered channels to ensure blocking sends
	producerCount := 3
	itemsPerProducer := 100
	limit := 10

	producers := make([]<-chan tsItem, producerCount)
	producerBlocked := make([]bool, producerCount)
	var producerWg sync.WaitGroup
	producerWg.Add(producerCount)

	for i := range producerCount {
		ch := make(chan tsItem) // Unbuffered channel - will block on send if not consumed
		producers[i] = ch

		go func(idx int) {
			defer producerWg.Done()
			defer close(ch)

			for j := range itemsPerProducer {
				ts := int64(idx*1000 + j)
				item := tsItem{ts: ts}

				// Use a timeout to detect if we're blocked
				timer := time.NewTimer(100 * time.Millisecond)
				select {
				case ch <- item:
					timer.Stop()
				case <-timer.C:
					producerBlocked[idx] = true
					return
				case <-ctx.Done():
					timer.Stop()
					return
				}
			}
		}(i)
	}

	// Start MergeSorted with a limit
	merged := MergeSorted(ctx, nil, 8, false, limit, producers...)

	// Consume all items from merged channel
	consumed := 0
	for range merged {
		consumed++
	}

	if consumed != limit {
		t.Errorf("Expected exactly %d items, got %d", limit, consumed)
	}

	// Wait for producers to finish (they should not block)
	done := make(chan struct{})
	go func() {
		producerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Good - all producers finished
	case <-time.After(2 * time.Second):
		t.Fatal("Producers blocked - drain mechanism failed")
	}

	for i, blocked := range producerBlocked {
		if blocked {
			t.Errorf("Producer %d was blocked on send", i)
		}
	}
}

// TestMergeSorted_NoGoroutineLeakWithLimit verifies that reaching the limit
// doesn't leave goroutines running.
func TestMergeSorted_NoGoroutineLeakWithLimit(t *testing.T) {
	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()

	ctx := context.Background()

	// Run the merge with limit multiple times
	for i := range 5 {
		producers := make([]<-chan tsItem, 3)
		for j := range 3 {
			ch := make(chan tsItem, 1000)
			producers[j] = ch

			go func(ch chan tsItem, offset int) {
				defer close(ch)
				for k := range 1000 {
					ch <- tsItem{ts: int64(offset + k)}
				}
			}(ch, j*1000)
		}

		merged := MergeSorted(ctx, nil, 8, false, 5, producers...)

		count := 0
		for range merged {
			count++
		}

		if count != 5 {
			t.Errorf("iteration %d: expected 5 items, got %d", i, count)
		}
	}

	// Give goroutines time to exit
	time.Sleep(100 * time.Millisecond)
	runtime.GC()

	finalGoroutines := runtime.NumGoroutine()

	// Allow for some variance, but should be close to initial
	if finalGoroutines > initialGoroutines+2 {
		t.Errorf("Potential goroutine leak: started with %d, ended with %d",
			initialGoroutines, finalGoroutines)
	}
}

// TestMergeSorted_BlockingProducerScenario simulates the exact scenario from the bug:
// producers with blocking sends that would hang without proper draining.
func TestMergeSorted_BlockingProducerScenario(t *testing.T) {
	ctx := context.Background()

	type producer struct {
		items    []tsItem
		finished chan struct{}
	}

	producers := []producer{
		{items: make([]tsItem, 100), finished: make(chan struct{})},
		{items: make([]tsItem, 100), finished: make(chan struct{})},
		{items: make([]tsItem, 100), finished: make(chan struct{})},
	}

	for i := range producers {
		for j := range producers[i].items {
			producers[i].items[j] = tsItem{ts: int64(i*100 + j)}
		}
	}

	chans := make([]<-chan tsItem, len(producers))
	for i, p := range producers {
		ch := make(chan tsItem, 8) // Small buffer
		chans[i] = ch

		go func(p producer, ch chan tsItem) {
			defer close(p.finished)
			defer close(ch)

			for _, item := range p.items {
				select {
				case <-ctx.Done():
					return
				case ch <- item:
				}
			}
		}(p, ch)
	}

	limit := 20
	merged := MergeSorted(ctx, nil, 8, false, limit, chans...)

	consumed := 0
	for range merged {
		consumed++
		if consumed >= limit {
			break
		}
	}

	// Verify all producers finish (don't block)
	allFinished := make(chan struct{})
	go func() {
		for _, p := range producers {
			<-p.finished
		}
		close(allFinished)
	}()

	select {
	case <-allFinished:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("Producers blocked - this is the exact bug that was fixed")
	}
}

// TestMergeSorted_ProducerCancelOnLimit verifies that when a limit is reached,
// the producer cancel function is called to stop upstream producers immediately.
func TestMergeSorted_ProducerCancelOnLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var producerCount atomic.Int32
	ch := make(chan tsItem, 10)

	go func() {
		defer close(ch)
		for i := range 1000 {
			select {
			case <-ctx.Done():
				return
			case ch <- tsItem{ts: int64(i)}:
				producerCount.Add(1)
			}
		}
	}()

	limit := 5
	merged := MergeSorted(ctx, cancel, 8, false, limit, ch)

	got := toSlice(merged)

	if len(got) != limit {
		t.Fatalf("expected %d items, got %d", limit, len(got))
	}

	// Give producer time to react to cancellation
	time.Sleep(100 * time.Millisecond)

	// Producer should have been cancelled early
	count := producerCount.Load()
	if count >= 30 {
		t.Fatalf("producer generated too many items (%d), should have been cancelled early", count)
	}
}
