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

package promql

import (
	"context"
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

	out := MergeSorted[tsItem](ctx /*ascending*/, 8, false, 0, ch1, ch2)
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

	out := MergeSorted[tsItem](ctx, 2, false, 0, ch1, ch2)
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

	out := MergeSorted[tsItem](ctx, 1, false, 0, c1, c2, c3)
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
	out := MergeSorted[tsItem](ctx, 1, false, 0)
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

	out := MergeSorted[tsItem](ctx, 32, false, 0, ch)

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

	out := MergeSorted[tsItem](ctx, 8, true, 0, ch1, ch2)
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

	out := MergeSorted[tsItem](ctx, 2, true, 0, ch1, ch2)
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

	out := MergeSorted[tsItem](ctx, 1, true, 0, c1, c2, c3)
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
	out := MergeSorted[tsItem](ctx, 8, false /*asc*/, 5 /*limit*/, chFromSlice(ctx, 0, a), chFromSlice(ctx, 0, b))
	got := toSlice(out)
	if len(got) != 5 {
		t.Fatalf("len mismatch: got=%d want=5", len(got))
	}
	if !isSortedAsc(got) {
		t.Fatalf("not sorted asc: %#v", got)
	}
}

func TestMergeSorted_Limit_Descending(t *testing.T) {
	ctx := context.Background()
	a := []tsItem{{8}, {6}, {4}, {2}}
	b := []tsItem{{7}, {5}, {3}, {1}}
	out := MergeSorted[tsItem](ctx, 8, true /*desc*/, 3 /*limit*/, chFromSlice(ctx, 0, a), chFromSlice(ctx, 0, b))
	got := toSlice(out)
	if len(got) != 3 {
		t.Fatalf("len mismatch: got=%d want=3", len(got))
	}
	if !isSortedDesc(got) {
		t.Fatalf("not sorted desc: %#v", got)
	}
}
