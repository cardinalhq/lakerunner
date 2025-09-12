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
	"container/heap"
	"context"
)

// Timestamped is the constraint for mergeable items.
type Timestamped interface {
	GetTimestamp() int64
}

// MergeSorted merges N locally-sorted channels into one globally-sorted stream.
// reverse=false → ascending; reverse=true → descending.
// limit=0 → unlimited; limit>0 → stop after emitting exactly limit items.
func MergeSorted[T Timestamped](
	ctx context.Context,
	outBuf int,
	reverse bool,
	limit int,
	chans ...<-chan T,
) <-chan T {
	out := make(chan T, outBuf)
	if len(chans) == 0 {
		close(out)
		return out
	}

	type headMsg struct {
		src int
		val T
		ok  bool
	}

	// Per-source request/response channels.
	req := make([]chan struct{}, len(chans))
	rsp := make([]chan headMsg, len(chans))
	for i := range chans {
		req[i] = make(chan struct{}, 1)
		rsp[i] = make(chan headMsg, 1)
	}

	// Per-source fetchers: on request, pull next item (or close).
	for i, ch := range chans {
		i, ch := i, ch
		go func() {
			defer close(rsp[i])
			for {
				select {
				case <-ctx.Done():
					return
				case _, ok := <-req[i]:
					if !ok {
						return
					}
					v, ok := <-ch
					if !ok {
						// Source closed.
						select {
						case <-ctx.Done():
						case rsp[i] <- headMsg{src: i, ok: false}:
						}
						return
					}
					select {
					case <-ctx.Done():
						return
					case rsp[i] <- headMsg{src: i, val: v, ok: true}:
					}
				}
			}
		}()
	}

	go func() {
		defer close(out)
		defer func() {
			// Unblock/wind-down sources
			for i := range req {
				close(req[i])
			}
		}()

		h := &headHeap[T]{reverse: reverse}
		heap.Init(h)

		open := make([]bool, len(chans)) // source is still open (not fully closed)
		inHeap := make([]bool, len(chans))

		// --- Phase 1: request first head from every source and wait until each
		// either provides the first head or closes. This guarantees a valid
		// initial heap containing the "current head" for every open source.
		pending := 0
		for i := range chans {
			open[i] = true
			pending++
			select {
			case <-ctx.Done():
				return
			case req[i] <- struct{}{}:
			}
		}

		for pending > 0 {
			// Wait for responses from any outstanding first-head request.
			handled := false
			for i := range chans {
				if !open[i] || inHeap[i] { // responded already
					continue
				}
				select {
				case <-ctx.Done():
					return
				case m, ok := <-rsp[i]:
					handled = true
					pending--
					if !ok || !m.ok {
						// Source closed before first head
						open[i] = false
						continue
					}
					heap.Push(h, head[T]{src: i, val: m.val})
					inHeap[i] = true
				default:
				}
			}
			if !handled {
				// Block on at least one still-pending source to avoid spin.
				idx := -1
				for i := range chans {
					if open[i] && !inHeap[i] {
						idx = i
						break
					}
				}
				if idx == -1 {
					break // nothing pending
				}
				select {
				case <-ctx.Done():
					return
				case m, ok := <-rsp[idx]:
					pending--
					if !ok || !m.ok {
						open[idx] = false
						continue
					}
					heap.Push(h, head[T]{src: idx, val: m.val})
					inHeap[idx] = true
				}
			}
		}

		// If no open sources produced any head, we are done.
		if h.Len() == 0 {
			return
		}

		emitted := 0

		// --- Phase 2: steady-state.
		// Always pop the best head; then we ONLY need the *next* head from the same
		// source before we can safely emit the next global item.
		for {
			// If heap empty, all open sources must have closed.
			if h.Len() == 0 {
				return
			}

			// Pop best global head and emit it.
			best := heap.Pop(h).(head[T])
			src := best.src
			inHeap[src] = false

			select {
			case <-ctx.Done():
				return
			case out <- best.val:
			}
			emitted++
			if limit > 0 && emitted >= limit {
				return
			}

			// Request the next head from the SAME source (src) and wait for it
			// (or its close). This is the only required blocking to keep strict order.
			select {
			case <-ctx.Done():
				return
			case req[src] <- struct{}{}:
			}

			// Wait for the response from src. We *can* opportunistically drain
			// any other available responses to avoid goroutine mailbox growth,
			// but it's not required for correctness. We'll handle only src to keep
			// the logic clear and minimal-stall.
			var nextOk bool
			var next headMsg

			for {
				select {
				case <-ctx.Done():
					return
				case m, ok := <-rsp[src]:
					next, nextOk = m, ok
					goto receivedSrc
				default:
					// Optionally drain others here if desired:
					// (No-op by default)
				}
				// If non-blocking read didn't catch src yet, block on it.
				select {
				case <-ctx.Done():
					return
				case m, ok := <-rsp[src]:
					next, nextOk = m, ok
					goto receivedSrc
				}
			}

		receivedSrc:
			if !nextOk || !next.ok {
				// src closed: it no longer contributes to ordering; just continue.
				open[src] = false
				continue
			}
			// Push the new head for src; we now have current heads for every open source again.
			heap.Push(h, head[T]{src: src, val: next.val})
			inHeap[src] = true
		}
	}()

	return out
}

// ----- heap plumbing -----

type head[T Timestamped] struct {
	src int
	val T
}

type headHeap[T Timestamped] struct {
	data    []head[T]
	reverse bool // when true, choose larger timestamps first
}

func (h *headHeap[T]) Len() int { return len(h.data) }

func (h *headHeap[T]) Less(i, j int) bool {
	ti := h.data[i].val.GetTimestamp()
	tj := h.data[j].val.GetTimestamp()
	if h.reverse {
		return ti > tj // max-heap behavior by timestamp
	}
	return ti < tj // min-heap behavior by timestamp
}

func (h *headHeap[T]) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }
func (h *headHeap[T]) Push(x any)    { h.data = append(h.data, x.(head[T])) }
func (h *headHeap[T]) Pop() any {
	n := len(h.data)
	v := h.data[n-1]
	h.data = h.data[:n-1]
	return v
}
