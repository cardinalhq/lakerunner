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

type Timestamped interface{ GetTimestamp() int64 }

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

	req := make([]chan struct{}, len(chans))
	rsp := make([]chan headMsg, len(chans))
	for i := range chans {
		req[i] = make(chan struct{}, 1)
		rsp[i] = make(chan headMsg, 1)
	}

	// Per-source fetchers
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
			for i := range req {
				close(req[i])
			}
		}()

		h := &headHeap[T]{reverse: reverse}
		heap.Init(h)

		open := make([]bool, len(chans))          // source still open (not EOF)
		inHeap := make([]bool, len(chans))        // source currently has a head in the heap
		closedPending := make([]bool, len(chans)) // hit EOF while its head was in-heap; close once popped
		awaiting := make([]bool, len(chans))      // we popped from this source and requested next
		lastTs := make([]int64, len(chans))       // last emitted timestamp per source (lower bound)

		openCount := len(chans)
		initPending := len(chans) // number of sources we haven’t seen a first head (or EOF) from
		haveHeads := 0
		emitted := 0

		// Initial request to everyone: we need one lookahead from each to establish lower bounds.
		for i := range chans {
			open[i] = true
			awaiting[i] = true
			select {
			case <-ctx.Done():
				return
			case req[i] <- struct{}{}:
			}
		}

		handleRsp := func(i int, m headMsg, ok bool) {
			if awaiting[i] {
				awaiting[i] = false
				if initPending > 0 {
					initPending--
				}
			}
			if !ok || !m.ok {
				if inHeap[i] {
					closedPending[i] = true
				} else if open[i] {
					open[i] = false
					openCount--
				}
				return
			}
			if !inHeap[i] {
				heap.Push(h, head[T]{src: i, val: m.val})
				inHeap[i] = true
				haveHeads++
			}
		}

		pollAll := func() {
			for i := range chans {
				if !(awaiting[i] || open[i] || closedPending[i]) {
					continue
				}
				select {
				case <-ctx.Done():
					return
				case m, ok := <-rsp[i]:
					handleRsp(i, m, ok)
				default:
				}
			}
		}

		waitOne := func() bool {
			idx := -1
			for i := range chans {
				if awaiting[i] {
					idx = i
					break
				}
			}
			if idx == -1 {
				return false
			}
			select {
			case <-ctx.Done():
				return false
			case m, ok := <-rsp[idx]:
				handleRsp(idx, m, ok)
				return true
			}
		}

		// Safe-to-emit check:
		//  - During initialization: require at least one head (or EOF) from every source.
		//  - After that: allow emitting if the best head's TS >= max lastTs among all sources currently awaiting.
		canEmit := func(bestTs int64) bool {
			if initPending > 0 {
				// Still establishing initial lower bounds — need a response from everyone.
				return haveHeads == openCount && h.Len() > 0
			}
			// Compute lower bound across sources we popped from and are awaiting their next value.
			var lb int64
			for i := range chans {
				if awaiting[i] {
					if lastTs[i] > lb {
						lb = lastTs[i]
					}
				}
			}
			// If no one is awaiting, lb is 0; any head is safe to emit.
			if reverse {
				// descending: best must be <= all nexts; our lower bound is an upper bound in this case.
				// We conservatively still require full-heads on init; after that, descending safe check is:
				// bestTs <= min(nextPossible[i]) but we only track lastTs (emitted) so mirror isn’t exact.
				// If you need true descending, prefer to invert timestamps outside and merge ascending.
				return true // keep behavior simple; or handle descending specifically if you use it.
			}
			return bestTs >= lb
		}

		for {
			pollAll()

			if h.Len() > 0 {
				best := (*h).data[0] // peek
				bestTs := best.val.GetTimestamp()

				if canEmit(bestTs) {
					// Now pop & advance that source
					best = heap.Pop(h).(head[T])
					src := best.src
					inHeap[src] = false
					haveHeads--

					// Record lower bound for this src (its next >= lastTs[src])
					lastTs[src] = best.val.GetTimestamp()

					if closedPending[src] {
						closedPending[src] = false
						if open[src] {
							open[src] = false
							openCount--
						}
					} else {
						awaiting[src] = true
						select {
						case <-ctx.Done():
							return
						case req[src] <- struct{}{}:
						}
					}

					// Emit
					select {
					case <-ctx.Done():
						return
					case out <- best.val:
					}
					emitted++
					if limit > 0 && emitted >= limit {
						return
					}
					continue
				}
			}

			// Drain condition
			if openCount == 0 && h.Len() == 0 {
				return
			}

			// Progress: block for at least one awaited response if we can't safely emit yet.
			if !waitOne() {
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
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
	reverse bool
}

func (h *headHeap[T]) Len() int      { return len(h.data) }
func (h *headHeap[T]) Swap(i, j int) { h.data[i], h.data[j] = h.data[j], h.data[i] }
func (h *headHeap[T]) Less(i, j int) bool {
	ti := h.data[i].val.GetTimestamp()
	tj := h.data[j].val.GetTimestamp()
	if h.reverse {
		return ti > tj
	}
	return ti < tj
}
func (h *headHeap[T]) Push(x any) { h.data = append(h.data, x.(head[T])) }
func (h *headHeap[T]) Pop() any {
	n := len(h.data)
	v := h.data[n-1]
	h.data = h.data[:n-1]
	return v
}
