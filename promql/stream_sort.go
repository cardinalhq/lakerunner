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
	limit int, // <-- new
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

	// Per-source fetchers.
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
		defer func() { // unblock/wind-down sources
			for i := range req {
				close(req[i])
			}
		}()

		h := &headHeap[T]{reverse: reverse}
		heap.Init(h)

		open := make([]bool, len(chans))
		inHeap := make([]bool, len(chans))
		closedPending := make([]bool, len(chans))
		awaiting := make([]bool, len(chans))

		openCount := len(chans)
		initPending := len(chans)
		haveHeads := 0
		emitted := 0 // <-- new

		// Request the first head from every source.
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

		for {
			pollAll()

			if initPending == 0 && haveHeads == openCount && h.Len() > 0 {
				best := heap.Pop(h).(head[T])
				src := best.src
				inHeap[src] = false
				haveHeads--

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

				// Emit the chosen item.
				select {
				case <-ctx.Done():
					return
				case out <- best.val:
				}
				emitted++ // <-- count it
				if limit > 0 && emitted >= limit {
					return // graceful stop: defers close(out) and close(req[*])
				}
				continue
			}

			if openCount == 0 && h.Len() == 0 {
				return
			}
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
