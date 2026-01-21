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

package queryworker

import (
	"strconv"
	"sync"
	"testing"
)

// Simulates the current allocation pattern in mapper functions
func allocateBuffersDirect(n int) ([]any, []any) {
	vals := make([]any, n)
	ptrs := make([]any, n)
	for i := range vals {
		ptrs[i] = &vals[i]
	}
	return vals, ptrs
}

// Pool-based allocation (benchmark version)
type benchScanBuffers struct {
	vals []any
	ptrs []any
}

var benchScanBufferPool = sync.Pool{
	New: func() any {
		return &benchScanBuffers{
			vals: make([]any, 0, 32),
			ptrs: make([]any, 0, 32),
		}
	},
}

func acquireBenchScanBuffers(n int) (*benchScanBuffers, func()) {
	buf := benchScanBufferPool.Get().(*benchScanBuffers)

	if cap(buf.vals) < n {
		buf.vals = make([]any, n)
	} else {
		buf.vals = buf.vals[:n]
	}

	if cap(buf.ptrs) < n {
		buf.ptrs = make([]any, n)
	} else {
		buf.ptrs = buf.ptrs[:n]
	}

	for i := range n {
		buf.vals[i] = nil
		buf.ptrs[i] = &buf.vals[i]
	}

	return buf, func() {
		for i := range buf.vals {
			buf.vals[i] = nil
		}
		for i := range buf.ptrs {
			buf.ptrs[i] = nil
		}
		buf.vals = buf.vals[:0]
		buf.ptrs = buf.ptrs[:0]
		benchScanBufferPool.Put(buf)
	}
}

func BenchmarkScanBufferAllocation(b *testing.B) {
	colCounts := []int{10, 20, 50, 100}

	for _, cols := range colCounts {
		b.Run("Direct/cols"+strconv.Itoa(cols), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				vals, ptrs := allocateBuffersDirect(cols)
				_ = vals
				_ = ptrs
			}
		})

		b.Run("Pooled/cols"+strconv.Itoa(cols), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				buf, release := acquireBenchScanBuffers(cols)
				_ = buf.vals
				_ = buf.ptrs
				release()
			}
		})
	}
}
