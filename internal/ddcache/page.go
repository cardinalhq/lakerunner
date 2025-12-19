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

package ddcache

const (
	// pageSize is the size of each page's backing buffer.
	// 64KB provides good balance: small enough to avoid waste,
	// large enough to amortize allocation overhead.
	pageSize = 64 * 1024 // 64KB
)

// pageState represents the lifecycle state of a page.
type pageState uint8

const (
	pageAppending pageState = iota // Can append new items
	pageDraining                   // Items being moved off (future use)
	pageSealed                     // No more writes, good fill factor
)

// page is a write-only buffer that stores fixed-size items contiguously.
// Items are appended and referenced by index. The page owns the memory.
type page struct {
	data   []byte    // backing buffer (1MB)
	stride int       // size of each item in bytes
	count  int       // number of items currently stored
	state  pageState // lifecycle state
}

// newPage creates a new page for items of the given stride size.
func newPage(stride int) *page {
	return &page{
		data:   make([]byte, pageSize),
		stride: stride,
		state:  pageAppending,
	}
}

// capacity returns the maximum number of items this page can hold.
func (p *page) capacity() int {
	return pageSize / p.stride
}

// available returns the number of items that can still be added.
func (p *page) available() int {
	return p.capacity() - p.count
}

// append adds data to the page and returns the index.
// Returns -1 if the page is full or not in appending state.
func (p *page) append(data []byte) int {
	if p.state != pageAppending {
		return -1
	}
	if len(data) > p.stride {
		return -1 // data too large for stride
	}
	if p.count >= p.capacity() {
		return -1 // full
	}

	offset := p.count * p.stride
	// Copy data, zero-pad if smaller than stride
	copy(p.data[offset:offset+p.stride], data)
	index := p.count
	p.count++
	return index
}

// get returns a slice pointing to the item at the given index.
// The returned slice is valid until the page is garbage collected.
func (p *page) get(index int) []byte {
	offset := index * p.stride
	return p.data[offset : offset+p.stride]
}

// seal marks the page as sealed (no more writes).
func (p *page) seal() {
	p.state = pageSealed
}

// pageManager manages pages grouped by stride size.
// It tracks the current appending page for each stride.
type pageManager struct {
	// activePages maps stride -> current appending page
	activePages map[int]*page

	// allPages tracks all pages for memory accounting
	allPages []*page
}

// newPageManager creates a new page manager.
func newPageManager() *pageManager {
	return &pageManager{
		activePages: make(map[int]*page),
	}
}

// allocate stores data and returns the page and index.
// Creates a new page if needed. Each unique data length gets its own page(s).
func (pm *pageManager) allocate(data []byte) (*page, int) {
	stride := len(data)
	if stride == 0 {
		return nil, -1
	}

	// Get or create active page for this stride (no alignment rounding)
	p := pm.activePages[stride]
	if p == nil || p.available() == 0 {
		// Need a new page
		if p != nil {
			p.seal()
		}
		p = newPage(stride)
		pm.activePages[stride] = p
		pm.allPages = append(pm.allPages, p)
	}

	index := p.append(data)
	return p, index
}
