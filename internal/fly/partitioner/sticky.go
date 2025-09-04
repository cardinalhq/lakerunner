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

package partitioner

import (
	"sync"

	"github.com/cardinalhq/lakerunner/internal/fly"
)

// StickyPartitioner maintains sticky key-to-partition mapping
type StickyPartitioner struct {
	mu         sync.RWMutex
	partitions map[string]int
	nextIndex  int
}

// NewStickyPartitioner creates a new sticky partitioner
func NewStickyPartitioner() *StickyPartitioner {
	return &StickyPartitioner{
		partitions: make(map[string]int),
	}
}

// GetPartition returns the same partition for the same key
func (p *StickyPartitioner) GetPartition(message fly.Message, partitionCount int) int {
	if partitionCount <= 0 {
		return 0
	}

	key := string(message.Key)
	if key == "" {
		// Fall back to round-robin for messages without keys
		p.mu.Lock()
		partition := p.nextIndex % partitionCount
		p.nextIndex++
		p.mu.Unlock()
		return partition
	}

	p.mu.RLock()
	partition, exists := p.partitions[key]
	p.mu.RUnlock()

	if exists {
		// Ensure partition is still valid if partition count changed
		return partition % partitionCount
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Double-check after acquiring write lock
	partition, exists = p.partitions[key]
	if exists {
		return partition % partitionCount
	}

	// Assign new partition
	partition = p.nextIndex % partitionCount
	p.partitions[key] = partition
	p.nextIndex++

	return partition
}
