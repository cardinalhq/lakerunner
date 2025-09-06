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
	"sync/atomic"

	"github.com/cardinalhq/lakerunner/internal/fly"
)

// RoundRobinPartitioner distributes messages evenly across partitions
type RoundRobinPartitioner struct {
	counter atomic.Uint64
}

// GetPartition returns partitions in round-robin order
func (p *RoundRobinPartitioner) GetPartition(message fly.Message, partitionCount int) int {
	if partitionCount <= 0 {
		return 0
	}
	next := p.counter.Add(1)
	return int((next - 1) % uint64(partitionCount))
}
