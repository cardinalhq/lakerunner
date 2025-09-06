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
	"hash/fnv"

	"github.com/cardinalhq/lakerunner/internal/fly"
)

// MetricKeyPartitioner partitions based on simplified metric key
type MetricKeyPartitioner struct{}

// GetPartition returns a partition based on simplified metric key hash
func (p *MetricKeyPartitioner) GetPartition(message fly.Message, partitionCount int) int {
	if partitionCount <= 0 {
		return 0
	}

	// Use the message key if available
	if len(message.Key) > 0 {
		h := fnv.New32a()
		h.Write(message.Key)
		return int(h.Sum32() % uint32(partitionCount))
	}

	// Fall back to random if no key
	rp := &RandomPartitioner{}
	return rp.GetPartition(message, partitionCount)
}
