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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/cardinalhq/lakerunner/internal/fly"
)

func TestRandomPartitioner(t *testing.T) {
	p := &RandomPartitioner{}

	tests := []struct {
		name           string
		message        fly.Message
		partitionCount int
	}{
		{
			name:           "multiple partitions",
			message:        fly.Message{Key: []byte("test")},
			partitionCount: 5,
		},
		{
			name:           "single partition",
			message:        fly.Message{},
			partitionCount: 1,
		},
		{
			name:           "zero partitions",
			message:        fly.Message{},
			partitionCount: 0,
		},
		{
			name:           "negative partitions",
			message:        fly.Message{},
			partitionCount: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Run multiple times to ensure randomness
			results := make(map[int]int)
			iterations := 1000

			for i := 0; i < iterations; i++ {
				partition := p.GetPartition(tt.message, tt.partitionCount)

				if tt.partitionCount <= 0 {
					assert.Equal(t, 0, partition)
				} else {
					assert.GreaterOrEqual(t, partition, 0)
					assert.Less(t, partition, tt.partitionCount)
					results[partition]++
				}
			}

			// For multiple partitions, verify distribution
			if tt.partitionCount > 1 {
				// All partitions should be hit at least once with high probability
				assert.GreaterOrEqual(t, len(results), 1)
				// With 1000 iterations, we expect reasonable distribution
				if tt.partitionCount <= 5 {
					assert.Equal(t, tt.partitionCount, len(results),
						"All partitions should be hit with 1000 iterations")
				}
			}
		})
	}
}
