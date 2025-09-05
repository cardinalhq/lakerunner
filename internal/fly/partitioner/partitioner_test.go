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

func TestHashPartitioner(t *testing.T) {
	p := &HashPartitioner{}

	tests := []struct {
		name           string
		message        fly.Message
		partitionCount int
		expectSame     bool // If same key should produce same partition
	}{
		{
			name:           "consistent hashing with key",
			message:        fly.Message{Key: []byte("consistent-key")},
			partitionCount: 5,
			expectSame:     true,
		},
		{
			name:           "empty key falls back to random",
			message:        fly.Message{Key: []byte{}},
			partitionCount: 5,
			expectSame:     false,
		},
		{
			name:           "nil key falls back to random",
			message:        fly.Message{Key: nil},
			partitionCount: 5,
			expectSame:     false,
		},
		{
			name:           "zero partitions",
			message:        fly.Message{Key: []byte("test")},
			partitionCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := make(map[int]int)
			iterations := 100

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

			if tt.expectSame && tt.partitionCount > 0 {
				// Same key should always produce same partition
				assert.Equal(t, 1, len(results), "Same key should always hash to same partition")
			} else if !tt.expectSame && tt.partitionCount > 1 {
				// Random fallback should hit multiple partitions
				assert.Greater(t, len(results), 1, "Random fallback should hit multiple partitions")
			}
		})
	}
}

func TestHashPartitioner_DifferentKeys(t *testing.T) {
	p := &HashPartitioner{}
	partitionCount := 10

	keys := []string{
		"key1", "key2", "key3", "key4", "key5",
		"different", "another", "test", "hello", "world",
	}

	partitions := make(map[string]int)

	// Each unique key should consistently hash to the same partition
	for _, key := range keys {
		msg := fly.Message{Key: []byte(key)}
		partition := p.GetPartition(msg, partitionCount)
		partitions[key] = partition

		// Verify consistency
		for i := 0; i < 10; i++ {
			samePartition := p.GetPartition(msg, partitionCount)
			assert.Equal(t, partition, samePartition, "Key %s should always hash to same partition", key)
		}
	}

	// Different keys should generally hash to different partitions
	uniquePartitions := make(map[int]bool)
	for _, p := range partitions {
		uniquePartitions[p] = true
	}

	// With 10 keys and 10 partitions, we expect good distribution
	assert.Greater(t, len(uniquePartitions), 3, "Keys should distribute across partitions")
}

func TestRoundRobinPartitioner(t *testing.T) {
	tests := []struct {
		name           string
		partitionCount int
		iterations     int
	}{
		{
			name:           "three partitions",
			partitionCount: 3,
			iterations:     9,
		},
		{
			name:           "single partition",
			partitionCount: 1,
			iterations:     5,
		},
		{
			name:           "zero partitions",
			partitionCount: 0,
			iterations:     3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Reset by creating new instance
			p := &RoundRobinPartitioner{}

			results := make([]int, 0, tt.iterations)
			msg := fly.Message{Key: []byte("test")}

			for i := 0; i < tt.iterations; i++ {
				partition := p.GetPartition(msg, tt.partitionCount)
				results = append(results, partition)

				if tt.partitionCount <= 0 {
					assert.Equal(t, 0, partition)
				} else {
					assert.Equal(t, i%tt.partitionCount, partition)
				}
			}

			// Verify round-robin pattern
			if tt.partitionCount > 1 {
				for i := 0; i < tt.iterations; i++ {
					expected := i % tt.partitionCount
					assert.Equal(t, expected, results[i], "Round-robin pattern should be maintained")
				}
			}
		})
	}
}

func TestRoundRobinPartitioner_Concurrent(t *testing.T) {
	p := &RoundRobinPartitioner{}
	partitionCount := 5
	goroutines := 10
	iterationsPerGoroutine := 100

	results := make(chan int, goroutines*iterationsPerGoroutine)

	for i := 0; i < goroutines; i++ {
		go func() {
			msg := fly.Message{}
			for j := 0; j < iterationsPerGoroutine; j++ {
				partition := p.GetPartition(msg, partitionCount)
				results <- partition
			}
		}()
	}

	// Collect all results
	partitionCounts := make(map[int]int)
	for i := 0; i < goroutines*iterationsPerGoroutine; i++ {
		partition := <-results
		assert.GreaterOrEqual(t, partition, 0)
		assert.Less(t, partition, partitionCount)
		partitionCounts[partition]++
	}

	// Each partition should be hit equally (±1 for rounding)
	expectedCount := goroutines * iterationsPerGoroutine / partitionCount
	for partition, count := range partitionCounts {
		diff := count - expectedCount
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqual(t, diff, 1, "Partition %d count should be close to expected", partition)
	}
}

func TestStickyPartitioner(t *testing.T) {
	p := NewStickyPartitioner()
	partitionCount := 5

	// Test sticky behavior for keyed messages
	keys := []string{"key1", "key2", "key3"}
	keyPartitions := make(map[string]int)

	for _, key := range keys {
		msg := fly.Message{Key: []byte(key)}
		partition := p.GetPartition(msg, partitionCount)
		keyPartitions[key] = partition

		// Verify stickiness
		for i := 0; i < 10; i++ {
			samePartition := p.GetPartition(msg, partitionCount)
			assert.Equal(t, partition, samePartition, "Key %s should stick to same partition", key)
		}
	}

	// Test round-robin for messages without keys
	emptyKeyPartitions := make([]int, 0)
	for i := 0; i < 10; i++ {
		msg := fly.Message{Key: []byte("")}
		partition := p.GetPartition(msg, partitionCount)
		emptyKeyPartitions = append(emptyKeyPartitions, partition)
		assert.GreaterOrEqual(t, partition, 0)
		assert.Less(t, partition, partitionCount)
	}

	// Empty keys should use different partitions (round-robin)
	uniquePartitions := make(map[int]bool)
	for _, p := range emptyKeyPartitions {
		uniquePartitions[p] = true
	}
	assert.Greater(t, len(uniquePartitions), 1, "Empty keys should use multiple partitions")
}

func TestStickyPartitioner_PartitionCountChange(t *testing.T) {
	p := NewStickyPartitioner()

	msg := fly.Message{Key: []byte("stable-key")}

	// Get initial partition with 5 partitions
	partition5 := p.GetPartition(msg, 5)
	assert.GreaterOrEqual(t, partition5, 0)
	assert.Less(t, partition5, 5)

	// Change partition count to 3
	partition3 := p.GetPartition(msg, 3)
	assert.GreaterOrEqual(t, partition3, 0)
	assert.Less(t, partition3, 3)
	// Should be modulo of original partition
	assert.Equal(t, partition5%3, partition3)

	// Increase to 10 partitions
	partition10 := p.GetPartition(msg, 10)
	assert.GreaterOrEqual(t, partition10, 0)
	assert.Less(t, partition10, 10)
	// Should still be based on original assignment
	assert.Equal(t, partition5%10, partition10)
}

func TestMetricKeyPartitioner(t *testing.T) {
	p := &MetricKeyPartitioner{}

	tests := []struct {
		name             string
		message          fly.Message
		partitionCount   int
		expectConsistent bool
	}{
		{
			name:             "with key",
			message:          fly.Message{Key: []byte("metric.cpu.usage")},
			partitionCount:   5,
			expectConsistent: true,
		},
		{
			name:             "empty key falls back to random",
			message:          fly.Message{Key: []byte{}},
			partitionCount:   5,
			expectConsistent: false,
		},
		{
			name:             "nil key falls back to random",
			message:          fly.Message{Key: nil},
			partitionCount:   5,
			expectConsistent: false,
		},
		{
			name:           "zero partitions",
			message:        fly.Message{Key: []byte("test")},
			partitionCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			results := make(map[int]int)
			iterations := 100

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

			if tt.expectConsistent && tt.partitionCount > 0 {
				// Same key should always produce same partition
				assert.Equal(t, 1, len(results), "Consistent key should always produce same partition")
			} else if !tt.expectConsistent && tt.partitionCount > 1 {
				// Random fallback should hit multiple partitions
				assert.Greater(t, len(results), 1, "Random fallback should hit multiple partitions")
			}
		})
	}
}

func TestMetricKeyPartitioner_Distribution(t *testing.T) {
	p := &MetricKeyPartitioner{}
	partitionCount := 10

	// Test with various metric keys
	metricKeys := []string{
		"cpu.usage.system",
		"cpu.usage.user",
		"memory.usage",
		"memory.available",
		"disk.read.bytes",
		"disk.write.bytes",
		"network.rx.bytes",
		"network.tx.bytes",
		"app.requests.count",
		"app.errors.rate",
	}

	partitions := make(map[string]int)

	for _, key := range metricKeys {
		msg := fly.Message{Key: []byte(key)}
		partition := p.GetPartition(msg, partitionCount)
		partitions[key] = partition

		// Verify consistency
		for i := 0; i < 10; i++ {
			samePartition := p.GetPartition(msg, partitionCount)
			assert.Equal(t, partition, samePartition, "Metric key %s should be consistent", key)
		}
	}

	// Check distribution
	uniquePartitions := make(map[int]bool)
	for _, p := range partitions {
		uniquePartitions[p] = true
	}

	assert.Greater(t, len(uniquePartitions), 3, "Metric keys should distribute across partitions")
}
