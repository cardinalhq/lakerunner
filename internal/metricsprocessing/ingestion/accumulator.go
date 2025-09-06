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

package ingestion

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// OrgInstanceKey uniquely identifies an organization and instance pair
type OrgInstanceKey struct {
	OrganizationID uuid.UUID
	InstanceNum    int32
}

// ReaderMetadata tracks metadata for each reader
type ReaderMetadata struct {
	ObjectID       string
	OrganizationID uuid.UUID
	InstanceNum    int32
	Bucket         string
	FileSize       int64
	IngestItem     ingest.IngestItem
}

// KafkaMessageInfo tracks Kafka message information
type KafkaMessageInfo struct {
	Partition int32
	Offset    int64
}

// OrgInstanceAccumulator accumulates readers for a specific (org, instance) pair
type OrgInstanceAccumulator struct {
	key            OrgInstanceKey
	readers        []filereader.Reader
	readerMetadata []ReaderMetadata
	kafkaMessages  []KafkaMessageInfo
	writerManager  *metricWriterManager
	startTime      time.Time
}

// NewOrgInstanceAccumulator creates a new accumulator for an (org, instance) pair
func NewOrgInstanceAccumulator(key OrgInstanceKey) *OrgInstanceAccumulator {
	return &OrgInstanceAccumulator{
		key:            key,
		readers:        make([]filereader.Reader, 0),
		readerMetadata: make([]ReaderMetadata, 0),
		kafkaMessages:  make([]KafkaMessageInfo, 0),
		startTime:      time.Now(),
	}
}

// AddReader adds a reader and its metadata to the accumulator
func (a *OrgInstanceAccumulator) AddReader(reader filereader.Reader, metadata ReaderMetadata, kafkaMsg KafkaMessageInfo) {
	a.readers = append(a.readers, reader)
	a.readerMetadata = append(a.readerMetadata, metadata)
	a.kafkaMessages = append(a.kafkaMessages, kafkaMsg)
}

// Close closes all readers in the accumulator
func (a *OrgInstanceAccumulator) Close() error {
	var firstErr error
	for _, reader := range a.readers {
		if err := reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// AccumulatorManager manages multiple OrgInstanceAccumulators
type AccumulatorManager struct {
	accumulators        map[OrgInstanceKey]*OrgInstanceAccumulator
	maxAccumulationTime time.Duration
	globalStartTime     time.Time
	kafkaOffsets        map[int32]int64 // Track highest offset per partition
	mu                  sync.Mutex
	totalReadersCount   int
}

// NewAccumulatorManager creates a new accumulator manager
func NewAccumulatorManager(maxAccumulationTime time.Duration) *AccumulatorManager {
	return &AccumulatorManager{
		accumulators:        make(map[OrgInstanceKey]*OrgInstanceAccumulator),
		maxAccumulationTime: maxAccumulationTime,
		globalStartTime:     time.Now(),
		kafkaOffsets:        make(map[int32]int64),
	}
}

// GetOrCreateAccumulator gets or creates an accumulator for the given key
func (m *AccumulatorManager) GetOrCreateAccumulator(key OrgInstanceKey) *OrgInstanceAccumulator {
	m.mu.Lock()
	defer m.mu.Unlock()

	if acc, exists := m.accumulators[key]; exists {
		return acc
	}

	acc := NewOrgInstanceAccumulator(key)
	m.accumulators[key] = acc
	return acc
}

// AddReader adds a reader to the appropriate accumulator
func (m *AccumulatorManager) AddReader(key OrgInstanceKey, reader filereader.Reader, metadata ReaderMetadata, kafkaMsg KafkaMessageInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()

	acc := m.GetOrCreateAccumulatorLocked(key)
	acc.AddReader(reader, metadata, kafkaMsg)
	m.totalReadersCount++

	// Track highest offset per partition
	if currentOffset, exists := m.kafkaOffsets[kafkaMsg.Partition]; !exists || kafkaMsg.Offset > currentOffset {
		m.kafkaOffsets[kafkaMsg.Partition] = kafkaMsg.Offset
	}
}

// GetOrCreateAccumulatorLocked gets or creates an accumulator (must be called with lock held)
func (m *AccumulatorManager) GetOrCreateAccumulatorLocked(key OrgInstanceKey) *OrgInstanceAccumulator {
	if acc, exists := m.accumulators[key]; exists {
		return acc
	}

	acc := NewOrgInstanceAccumulator(key)
	m.accumulators[key] = acc
	return acc
}

// UpdateOffset updates the highest offset for a partition
func (m *AccumulatorManager) UpdateOffset(partition int32, offset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if currentOffset, exists := m.kafkaOffsets[partition]; !exists || offset > currentOffset {
		m.kafkaOffsets[partition] = offset
	}
}

// ShouldFlush determines if any accumulator should be flushed
func (m *AccumulatorManager) ShouldFlush() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Flush if time window exceeded
	if time.Since(m.globalStartTime) >= m.maxAccumulationTime {
		return true
	}

	// Could add additional conditions here (memory pressure, reader count, etc.)
	return false
}

// GetAccumulators returns all accumulators (safe copy)
func (m *AccumulatorManager) GetAccumulators() map[OrgInstanceKey]*OrgInstanceAccumulator {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return the map directly as it will be used immediately
	return m.accumulators
}

// GetKafkaOffsetUpdates returns Kafka offset updates for all partitions
func (m *AccumulatorManager) GetKafkaOffsetUpdates(consumerGroup, topic string) []lrdb.KafkaOffsetUpdate {
	m.mu.Lock()
	defer m.mu.Unlock()

	updates := make([]lrdb.KafkaOffsetUpdate, 0, len(m.kafkaOffsets))
	for partition, offset := range m.kafkaOffsets {
		updates = append(updates, lrdb.KafkaOffsetUpdate{
			ConsumerGroup: consumerGroup,
			Topic:         topic,
			Partition:     partition,
			Offset:        offset,
		})
	}
	return updates
}

// GetHighestOffsets returns the highest offset per partition
func (m *AccumulatorManager) GetHighestOffsets() map[int32]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Return a copy to avoid concurrent access
	offsets := make(map[int32]int64, len(m.kafkaOffsets))
	for k, v := range m.kafkaOffsets {
		offsets[k] = v
	}
	return offsets
}

// Reset resets the manager for a new accumulation window
func (m *AccumulatorManager) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Close all readers in all accumulators
	for _, acc := range m.accumulators {
		acc.Close()
	}

	// Reset state
	m.accumulators = make(map[OrgInstanceKey]*OrgInstanceAccumulator)
	m.globalStartTime = time.Now()
	m.kafkaOffsets = make(map[int32]int64)
	m.totalReadersCount = 0
}

// Close closes all accumulators and their readers
func (m *AccumulatorManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var firstErr error
	for _, acc := range m.accumulators {
		if err := acc.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// HasData returns true if there's any data to flush
func (m *AccumulatorManager) HasData() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.totalReadersCount > 0
}

// FlushAccumulator processes a single accumulator and returns parquet results
func FlushAccumulator(ctx context.Context, acc *OrgInstanceAccumulator, tmpDir string, ingestDateint int32, rpfEstimate int64) ([]parquetwriter.Result, error) {
	if len(acc.readers) == 0 {
		return nil, nil
	}

	// Create unified reader from accumulated sorted readers
	keyProvider := metricsprocessing.GetCurrentMetricSortKeyProvider()
	mergeReader, err := filereader.NewMergesortReader(ctx, acc.readers, keyProvider, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to create mergesort reader: %w", err)
	}

	// Add aggregation
	var finalReader filereader.Reader
	finalReader, err = filereader.NewAggregatingMetricsReader(mergeReader, 10000, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregating reader: %w", err)
	}

	// Create or reuse writer manager for this (org, instance)
	if acc.writerManager == nil {
		acc.writerManager = newMetricWriterManager(
			ctx,
			tmpDir,
			acc.key.OrganizationID.String(),
			ingestDateint,
			rpfEstimate,
		)
	}

	// Process all rows through the writer manager
	for {
		batch, readErr := finalReader.Next(ctx)
		if readErr != nil && readErr != io.EOF {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			return nil, fmt.Errorf("failed to read from unified pipeline: %w", readErr)
		}

		if batch != nil {
			acc.writerManager.processBatch(ctx, batch)
			pipeline.ReturnBatch(batch)
		}

		if readErr == io.EOF {
			break
		}
	}

	// Close writers and get results
	results, err := acc.writerManager.closeAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to close writers: %w", err)
	}

	return results, nil
}
