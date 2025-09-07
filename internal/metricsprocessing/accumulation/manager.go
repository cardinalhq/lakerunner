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

package accumulation

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// Manager manages multiple accumulators for a specific strategy
type Manager struct {
	strategy            Strategy
	store               Store
	accumulators        map[AccumulationKey]*Accumulator
	maxAccumulationTime time.Duration
	globalStartTime     time.Time
	kafkaOffsets        map[int32]int64 // Track highest offset per partition
	mu                  sync.Mutex
	tmpDir              string
	totalWorkCount      int
	targetFileSizeBytes int64
}

// NewManager creates a new accumulation manager
func NewManager(strategy Strategy, store Store, maxAccumulationTime time.Duration, targetFileSizeBytes int64) (*Manager, error) {
	// Create temp directory for this accumulation cycle
	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("%s-", strategy.GetConsumerGroup()))
	if err != nil {
		return nil, fmt.Errorf("creating tmpdir: %w", err)
	}

	// Check disk space and warn if less than 80% free
	checkDiskSpaceWarning(tmpDir)

	return &Manager{
		strategy:            strategy,
		store:               store,
		accumulators:        make(map[AccumulationKey]*Accumulator),
		maxAccumulationTime: maxAccumulationTime,
		globalStartTime:     time.Now(),
		kafkaOffsets:        make(map[int32]int64),
		tmpDir:              tmpDir,
		targetFileSizeBytes: targetFileSizeBytes,
	}, nil
}

// AddWorkFromKafka processes a Kafka message and adds it to the accumulator
func (m *Manager) AddWorkFromKafka(
	ctx context.Context,
	notification *messages.MetricSegmentNotificationMessage,
	sp storageprofile.StorageProfileProvider,
	kafkaOffset *lrdb.KafkaOffsetUpdate,
) error {
	ll := logctx.FromContext(ctx)

	// Check if strategy wants to process this message
	if !m.strategy.ShouldProcess(notification) {
		ll.Debug("Skipping message based on strategy",
			slog.Int("frequencyMs", int(notification.FrequencyMs)))
		return nil
	}

	// Get storage profile
	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, notification.OrganizationID, notification.InstanceNum)
	if err != nil {
		return fmt.Errorf("failed to get storage profile: %w", err)
	}

	// Get source segments using strategy
	segments, err := m.strategy.GetSourceSegments(ctx, m.store, notification)
	if err != nil {
		return fmt.Errorf("failed to get source segments: %w", err)
	}

	if len(segments) == 0 {
		ll.Warn("No segments found for notification",
			slog.Int64("segmentID", notification.SegmentID))
		return nil
	}

	// Check if already optimized (for compaction mainly)
	rpfEstimate := m.store.GetMetricEstimate(ctx, notification.OrganizationID, notification.FrequencyMs)
	if m.strategy.IsAlreadyOptimized(notification, segments[0], rpfEstimate, m.targetFileSizeBytes) {
		ll.Info("Segment already optimized, skipping",
			slog.Int64("segmentID", notification.SegmentID))
		return nil
	}

	// Get accumulation key from strategy
	key := m.strategy.GetAccumulationKey(notification)

	// Add to accumulator
	m.addWork(ctx, key, segments, profile, kafkaOffset)

	ll.Debug("Added work to accumulator",
		slog.Int64("segmentID", notification.SegmentID),
		slog.String("organizationID", notification.OrganizationID.String()))

	return nil
}

// addWork adds work to the appropriate accumulator
func (m *Manager) addWork(
	ctx context.Context,
	key AccumulationKey,
	segments []lrdb.MetricSeg,
	profile storageprofile.StorageProfile,
	kafkaOffset *lrdb.KafkaOffsetUpdate,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	acc := m.getOrCreateAccumulatorLocked(ctx, key)

	work := AccumulationWork{
		Segments: segments,
		Key:      key,
		Profile:  profile,
	}

	acc.AddWork(work)
	m.totalWorkCount++

	// Track highest offset per partition
	if kafkaOffset != nil {
		if currentOffset, exists := m.kafkaOffsets[kafkaOffset.Partition]; !exists || kafkaOffset.Offset > currentOffset {
			m.kafkaOffsets[kafkaOffset.Partition] = kafkaOffset.Offset
		}
	}
}

// getOrCreateAccumulatorLocked gets or creates an accumulator (must be called with lock held)
func (m *Manager) getOrCreateAccumulatorLocked(ctx context.Context, key AccumulationKey) *Accumulator {
	if acc, exists := m.accumulators[key]; exists {
		return acc
	}

	rpfEstimate := m.store.GetMetricEstimate(ctx, key.OrganizationID, key.FrequencyMs)
	acc := NewAccumulator(key, rpfEstimate)
	m.accumulators[key] = acc
	return acc
}

// ShouldFlush determines if accumulators should be flushed
func (m *Manager) ShouldFlush() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if time.Since(m.globalStartTime) >= m.maxAccumulationTime {
		return true
	}

	if m.isDiskSpaceLow() {
		return true
	}

	// Could add additional conditions here
	return false
}

// isDiskSpaceLow checks if disk space is more than 50% full
func (m *Manager) isDiskSpaceLow() bool {
	usage, err := helpers.DiskUsage(m.tmpDir)
	if err != nil {
		logctx.FromContext(context.Background()).Warn("Failed to check disk usage",
			slog.String("tmpDir", m.tmpDir),
			slog.Any("error", err))
		return false
	}

	usedPercent := float64(usage.UsedBytes) / float64(usage.TotalBytes) * 100
	if usedPercent > 50 {
		logctx.FromContext(context.Background()).Warn("Disk space is more than 50% full, triggering flush",
			slog.Float64("usedPercent", usedPercent),
			slog.Uint64("freeBytes", usage.FreeBytes),
			slog.Uint64("totalBytes", usage.TotalBytes))
		return true
	}

	return false
}

// GetAccumulators returns all accumulators
func (m *Manager) GetAccumulators() map[AccumulationKey]*Accumulator {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.accumulators
}

// GetKafkaOffsetUpdates returns Kafka offset updates for all partitions
func (m *Manager) GetKafkaOffsetUpdates(consumerGroup, topic string) []lrdb.KafkaOffsetUpdate {
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

// GetTmpDir returns the temp directory for this accumulation cycle
func (m *Manager) GetTmpDir() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tmpDir
}

// HasWork returns true if there's any work to flush
func (m *Manager) HasWork() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalWorkCount > 0
}

// Reset resets the manager for a new accumulation window
func (m *Manager) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.tmpDir != "" {
		if err := os.RemoveAll(m.tmpDir); err != nil {
			return fmt.Errorf("removing old tmpdir: %w", err)
		}
	}

	tmpDir, err := os.MkdirTemp("", fmt.Sprintf("%s-", m.strategy.GetConsumerGroup()))
	if err != nil {
		return fmt.Errorf("creating new tmpdir: %w", err)
	}

	checkDiskSpaceWarning(tmpDir)

	m.accumulators = make(map[AccumulationKey]*Accumulator)
	m.globalStartTime = time.Now()
	m.kafkaOffsets = make(map[int32]int64)
	m.totalWorkCount = 0
	m.tmpDir = tmpDir

	return nil
}

// Close closes the manager and cleans up resources
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.tmpDir != "" {
		if err := os.RemoveAll(m.tmpDir); err != nil {
			return err
		}
	}

	return nil
}

// checkDiskSpaceWarning logs a warning if disk space is less than 80% free
func checkDiskSpaceWarning(tmpDir string) {
	usage, err := helpers.DiskUsage(tmpDir)
	if err != nil {
		logctx.FromContext(context.Background()).Warn("Failed to check disk usage at startup",
			slog.String("tmpDir", tmpDir),
			slog.Any("error", err))
		return
	}

	freePercent := float64(usage.FreeBytes) / float64(usage.TotalBytes) * 100
	if freePercent < 80 {
		logctx.FromContext(context.Background()).Warn("Disk space is less than 80% free at startup - cleanup may be needed",
			slog.Float64("freePercent", freePercent),
			slog.Uint64("freeBytes", usage.FreeBytes),
			slog.Uint64("totalBytes", usage.TotalBytes),
			slog.String("tmpDir", tmpDir))
	}
}
