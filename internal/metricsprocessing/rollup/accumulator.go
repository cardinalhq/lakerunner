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

package rollup

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// RollupKey uniquely identifies a rollup group
type RollupKey struct {
	OrganizationID  uuid.UUID
	Dateint         int32
	SourceFrequency int32
	InstanceNum     int16
	SlotID          int32
	SlotCount       int32
}

// RollupWork represents work to be rolled up
type RollupWork struct {
	Segment lrdb.MetricSeg
	Profile storageprofile.StorageProfile
}

// RollupAccumulator accumulates rollup work for a specific key
type RollupAccumulator struct {
	key           RollupKey
	work          []RollupWork
	writerManager *RollupWriterManager
	rpfEstimate   int64 // Cached RPF estimate for target frequency
	startTime     time.Time
	mu            sync.Mutex
}

// NewRollupAccumulator creates a new accumulator for a specific rollup key
func NewRollupAccumulator(key RollupKey, rpfEstimate int64) *RollupAccumulator {
	return &RollupAccumulator{
		key:         key,
		work:        make([]RollupWork, 0),
		rpfEstimate: rpfEstimate,
		startTime:   time.Now(),
	}
}

// AddWork adds rollup work to this accumulator
func (a *RollupAccumulator) AddWork(segment lrdb.MetricSeg, profile storageprofile.StorageProfile) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.work = append(a.work, RollupWork{
		Segment: segment,
		Profile: profile,
	})
}

// GetWork returns all accumulated work
func (a *RollupAccumulator) GetWork() []RollupWork {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.work
}

// ShouldFlush checks if this accumulator should be flushed based on time
func (a *RollupAccumulator) ShouldFlush() bool {
	maxDuration := config.GetRollupAccumulationTime(a.key.SourceFrequency)
	return time.Since(a.startTime) > maxDuration
}

// RollupManager manages all rollup accumulators
type RollupManager struct {
	accumulators       map[RollupKey]*RollupAccumulator
	kafkaOffsetUpdates map[string]*lrdb.KafkaOffsetUpdate // key: "partition:offset"
	tmpDir             string
	startTime          time.Time
	mu                 sync.RWMutex
	db                 rollupStore
}

// NewRollupManager creates a new rollup manager
func NewRollupManager(db rollupStore) (*RollupManager, error) {
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %w", err)
	}

	// Check disk space warning threshold (80%)
	if err := checkDiskSpaceWarning(tmpDir); err != nil {
		logctx.FromContext(context.Background()).Warn("Disk space warning during rollup manager creation",
			slog.Any("error", err))
	}

	return &RollupManager{
		accumulators:       make(map[RollupKey]*RollupAccumulator),
		kafkaOffsetUpdates: make(map[string]*lrdb.KafkaOffsetUpdate),
		tmpDir:             tmpDir,
		startTime:          time.Now(),
		db:                 db,
	}, nil
}

// AddRollupWork adds work to the appropriate accumulator
func (m *RollupManager) AddRollupWork(
	ctx context.Context,
	segment lrdb.MetricSeg,
	profile storageprofile.StorageProfile,
	key RollupKey,
	kafkaOffset *lrdb.KafkaOffsetUpdate,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Get or create accumulator for this key
	acc, exists := m.accumulators[key]
	if !exists {
		// Get target frequency for this rollup
		targetFrequency, ok := config.GetTargetRollupFrequency(key.SourceFrequency)
		if !ok {
			ll := logctx.FromContext(ctx)
			ll.Error("Invalid source frequency for rollup",
				slog.Int("frequencyMs", int(key.SourceFrequency)))
			return
		}

		// Fetch RPF estimate for the target frequency
		rpfEstimate := m.db.GetMetricEstimate(ctx, key.OrganizationID, targetFrequency)

		acc = NewRollupAccumulator(key, rpfEstimate)
		m.accumulators[key] = acc
	}

	// Add work to accumulator
	acc.AddWork(segment, profile)

	// Track Kafka offset
	if kafkaOffset != nil {
		offsetKey := fmt.Sprintf("%d:%d", kafkaOffset.Partition, kafkaOffset.Offset)
		m.kafkaOffsetUpdates[offsetKey] = kafkaOffset
	}

	ll := logctx.FromContext(ctx)
	ll.Debug("Added rollup work to accumulator",
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("dateint", int(key.Dateint)),
		slog.Int("frequencyMs", int(key.SourceFrequency)),
		slog.Int("slotID", int(key.SlotID)),
		slog.Int("workCount", len(acc.work)))
}

// GetAccumulators returns all accumulators
func (m *RollupManager) GetAccumulators() map[RollupKey]*RollupAccumulator {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.accumulators
}

// GetKafkaOffsetUpdates returns all Kafka offset updates
func (m *RollupManager) GetKafkaOffsetUpdates(consumerGroup, topic string) []lrdb.KafkaOffsetUpdate {
	m.mu.RLock()
	defer m.mu.RUnlock()

	updates := make([]lrdb.KafkaOffsetUpdate, 0, len(m.kafkaOffsetUpdates))
	for _, update := range m.kafkaOffsetUpdates {
		// Update consumer group and topic to match current consumer
		update.ConsumerGroup = consumerGroup
		update.Topic = topic
		updates = append(updates, *update)
	}
	return updates
}

// HasWork checks if there's any accumulated work
func (m *RollupManager) HasWork() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.accumulators) > 0
}

// ShouldFlush checks if any accumulator should be flushed or disk space is low
func (m *RollupManager) ShouldFlush() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Check disk space (50% threshold)
	if isDiskSpaceLow(m.tmpDir) {
		return true
	}

	// Check if any accumulator has reached its time limit
	for _, acc := range m.accumulators {
		if acc.ShouldFlush() {
			return true
		}
	}

	return false
}

// GetTmpDir returns the temporary directory path
func (m *RollupManager) GetTmpDir() string {
	return m.tmpDir
}

// Reset clears all accumulators and creates a new temp directory
func (m *RollupManager) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clean up old temp directory
	if m.tmpDir != "" {
		if err := os.RemoveAll(m.tmpDir); err != nil {
			logctx.FromContext(context.Background()).Warn("Failed to remove old temp directory",
				slog.String("tmpDir", m.tmpDir),
				slog.Any("error", err))
		}
	}

	// Create new temp directory
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("failed to create new temp directory: %w", err)
	}

	// Check disk space warning threshold
	if err := checkDiskSpaceWarning(tmpDir); err != nil {
		logctx.FromContext(context.Background()).Warn("Disk space warning after reset",
			slog.Any("error", err))
	}

	m.tmpDir = tmpDir
	m.accumulators = make(map[RollupKey]*RollupAccumulator)
	m.kafkaOffsetUpdates = make(map[string]*lrdb.KafkaOffsetUpdate)
	m.startTime = time.Now()

	return nil
}

// Close cleans up resources
func (m *RollupManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.tmpDir != "" {
		return os.RemoveAll(m.tmpDir)
	}
	return nil
}

// isDiskSpaceLow checks if disk usage is above 50%
func isDiskSpaceLow(tmpDir string) bool {
	usage, err := helpers.DiskUsage(tmpDir)
	if err != nil {
		return false
	}

	usedPercent := float64(usage.UsedBytes) / float64(usage.TotalBytes) * 100
	if usedPercent > 50 {
		logctx.FromContext(context.Background()).Warn("Disk space is more than 50% full, triggering flush",
			slog.Float64("usedPercent", usedPercent))
		return true
	}
	return false
}

// checkDiskSpaceWarning checks if disk usage is above 80% and logs a warning
func checkDiskSpaceWarning(tmpDir string) error {
	usage, err := helpers.DiskUsage(tmpDir)
	if err != nil {
		return fmt.Errorf("failed to check disk usage: %w", err)
	}

	usedPercent := float64(usage.UsedBytes) / float64(usage.TotalBytes) * 100
	if usedPercent > 80 {
		return fmt.Errorf("disk space critically low: %.1f%% used", usedPercent)
	}
	return nil
}
