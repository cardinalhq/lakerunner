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

package compaction

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// CompactionKey uniquely identifies a compaction group
type CompactionKey struct {
	OrganizationID uuid.UUID
	Dateint        int32
	FrequencyMs    int32
	InstanceNum    int16
}

// CompactionWork represents work to be compacted
type CompactionWork struct {
	Segments []lrdb.MetricSeg
	Metadata CompactionWorkMetadata
	Profile  storageprofile.StorageProfile
}

// CompactionAccumulator accumulates compaction work for a specific key
type CompactionAccumulator struct {
	key           CompactionKey
	work          []CompactionWork
	writerManager *CompactionWriterManager
	rpfEstimate   int64 // Cached RPF estimate for this key
	startTime     time.Time
	mu            sync.Mutex
}

// NewCompactionAccumulator creates a new accumulator for a compaction key
func NewCompactionAccumulator(key CompactionKey, rpfEstimate int64) *CompactionAccumulator {
	return &CompactionAccumulator{
		key:         key,
		work:        make([]CompactionWork, 0),
		rpfEstimate: rpfEstimate,
		startTime:   time.Now(),
	}
}

// AddWork adds compaction work to the accumulator
func (a *CompactionAccumulator) AddWork(work CompactionWork) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.work = append(a.work, work)
}

// GetWork returns all accumulated work
func (a *CompactionAccumulator) GetWork() []CompactionWork {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.work
}

// CompactionManager manages multiple compaction accumulators and temp directories
type CompactionManager struct {
	accumulators        map[CompactionKey]*CompactionAccumulator
	maxAccumulationTime time.Duration
	globalStartTime     time.Time
	kafkaOffsets        map[int32]int64 // Track highest offset per partition
	mu                  sync.Mutex
	tmpDir              string // Temp directory for this accumulation cycle
	totalWorkCount      int
	db                  CompactionStore // For fetching RPF estimates
}

// NewCompactionManager creates a new compaction manager
func NewCompactionManager(maxAccumulationTime time.Duration, db CompactionStore) (*CompactionManager, error) {
	// Create temp directory for this accumulation cycle
	tmpDir, err := os.MkdirTemp("", "compaction-")
	if err != nil {
		return nil, fmt.Errorf("creating compaction tmpdir: %w", err)
	}

	// Check disk space and warn if less than 80% free (more than 20% used)
	checkDiskSpaceWarning(tmpDir)

	return &CompactionManager{
		accumulators:        make(map[CompactionKey]*CompactionAccumulator),
		maxAccumulationTime: maxAccumulationTime,
		globalStartTime:     time.Now(),
		kafkaOffsets:        make(map[int32]int64),
		tmpDir:              tmpDir,
		db:                  db,
	}, nil
}

// GetOrCreateAccumulator gets or creates an accumulator for the given key
func (m *CompactionManager) GetOrCreateAccumulator(ctx context.Context, key CompactionKey) *CompactionAccumulator {
	m.mu.Lock()
	defer m.mu.Unlock()

	if acc, exists := m.accumulators[key]; exists {
		return acc
	}

	// Fetch RPF estimate once for this accumulator
	rpfEstimate := m.db.GetMetricEstimate(ctx, key.OrganizationID, key.FrequencyMs)

	acc := NewCompactionAccumulator(key, rpfEstimate)
	m.accumulators[key] = acc
	return acc
}

// AddCompactionWork adds compaction work from a Kafka message
func (m *CompactionManager) AddCompactionWork(
	ctx context.Context,
	notification *messages.MetricSegmentNotificationMessage,
	segments []lrdb.MetricSeg,
	profile storageprofile.StorageProfile,
	kafkaOffset *lrdb.KafkaOffsetUpdate,
) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := CompactionKey{
		OrganizationID: notification.OrganizationID,
		Dateint:        notification.DateInt,
		FrequencyMs:    notification.FrequencyMs,
		InstanceNum:    notification.InstanceNum,
	}

	acc := m.getOrCreateAccumulatorLocked(ctx, key)

	// Get IngestDateint from first segment (all segments should have same IngestDateint)
	ingestDateint := int32(0)
	if len(segments) > 0 {
		ingestDateint = segments[0].IngestDateint
	}

	work := CompactionWork{
		Segments: segments,
		Metadata: CompactionWorkMetadata{
			OrganizationID: notification.OrganizationID,
			Dateint:        notification.DateInt,
			FrequencyMs:    notification.FrequencyMs,
			InstanceNum:    notification.InstanceNum,
			IngestDateint:  ingestDateint,
		},
		Profile: profile,
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
func (m *CompactionManager) getOrCreateAccumulatorLocked(ctx context.Context, key CompactionKey) *CompactionAccumulator {
	if acc, exists := m.accumulators[key]; exists {
		return acc
	}

	// Fetch RPF estimate once for this accumulator
	rpfEstimate := m.db.GetMetricEstimate(ctx, key.OrganizationID, key.FrequencyMs)

	acc := NewCompactionAccumulator(key, rpfEstimate)
	m.accumulators[key] = acc
	return acc
}

// ShouldFlush determines if accumulators should be flushed
func (m *CompactionManager) ShouldFlush() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Flush if time window exceeded
	if time.Since(m.globalStartTime) >= m.maxAccumulationTime {
		return true
	}

	// Check disk space - flush if more than 50% full
	if m.isDiskSpaceLow() {
		return true
	}

	// Could add additional conditions here (memory pressure, work count, etc.)
	return false
}

// isDiskSpaceLow checks if disk space is more than 50% full
func (m *CompactionManager) isDiskSpaceLow() bool {
	usage, err := helpers.DiskUsage(m.tmpDir)
	if err != nil {
		// If we can't check disk space, assume it's OK and log warning
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
func (m *CompactionManager) GetAccumulators() map[CompactionKey]*CompactionAccumulator {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.accumulators
}

// GetKafkaOffsetUpdates returns Kafka offset updates for all partitions
func (m *CompactionManager) GetKafkaOffsetUpdates(consumerGroup, topic string) []lrdb.KafkaOffsetUpdate {
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
func (m *CompactionManager) GetTmpDir() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.tmpDir
}

// HasWork returns true if there's any work to flush
func (m *CompactionManager) HasWork() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.totalWorkCount > 0
}

// Reset resets the manager for a new accumulation window
func (m *CompactionManager) Reset() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clean up old temp directory
	if m.tmpDir != "" {
		if err := os.RemoveAll(m.tmpDir); err != nil {
			return fmt.Errorf("removing old tmpdir: %w", err)
		}
	}

	// Create new temp directory for next accumulation cycle
	tmpDir, err := os.MkdirTemp("", "compaction-")
	if err != nil {
		return fmt.Errorf("creating new tmpdir: %w", err)
	}

	// Check disk space and warn if less than 80% free
	checkDiskSpaceWarning(tmpDir)

	// Reset state
	m.accumulators = make(map[CompactionKey]*CompactionAccumulator)
	m.globalStartTime = time.Now()
	m.kafkaOffsets = make(map[int32]int64)
	m.totalWorkCount = 0
	m.tmpDir = tmpDir

	return nil
}

// Close closes the manager and cleans up resources
func (m *CompactionManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Clean up temp directory
	if m.tmpDir != "" {
		if err := os.RemoveAll(m.tmpDir); err != nil {
			return err
		}
	}

	return nil
}

// FlushAccumulator processes a single accumulator and performs compaction
func FlushAccumulator(
	ctx context.Context,
	acc *CompactionAccumulator,
	db CompactionStore,
	blobclient cloudstorage.Client,
	tmpDir string,
) error {
	ll := logctx.FromContext(ctx)

	work := acc.GetWork()
	if len(work) == 0 {
		return nil
	}

	// Initialize writer manager if needed (using cached RPF estimate)
	if acc.writerManager == nil {
		acc.writerManager = NewCompactionWriterManager(tmpDir, acc.rpfEstimate)
	}

	// Process each work item
	for _, w := range work {
		ll.Debug("Processing compaction work",
			slog.String("organizationID", w.Metadata.OrganizationID.String()),
			slog.Int("segmentCount", len(w.Segments)),
			slog.Int("dateint", int(w.Metadata.Dateint)),
			slog.Int("frequencyMs", int(w.Metadata.FrequencyMs)))

		// Create reader stack from segments
		readerStack, err := metricsprocessing.CreateReaderStack(ctx, tmpDir, blobclient, w.Metadata.OrganizationID, w.Profile, w.Segments)
		if err != nil {
			return fmt.Errorf("creating reader stack: %w", err)
		}
		defer metricsprocessing.CloseReaderStack(ctx, readerStack)

		// Process through writer manager
		if err := acc.writerManager.ProcessReaders(ctx, readerStack.Readers, w.Metadata); err != nil {
			return fmt.Errorf("processing readers: %w", err)
		}
	}

	// Flush all writers and get results
	results, err := acc.writerManager.FlushAll(ctx)
	if err != nil {
		return fmt.Errorf("flushing writers: %w", err)
	}

	// Upload results and update database
	if err := uploadAndUpdateDatabase(ctx, db, blobclient, acc.key, work, results); err != nil {
		return fmt.Errorf("uploading and updating database: %w", err)
	}

	return nil
}

// uploadAndUpdateDatabase handles uploading segments and database updates
func uploadAndUpdateDatabase(
	ctx context.Context,
	db CompactionStore,
	blobclient cloudstorage.Client,
	key CompactionKey,
	work []CompactionWork,
	results []metricsprocessing.ProcessingResult,
) error {
	ll := logctx.FromContext(ctx)

	if len(results) == 0 {
		ll.Warn("No output files from compaction",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.Dateint)))
		return nil
	}

	// Collect all old segments
	var oldRecords []lrdb.CompactMetricSegsOld
	for _, w := range work {
		for _, seg := range w.Segments {
			oldRecords = append(oldRecords, lrdb.CompactMetricSegsOld{
				SegmentID: seg.SegmentID,
				SlotID:    seg.SlotID,
			})
		}
	}

	// Get profile from first work item (all should have same profile for this key)
	profile := work[0].Profile

	// Upload segments
	segments, err := metricsprocessing.CreateSegmentsFromResults(ctx, results[0].RawResults, key.OrganizationID, profile.CollectorName)
	if err != nil {
		return fmt.Errorf("creating segments: %w", err)
	}

	uploadedSegments, err := metricsprocessing.UploadSegments(ctx, blobclient, profile.Bucket, segments)
	if err != nil {
		if len(uploadedSegments) > 0 {
			ll.Warn("S3 upload failed partway through, scheduling cleanup",
				slog.Int("uploadedFiles", len(uploadedSegments)))
			uploadedSegments.ScheduleCleanupAll(ctx, db, key.OrganizationID, key.InstanceNum, profile.Bucket)
		}
		return fmt.Errorf("uploading segments: %w", err)
	}

	// Prepare new records
	newRecords := make([]lrdb.CompactMetricSegsNew, len(uploadedSegments))
	for i, segment := range uploadedSegments {
		newRecords[i] = lrdb.CompactMetricSegsNew{
			SegmentID:    segment.SegmentID,
			StartTs:      segment.StartTs,
			EndTs:        segment.EndTs,
			RecordCount:  segment.Result.RecordCount,
			FileSize:     segment.Result.FileSize,
			Fingerprints: segment.Fingerprints,
		}
	}

	// Update database atomically
	err = db.CompactMetricSegs(ctx, lrdb.CompactMetricSegsParams{
		OrganizationID: key.OrganizationID,
		Dateint:        key.Dateint,
		InstanceNum:    key.InstanceNum,
		SlotID:         0, // compaction is terminal, single slot
		SlotCount:      1,
		IngestDateint:  work[0].Metadata.IngestDateint,
		FrequencyMs:    key.FrequencyMs,
		CreatedBy:      lrdb.CreatedByCompact,
		OldRecords:     oldRecords,
		NewRecords:     newRecords,
	})

	if err != nil {
		return fmt.Errorf("updating database: %w", err)
	}

	ll.Info("Compaction complete",
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("dateint", int(key.Dateint)),
		slog.Int("inputSegments", len(oldRecords)),
		slog.Int("outputSegments", len(newRecords)))

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
