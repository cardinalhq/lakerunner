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
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// Accumulator accumulates work for a specific key
type Accumulator struct {
	key           AccumulationKey
	work          []AccumulationWork
	writerManager *WriterManager
	rpfEstimate   int64
	startTime     time.Time
	mu            sync.Mutex
}

// NewAccumulator creates a new accumulator for a key
func NewAccumulator(key AccumulationKey, rpfEstimate int64) *Accumulator {
	return &Accumulator{
		key:         key,
		work:        make([]AccumulationWork, 0),
		rpfEstimate: rpfEstimate,
		startTime:   time.Now(),
	}
}

// AddWork adds work to the accumulator
func (a *Accumulator) AddWork(work AccumulationWork) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.work = append(a.work, work)
}

// GetWork returns a copy of all accumulated work
func (a *Accumulator) GetWork() []AccumulationWork {
	a.mu.Lock()
	defer a.mu.Unlock()
	result := make([]AccumulationWork, len(a.work))
	copy(result, a.work)
	return result
}

// FlushAccumulator processes a single accumulator and performs the operation
func FlushAccumulator(
	ctx context.Context,
	acc *Accumulator,
	db Store,
	blobclient cloudstorage.Client,
	tmpDir string,
	strategy Strategy,
) error {
	work := acc.GetWork()
	if len(work) == 0 {
		return nil
	}

	// Create writer manager if needed (will be nil after previous flush)
	if acc.writerManager == nil {
		acc.writerManager = NewWriterManager(tmpDir, acc.rpfEstimate)
	}

	// Process all work items
	for _, w := range work {
		// Create reader stack from segments
		readerStack, err := metricsprocessing.CreateReaderStack(ctx, tmpDir, blobclient, w.Key.OrganizationID, w.Profile, w.Segments)
		if err != nil {
			return fmt.Errorf("creating reader stack: %w", err)
		}
		defer metricsprocessing.CloseReaderStack(ctx, readerStack)

		// Get target frequency from strategy
		targetFrequency := strategy.GetTargetFrequency(w.Key)

		// Process through writer manager
		if err := acc.writerManager.ProcessReaders(ctx, readerStack.Readers, w.Key, targetFrequency); err != nil {
			return fmt.Errorf("processing readers: %w", err)
		}
	}

	// Flush all writers and get results
	result, err := acc.writerManager.FlushAll(ctx)
	if err != nil {
		// Clear writer manager even on error to prevent reuse
		acc.writerManager = nil
		return fmt.Errorf("flushing writers: %w", err)
	}

	// Clear writer manager before upload to prevent reuse even if upload fails
	acc.writerManager = nil

	// Upload and update database
	if err := uploadAndUpdateDatabase(ctx, db, blobclient, acc.key, work, result, strategy); err != nil {
		return fmt.Errorf("uploading and updating database: %w", err)
	}

	// Clear the work after successful processing
	acc.mu.Lock()
	acc.work = nil
	acc.mu.Unlock()

	return nil
}

// uploadAndUpdateDatabase handles uploading segments and database updates
func uploadAndUpdateDatabase(
	ctx context.Context,
	db Store,
	blobclient cloudstorage.Client,
	key AccumulationKey,
	work []AccumulationWork,
	result metricsprocessing.ProcessingResult,
	strategy Strategy,
) error {
	ll := logctx.FromContext(ctx)

	if len(result.RawResults) == 0 {
		ll.Warn("No output files from processing",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("dateint", int(key.Dateint)))
		return nil
	}

	// Collect all old segments
	var oldRecords []lrdb.MetricSeg
	for _, w := range work {
		oldRecords = append(oldRecords, w.Segments...)
	}

	// Get profile from first work item (all should have same profile for this key)
	profile := work[0].Profile

	// Upload segments
	segments, err := metricsprocessing.CreateSegmentsFromResults(ctx, result.RawResults, key.OrganizationID, profile.CollectorName)
	if err != nil {
		return fmt.Errorf("creating segments: %w", err)
	}

	uploadedSegments, err := metricsprocessing.UploadSegments(ctx, blobclient, profile.Bucket, segments)
	if err != nil {
		if len(uploadedSegments) > 0 {
			ll.Warn("S3 upload failed partway through, scheduling cleanup",
				slog.Int("uploadedFiles", len(uploadedSegments)))
			// Note: We might want to add cleanup scheduling here
		}
		return fmt.Errorf("uploading segments: %w", err)
	}

	// Prepare new records
	newRecords := make([]lrdb.MetricSeg, 0, len(uploadedSegments))
	for _, segment := range uploadedSegments {
		// Create minimal MetricSeg with required fields for the strategy
		newRecords = append(newRecords, lrdb.MetricSeg{
			OrganizationID: key.OrganizationID,
			Dateint:        key.Dateint,
			InstanceNum:    key.InstanceNum,
			FrequencyMs:    key.FrequencyMs,
			SegmentID:      segment.SegmentID,
			RecordCount:    segment.Result.RecordCount,
			FileSize:       segment.Result.FileSize,
			TsRange: pgtype.Range[pgtype.Int8]{
				LowerType: pgtype.Inclusive,
				UpperType: pgtype.Exclusive,
				Lower:     pgtype.Int8{Int64: segment.StartTs, Valid: true},
				Upper:     pgtype.Int8{Int64: segment.EndTs, Valid: true},
				Valid:     true,
			},
			Fingerprints: segment.Fingerprints,
		})
	}

	// Use strategy to mark segments as processed
	params := ProcessedParams{
		Key:        key,
		OldRecords: oldRecords,
		NewRecords: newRecords,
		Profile:    profile,
	}

	if err := strategy.MarkSegmentsProcessed(ctx, db, params); err != nil {
		return fmt.Errorf("marking segments processed: %w", err)
	}

	// Calculate segment reduction
	var segmentReduction float64
	if len(oldRecords) > 0 {
		segmentReduction = 1.0 - (float64(len(newRecords)) / float64(len(oldRecords)))
	}

	ll.Info("Processing complete",
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("dateint", int(key.Dateint)),
		slog.Int("frequencyMs", int(key.FrequencyMs)),
		slog.Int("inputSegments", len(oldRecords)),
		slog.Int("outputSegments", len(newRecords)),
		slog.Float64("segmentReduction", segmentReduction))

	return nil
}
