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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type workerInterruptedError struct {
	Reason string
}

func (e *workerInterruptedError) Error() string {
	return fmt.Sprintf("worker interrupted: %s", e.Reason)
}

func newWorkerInterrupted(reason string) error {
	return &workerInterruptedError{Reason: reason}
}

func RunLoop(
	ctx context.Context,
	manager *Manager,
	mdb compactionStore,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
) error {
	ll := slog.Default().With(slog.String("component", "metric-compaction-loop"))

	for {
		select {
		case <-ctx.Done():
			ll.Info("Shutdown signal received, stopping compaction loop")
			return nil
		default:
		}

		claimedWork, err := manager.ClaimWork(ctx)
		if err != nil {
			ll.Error("Failed to claim work", slog.Any("error", err))
			time.Sleep(2 * time.Second)
			continue
		}

		if len(claimedWork) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		err = processBatch(ctx, ll, mdb, sp, awsmanager, claimedWork)
		if err != nil {
			ll.Error("Failed to process compaction batch", slog.Any("error", err))
			if failErr := manager.FailWork(ctx, claimedWork); failErr != nil {
				ll.Error("Failed to fail work items", slog.Any("error", failErr))
			}
		} else {
			if completeErr := manager.CompleteWork(ctx, claimedWork); completeErr != nil {
				ll.Error("Failed to complete work items", slog.Any("error", completeErr))
			}
		}
	}
}

func processBatch(
	ctx context.Context,
	ll *slog.Logger,
	mdb compactionStore,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
	claimedWork []lrdb.ClaimMetricCompactionWorkRow,
) error {
	if len(claimedWork) == 0 {
		return nil
	}

	// Safety check: All work items in a batch must have identical grouping fields
	firstItem := claimedWork[0]
	for i, item := range claimedWork {
		if item.OrganizationID != firstItem.OrganizationID ||
			item.Dateint != firstItem.Dateint ||
			item.FrequencyMs != firstItem.FrequencyMs ||
			item.InstanceNum != firstItem.InstanceNum {
			ll.Error("Inconsistent work batch detected - all items must have same org/dateint/frequency/instance",
				slog.Int("itemIndex", i),
				slog.String("expectedOrg", firstItem.OrganizationID.String()),
				slog.String("actualOrg", item.OrganizationID.String()),
				slog.Int("expectedDateint", int(firstItem.Dateint)),
				slog.Int("actualDateint", int(item.Dateint)),
				slog.Int64("expectedFreq", firstItem.FrequencyMs),
				slog.Int64("actualFreq", item.FrequencyMs),
				slog.Int("expectedInstance", int(firstItem.InstanceNum)),
				slog.Int("actualInstance", int(item.InstanceNum)))
			return fmt.Errorf("inconsistent work batch: item %d has different grouping fields", i)
		}
	}

	if !helpers.IsWantedFrequency(int32(firstItem.FrequencyMs)) {
		ll.Debug("Skipping compaction for unwanted frequency", slog.Int64("frequencyMs", firstItem.FrequencyMs))
		return nil
	}

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, firstItem.OrganizationID, firstItem.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return err
	}

	rpfEstimate := int64(40_000) // Use fixed default since estimation is now in-database

	tmpdir, err := os.MkdirTemp("", "work-")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to remove temporary directory", slog.Any("error", err))
		}
	}()

	ll.Info("Starting metric compaction batch",
		slog.String("organizationID", firstItem.OrganizationID.String()),
		slog.Int("instanceNum", int(firstItem.InstanceNum)),
		slog.Int("dateint", int(firstItem.Dateint)),
		slog.Int64("frequencyMs", firstItem.FrequencyMs),
		slog.Int("batchSize", len(claimedWork)))

	// Convert claimed work to MetricSeg format for existing processing logic
	segments, err := fetchMetricSegsForCompaction(ctx, mdb, claimedWork)
	if err != nil {
		ll.Error("Failed to fetch metric segments for compaction", slog.Any("error", err))
		return err
	}

	// Filter out any segments that are already compacted (safety check)
	validSegments := make([]lrdb.MetricSeg, 0, len(segments))
	for _, seg := range segments {
		if seg.Compacted {
			ll.Warn("Found already compacted segment in work batch - upstream issue detected",
				slog.Int64("segmentID", seg.SegmentID),
				slog.String("organizationID", seg.OrganizationID.String()),
				slog.Int("dateint", int(seg.Dateint)),
				slog.Int64("frequencyMs", int64(seg.FrequencyMs)),
				slog.Int("instanceNum", int(seg.InstanceNum)))
			continue
		}
		validSegments = append(validSegments, seg)
	}

	return compactSegments(ctx, ll, mdb, tmpdir, firstItem, profile, s3client, validSegments, rpfEstimate)
}

func compactSegments(
	ctx context.Context,
	ll *slog.Logger,
	mdb compactionStore,
	tmpdir string,
	workItem lrdb.ClaimMetricCompactionWorkRow,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rows []lrdb.MetricSeg,
	rpfEstimate int64,
) error {
	if len(rows) == 0 {
		ll.Debug("No segments to compact")
		return nil
	}

	if !shouldCompactMetrics(rows) {
		ll.Debug("No need to compact metrics in this batch", slog.Int("rowCount", len(rows)))
		return nil
	}

	// Check for context cancellation before starting compaction
	if ctx.Err() != nil {
		ll.Info("Context cancelled before starting compaction",
			slog.Int("segmentCount", len(rows)),
			slog.Any("error", ctx.Err()))
		return newWorkerInterrupted("context cancelled before compaction")
	}

	st, _, ok := helpers.RangeBounds(workItem.TsRange)
	if !ok {
		return fmt.Errorf("invalid time range in work item: %v", workItem.TsRange)
	}

	// Create counters for this package
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/metricsprocessing/compaction")
	fileSortedCounter, _ := meter.Int64Counter("lakerunner.metric.compact.file.sorted")
	commonAttributes := attribute.NewSet()

	config := metricsprocessing.ReaderStackConfig{
		FileSortedCounter: fileSortedCounter,
		CommonAttributes:  commonAttributes,
	}

	readerStack, err := metricsprocessing.CreateReaderStack(
		ctx, ll, tmpdir, s3client, workItem.OrganizationID, profile, st.Time.UTC().UnixMilli(), rows, config)
	if err != nil {
		return err
	}

	readers := readerStack.Readers
	downloadedFiles := readerStack.DownloadedFiles
	finalReader := readerStack.FinalReader

	if len(readers) == 0 {
		ll.Debug("No files to compact, skipping work item")
		return nil
	}

	defer metricsprocessing.CloseReaderStack(ll, readerStack)

	// Wrap with aggregating reader to merge duplicates during compaction
	frequencyMs := workItem.FrequencyMs
	ll.Debug("Creating aggregating metrics reader", slog.Int64("frequencyMs", frequencyMs))
	aggregatingReader, err := filereader.NewAggregatingMetricsReader(finalReader, frequencyMs, 1000)
	if err != nil {
		ll.Error("Failed to create aggregating metrics reader", slog.Any("error", err))
		return fmt.Errorf("creating aggregating metrics reader: %w", err)
	}
	defer aggregatingReader.Close()

	writer, err := factories.NewMetricsWriter(tmpdir, constants.WriterTargetSizeBytesMetrics, rpfEstimate)
	if err != nil {
		ll.Error("Failed to create metrics writer", slog.Any("error", err))
		return fmt.Errorf("creating metrics writer: %w", err)
	}
	defer writer.Abort()

	totalRows := int64(0)
	batchCount := 0

	for {
		batch, err := aggregatingReader.Next()
		batchCount++

		if err != nil && !errors.Is(err, io.EOF) {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			ll.Error("Failed to read from aggregating reader", slog.Any("error", err))
			return fmt.Errorf("reading from aggregating reader: %w", err)
		}

		if batch == nil || batch.Len() == 0 {
			pipeline.ReturnBatch(batch)
			break
		}

		// Create a new batch for normalized rows
		normalizedBatch := pipeline.GetBatch()

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if err := metricsprocessing.NormalizeRowForParquetWrite(row); err != nil {
				ll.Error("Failed to normalize row", slog.Any("error", err))
				pipeline.ReturnBatch(normalizedBatch)
				pipeline.ReturnBatch(batch)
				return fmt.Errorf("normalizing row: %w", err)
			}

			// Copy normalized row to the new batch
			normalizedRow := normalizedBatch.AddRow()
			for k, v := range row {
				normalizedRow[k] = v
			}
			totalRows++
		}

		// Write the entire normalized batch at once
		if normalizedBatch.Len() > 0 {
			if err := writer.WriteBatch(normalizedBatch); err != nil {
				ll.Error("Failed to write batch", slog.Any("error", err))
				pipeline.ReturnBatch(normalizedBatch)
				pipeline.ReturnBatch(batch)
				return fmt.Errorf("writing batch: %w", err)
			}
		}

		pipeline.ReturnBatch(normalizedBatch)
		pipeline.ReturnBatch(batch)

		if errors.Is(err, io.EOF) {
			break
		}
	}

	results, err := writer.Close(ctx)
	if err != nil {
		ll.Error("Failed to finish writing", slog.Any("error", err))
		return fmt.Errorf("finishing writer: %w", err)
	}

	// Calculate output file sizes
	outputBytes := int64(0)
	inputBytes := int64(0)
	for _, result := range results {
		outputBytes += result.FileSize
	}
	for _, row := range rows {
		inputBytes += row.FileSize
	}

	compressionRatio := float64(0)
	if inputBytes > 0 {
		compressionRatio = float64(outputBytes) / float64(inputBytes) * 100
	}

	ll.Debug("Compaction completed",
		slog.Int64("totalRows", totalRows),
		slog.Int("outputFiles", len(results)),
		slog.Int("inputFiles", len(downloadedFiles)),
		slog.Int64("recordsPerFile", rpfEstimate),
		slog.Int64("inputBytes", inputBytes),
		slog.Int64("outputBytes", outputBytes),
		slog.Float64("compressionRatio", compressionRatio))

	// If we produced 0 output files, log source S3 paths for debugging and skip database updates
	if len(results) == 0 {
		ll.Warn("Produced 0 output files from aggregating reader")
		return nil
	}

	// Final interruption check before critical section (S3 uploads + DB updates)
	if ctx.Err() != nil {
		ll.Info("Context cancelled before critical section - safe to abort",
			slog.Int("resultCount", len(results)),
			slog.Any("error", ctx.Err()))
		return newWorkerInterrupted("context cancelled before metrics upload phase")
	}

	// Use context without cancellation for critical section to ensure atomic completion
	criticalCtx := context.WithoutCancel(ctx)

	// Upload files to S3 first
	segmentIDs, err := uploadCompactedFiles(criticalCtx, ll, s3client, results, workItem, profile)
	if err != nil {
		return fmt.Errorf("failed to upload compacted files to S3: %w", err)
	}

	// Atomically replace segments in database
	err = replaceCompactedSegments(criticalCtx, ll, mdb, results, rows, workItem, segmentIDs)
	if err != nil {
		return fmt.Errorf("failed to replace compacted segments in database: %w", err)
	}

	return nil
}

func uploadCompactedFiles(
	ctx context.Context,
	ll *slog.Logger,
	s3client *awsclient.S3Client,
	results []parquetwriter.Result,
	workItem lrdb.ClaimMetricCompactionWorkRow,
	profile storageprofile.StorageProfile,
) ([]int64, error) {
	st, _, ok := helpers.RangeBounds(workItem.TsRange)
	if !ok {
		return nil, fmt.Errorf("invalid time range in work item: %v", workItem.TsRange)
	}

	segmentIDs := make([]int64, len(results))

	for i, result := range results {
		// Generate new segment ID
		segmentID := idgen.DefaultFlakeGenerator.NextID()
		segmentIDs[i] = segmentID

		dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())
		objectID := helpers.MakeDBObjectID(workItem.OrganizationID, profile.CollectorName, dateint, hour, segmentID, "metrics")

		if err := s3helper.UploadS3Object(ctx, s3client, profile.Bucket, objectID, result.FileName); err != nil {
			ll.Error("Failed to upload compacted file to S3",
				slog.String("bucket", profile.Bucket),
				slog.String("objectID", objectID),
				slog.String("fileName", result.FileName),
				slog.Any("error", err))
			return nil, fmt.Errorf("uploading file %s: %w", objectID, err)
		}

		ll.Debug("Uploaded compacted file to S3",
			slog.String("objectID", objectID),
			slog.Int64("fileSize", result.FileSize),
			slog.Int64("recordCount", result.RecordCount))
	}
	return segmentIDs, nil
}

func replaceCompactedSegments(
	ctx context.Context,
	ll *slog.Logger,
	mdb compactionStore,
	results []parquetwriter.Result,
	oldRows []lrdb.MetricSeg,
	workItem lrdb.ClaimMetricCompactionWorkRow,
	segmentIDs []int64,
) error {
	// Collect old segment IDs
	oldSegmentIds := make([]int64, len(oldRows))
	for i, row := range oldRows {
		oldSegmentIds[i] = row.SegmentID
	}

	// Prepare new segment data
	newSegmentIds := make([]int64, len(results))
	newTsRanges := make([]pgtype.Range[pgtype.Int8], len(results))
	newRecordCounts := make([]int64, len(results))
	newFileSizes := make([]int64, len(results))
	newFingerprints := make([][]int64, len(results))

	for i, result := range results {
		newSegmentIds[i] = segmentIDs[i]
		newRecordCounts[i] = result.RecordCount
		newFileSizes[i] = result.FileSize
		// Extract fingerprints from metadata
		if metadata, ok := result.Metadata.(factories.MetricsFileStats); ok {
			newFingerprints[i] = metadata.Fingerprints
		} else {
			ll.Error("Missing metadata for compacted segment - cannot proceed",
				"segment_id", segmentIDs[i],
				"organization_id", workItem.OrganizationID,
				"dateint", workItem.Dateint,
				"frequency_ms", workItem.FrequencyMs,
				"instance_num", workItem.InstanceNum)
			return fmt.Errorf("missing metadata for segment %d", segmentIDs[i])
		}

		// Convert timestamptz range to int8 millisecond range
		st, et, ok := helpers.RangeBounds(workItem.TsRange)
		if !ok {
			return fmt.Errorf("invalid time range in work item: %v", workItem.TsRange)
		}
		newTsRanges[i] = pgtype.Range[pgtype.Int8]{
			Lower:     pgtype.Int8{Int64: st.Time.UTC().UnixMilli(), Valid: true},
			Upper:     pgtype.Int8{Int64: et.Time.UTC().UnixMilli(), Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		}
	}

	params := lrdb.ReplaceCompactedMetricSegsParams{
		OrganizationID:  workItem.OrganizationID,
		Dateint:         workItem.Dateint,
		FrequencyMs:     int32(workItem.FrequencyMs),
		InstanceNum:     workItem.InstanceNum,
		IngestDateint:   metricsprocessing.GetIngestDateint(oldRows),
		CreatedBy:       lrdb.CreatedByIngest, // Use standard created_by for compacted segments
		SlotID:          0,                    // Default slot ID
		SlotCount:       1,                    // Default slot count
		SortVersion:     lrdb.CurrentMetricSortVersion,
		NewSegmentIds:   newSegmentIds,
		NewTsRanges:     newTsRanges,
		NewRecordCounts: newRecordCounts,
		NewFileSizes:    newFileSizes,
		NewFingerprints: newFingerprints,
		OldSegmentIds:   oldSegmentIds,
	}

	err := mdb.ReplaceCompactedMetricSegs(ctx, params)
	if err != nil {
		ll.Error("Failed to replace compacted metric segments",
			slog.Int("oldSegmentCount", len(oldSegmentIds)),
			slog.Int("newSegmentCount", len(newSegmentIds)),
			slog.Any("error", err))
		return err
	}

	ll.Info("Successfully replaced compacted metric segments",
		slog.Int("oldSegmentCount", len(oldSegmentIds)),
		slog.Int("newSegmentCount", len(newSegmentIds)))

	return nil
}

const targetFileSize = constants.TargetFileSizeBytes

func shouldCompactMetrics(rows []lrdb.MetricSeg) bool {
	if len(rows) < 2 {
		return false
	}

	const smallThreshold = int64(targetFileSize) * 3 / 10

	var totalSize int64
	for _, row := range rows {
		totalSize += row.FileSize
		if row.FileSize > targetFileSize*2 || row.FileSize < smallThreshold {
			return true
		}
	}

	estimatedFileCount := (totalSize + targetFileSize - 1) / targetFileSize
	compact := estimatedFileCount < int64(len(rows))-3
	return compact
}

func fetchMetricSegsForCompaction(ctx context.Context, db compactionStore, claimedWork []lrdb.ClaimMetricCompactionWorkRow) ([]lrdb.MetricSeg, error) {
	if len(claimedWork) == 0 {
		return nil, nil
	}

	// All work items must have same org/dateint/frequency/instance (safety check should ensure this)
	firstItem := claimedWork[0]

	// Extract segment IDs from claimed work
	segmentIDs := make([]int64, len(claimedWork))
	for i, item := range claimedWork {
		segmentIDs[i] = item.SegmentID
	}

	// Query actual segments from database
	segments, err := db.GetMetricSegsForCompactionWork(ctx, lrdb.GetMetricSegsForCompactionWorkParams{
		OrganizationID: firstItem.OrganizationID,
		Dateint:        firstItem.Dateint,
		FrequencyMs:    int32(firstItem.FrequencyMs),
		InstanceNum:    firstItem.InstanceNum,
		SegmentIds:     segmentIDs,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to fetch metric segments: %w", err)
	}

	return segments, nil
}
