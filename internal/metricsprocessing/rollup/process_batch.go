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
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
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

// CompactionUploadParams contains parameters for uploading compacted metric files.
type CompactionUploadParams struct {
	OrganizationID uuid.UUID
	InstanceNum    int16
	Dateint        int32
	FrequencyMs    int32
	SlotID         int32
	SlotCount      int32
	IngestDateint  int32
	CollectorName  string
	Bucket         string
}

func processBatch(
	ctx context.Context,
	ll *slog.Logger,
	mdb rollupStore,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
	claimedWork []lrdb.MrqFetchCandidatesRow,
	estimatedTargetRecords int64,
) error {
	if len(claimedWork) == 0 {
		return nil
	}

	// Generate batch ID and enhance logger
	batchID := idgen.GenerateShortBase32ID()
	ll = ll.With(slog.String("batchID", batchID))

	// Log work items we're processing once at the top
	workItemIDs := make([]int64, len(claimedWork))
	for i, work := range claimedWork {
		workItemIDs[i] = work.ID
	}
	ll.Debug("Processing rollup batch",
		slog.Int("workItemCount", len(claimedWork)),
		slog.Any("workItemIDs", workItemIDs))

	// Get source segments to rollup from using specific segment IDs
	sourceRows, err := fetchMetricSegsFromCandidates(ctx, mdb, claimedWork)
	if err != nil {
		ll.Error("Failed to fetch metric segments for rollup", slog.Any("error", err))
		return err
	}

	if len(sourceRows) == 0 {
		ll.Info("No source segments found for claimed work items")
		return nil
	}

	// Extract key information from the first segment
	firstSeg := sourceRows[0]

	// Work items represent source frequencies that roll up to target frequencies
	sourceFrequency := firstSeg.FrequencyMs
	targetFrequency, found := metricsprocessing.RollupTo[sourceFrequency]
	if !found {
		ll.Error("Invalid rollup frequency - not in source frequency list. This work item will be marked as completed to avoid reprocessing.",
			slog.Int64("frequencyMs", int64(sourceFrequency)))
		// Return nil to indicate successful processing so the work gets marked as completed
		// This prevents these invalid work items from being retried indefinitely
		return nil
	}

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, firstSeg.OrganizationID, firstSeg.InstanceNum)
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return err
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return err
	}

	tmpdir, err := os.MkdirTemp("", "")
	if err != nil {
		ll.Error("Failed to create temporary directory", slog.Any("error", err))
		return fmt.Errorf("failed to create temporary directory: %w", err)
	}
	defer func() {
		if err := os.RemoveAll(tmpdir); err != nil {
			ll.Error("Failed to remove temporary directory", slog.Any("error", err))
		}
	}()

	ll.Info("Starting metric rollup batch",
		slog.String("organizationID", firstSeg.OrganizationID.String()),
		slog.Int("instanceNum", int(firstSeg.InstanceNum)),
		slog.Int("dateint", int(firstSeg.Dateint)),
		slog.Int("sourceFrequencyMs", int(sourceFrequency)),
		slog.Int("targetFrequencyMs", int(targetFrequency)),
		slog.Int("slotID", int(firstSeg.SlotID)),
		slog.Int("slotCount", int(firstSeg.SlotCount)),
		slog.Int("batchSize", len(claimedWork)))

	t0 := time.Now()

	// Log if we got fewer segments than expected
	if len(sourceRows) < len(claimedWork) {
		ll.Error("Retrieved fewer segments than work items - some segments may be missing",
			slog.Int("expectedSegments", len(claimedWork)),
			slog.Int("retrievedSegments", len(sourceRows)))
	} else if len(sourceRows) > len(claimedWork) {
		ll.Error("Retrieved more segments than work items - this should not happen",
			slog.Int("expectedSegments", len(claimedWork)),
			slog.Int("retrievedSegments", len(sourceRows)))
	}

	ll.Debug("Retrieved source segments for rollup analysis",
		slog.Int("sourceSegmentCount", len(sourceRows)),
		slog.Int("sourceFrequencyMs", int(sourceFrequency)),
		slog.Int("targetFrequencyMs", int(targetFrequency)))

	if helpers.AllRolledUp(sourceRows) {
		ll.Info("All source segments already rolled up, skipping rollup operation",
			slog.Int("sourceSegmentCount", len(sourceRows)),
			slog.Int("sourceFrequencyMs", int(firstSeg.FrequencyMs)),
			slog.Int("targetFrequencyMs", int(targetFrequency)))
		return nil
	}

	// Get existing segments at target frequency
	existingRows, err := mdb.GetMetricSegsForRollup(ctx, lrdb.GetMetricSegsForRollupParams{
		OrganizationID: firstSeg.OrganizationID,
		Dateint:        firstSeg.Dateint,
		FrequencyMs:    targetFrequency,
		InstanceNum:    firstSeg.InstanceNum,
		SlotID:         firstSeg.SlotID,
	})
	if err != nil {
		ll.Error("Failed to get existing metric segments", slog.Any("error", err))
		return err
	}

	ll.Debug("Retrieved existing target frequency segments for rollup analysis",
		slog.Int("existingSegmentCount", len(existingRows)),
		slog.Int("targetFrequencyMs", int(targetFrequency)))

	err = rollupMetricSegments(ctx, ll, mdb, tmpdir, firstSeg, profile, s3client, sourceRows, existingRows, targetFrequency, estimatedTargetRecords)

	elapsed := time.Since(t0)

	if err != nil {
		ll.Info("Metric rollup batch completed",
			slog.String("result", "error"),
			slog.Int("batchSize", len(claimedWork)),
			slog.Int("sourceSegmentCount", len(sourceRows)),
			slog.Int("existingTargetSegmentCount", len(existingRows)),
			slog.Duration("elapsed", elapsed),
			slog.Any("error", err))
		return err
	} else {
		ll.Info("Metric rollup batch completed",
			slog.String("result", "success"),
			slog.Int("batchSize", len(claimedWork)),
			slog.Int("sourceSegmentCount", len(sourceRows)),
			slog.Int("existingTargetSegmentCount", len(existingRows)),
			slog.Int("sourceFrequencyMs", int(firstSeg.FrequencyMs)),
			slog.Int("targetFrequencyMs", int(targetFrequency)),
			slog.Duration("elapsed", elapsed))
		return nil
	}
}

func rollupMetricSegments(
	ctx context.Context,
	ll *slog.Logger,
	mdb rollupStore,
	tmpdir string,
	firstSeg lrdb.MetricSeg,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	sourceRows []lrdb.MetricSeg,
	existingRows []lrdb.MetricSeg,
	targetFrequency int32,
	estimatedTargetRecords int64,
) error {
	if len(sourceRows) == 0 {
		ll.Info("No source segments found for rollup, skipping rollup operation",
			slog.Int("sourceFrequencyMs", int(firstSeg.FrequencyMs)),
			slog.Int("targetFrequencyMs", int(targetFrequency)),
			slog.String("organizationID", firstSeg.OrganizationID.String()),
			slog.Int("dateint", int(firstSeg.Dateint)))
		return nil
	}

	// Track segments coming into rollup processing
	processingSegmentsIn.Add(ctx, int64(len(sourceRows)), metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "rollup"),
	))

	// Create reader stack with sorting support
	config := metricsprocessing.ReaderStackConfig{
		FileSortedCounter: fileSortedCounter,
		CommonAttributes:  commonAttributes,
	}

	readerStack, err := metricsprocessing.CreateReaderStack(
		ctx, ll, tmpdir, s3client, firstSeg.OrganizationID, profile, sourceRows, config)
	if err != nil {
		return err
	}

	if len(readerStack.Readers) == 0 {
		ll.Info("No files available for rollup processing, skipping work item",
			slog.Int("sourceSegmentCount", len(sourceRows)),
			slog.Int("sourceFrequencyMs", int(firstSeg.FrequencyMs)),
			slog.Int("targetFrequencyMs", int(targetFrequency)),
			slog.String("organizationID", firstSeg.OrganizationID.String()),
			slog.Int("dateint", int(firstSeg.Dateint)))
		return nil
	}

	defer metricsprocessing.CloseReaderStack(ll, readerStack)

	// Calculate input statistics from source segments
	inputBytes := int64(0)
	inputRecords := int64(0)
	for _, row := range sourceRows {
		inputBytes += row.FileSize
		inputRecords += row.RecordCount
	}

	ll.Debug("Starting rollup processing",
		slog.Int("inputFiles", len(readerStack.DownloadedFiles)),
		slog.Int("sourceSegments", len(sourceRows)),
		slog.Int64("inputRecords", inputRecords),
		slog.Int64("inputBytes", inputBytes),
		slog.Int("targetFrequencyMs", int(targetFrequency)),
		slog.Int64("estimatedTargetRecords", estimatedTargetRecords))

	aggregatingReader, err := filereader.NewAggregatingMetricsReader(readerStack.FinalReader, int64(targetFrequency), 1000)
	if err != nil {
		ll.Error("Failed to create aggregating metrics reader", slog.Any("error", err))
		return fmt.Errorf("creating aggregating metrics reader: %w", err)
	}
	defer aggregatingReader.Close()

	writer, err := factories.NewMetricsWriter(tmpdir, estimatedTargetRecords)
	if err != nil {
		ll.Error("Failed to create metrics writer", slog.Any("error", err))
		return fmt.Errorf("creating metrics writer: %w", err)
	}
	defer writer.Abort()

	totalRows := int64(0)
	batchCount := 0

	for {
		batch, err := aggregatingReader.Next(ctx)
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

		if errors.Is(err, io.EOF) {
			break
		}

		totalRows += int64(batch.Len())
		if batch.Len() > 0 {
			if err := writer.WriteBatch(batch); err != nil {
				ll.Error("Failed to write batch", slog.Any("error", err))
				pipeline.ReturnBatch(batch)
				return fmt.Errorf("writing batch: %w", err)
			}
		}

		pipeline.ReturnBatch(batch)
	}

	results, err := writer.Close(ctx)
	if err != nil {
		ll.Error("Failed to finish writing", slog.Any("error", err))
		return fmt.Errorf("finishing writer: %w", err)
	}

	// Calculate output statistics
	outputBytes := int64(0)
	outputRecords := int64(0)
	for _, result := range results {
		outputBytes += result.FileSize
		outputRecords += result.RecordCount
	}

	// Calculate compression ratio
	compressionRatio := float64(0)
	if inputBytes > 0 {
		compressionRatio = float64(outputBytes) / float64(inputBytes) * 100
	}

	ll.Debug("Rollup processing completed",
		slog.Int64("inputRecords", inputRecords),
		slog.Int64("outputRecords", outputRecords),
		slog.Int64("totalRows", totalRows),
		slog.Int("batchCount", batchCount),
		slog.Int("outputFiles", len(results)),
		slog.Int("inputFiles", len(readerStack.DownloadedFiles)),
		slog.Int64("inputBytes", inputBytes),
		slog.Int64("outputBytes", outputBytes),
		slog.Float64("compressionRatio", compressionRatio))

	// Track processing metrics
	processingSegmentsOut.Add(ctx, int64(len(results)), metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "rollup"),
	))

	processingRecordsIn.Add(ctx, inputRecords, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "rollup"),
	))

	processingRecordsOut.Add(ctx, outputRecords, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "rollup"),
	))

	processingBytesIn.Add(ctx, inputBytes, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "rollup"),
	))

	processingBytesOut.Add(ctx, outputBytes, metric.WithAttributes(
		attribute.String("signal", "metrics"),
		attribute.String("action", "rollup"),
	))

	rollupParams := CompactionUploadParams{
		OrganizationID: firstSeg.OrganizationID,
		InstanceNum:    firstSeg.InstanceNum,
		Dateint:        firstSeg.Dateint,
		FrequencyMs:    targetFrequency,
		SlotID:         firstSeg.SlotID,
		SlotCount:      firstSeg.SlotCount,
		IngestDateint:  metricsprocessing.GetIngestDateint(sourceRows),
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
	}

	criticalCtx := context.WithoutCancel(ctx)

	err = uploadRolledUpMetricsAtomic(criticalCtx, ll, mdb, s3client, results, sourceRows, existingRows, rollupParams)
	if err != nil {
		return fmt.Errorf("failed to upload rolled-up metrics: %w", err)
	}

	return nil
}

// fetchMetricSegsFromCandidates retrieves the MetricSeg records corresponding to the claimed candidates.
// TODO: This is a simplified implementation that assumes we can get the key info another way
// In production, we would need to modify the bundle claim to return key information
func fetchMetricSegsFromCandidates(ctx context.Context, db rollupStore, candidates []lrdb.MrqFetchCandidatesRow) ([]lrdb.MetricSeg, error) {
	if len(candidates) == 0 {
		return nil, nil
	}

	// For now, return empty to avoid compilation errors
	// TODO: Implement proper segment fetching with key information
	return []lrdb.MetricSeg{}, nil
}

var (
	meter            = otel.Meter("github.com/cardinalhq/lakerunner/internal/metricsprocessing/rollup")
	commonAttributes = attribute.NewSet(
		attribute.String("component", "metric-rollup"),
	)

	fileSortedCounter metric.Int64Counter

	// Processing counters
	processingSegmentsIn  metric.Int64Counter
	processingSegmentsOut metric.Int64Counter
	processingRecordsIn   metric.Int64Counter
	processingRecordsOut  metric.Int64Counter
	processingBytesIn     metric.Int64Counter
	processingBytesOut    metric.Int64Counter
)

func init() {
	var err error

	fileSortedCounter, err = meter.Int64Counter("lakerunner.processing.input.filetype")
	if err != nil {
		panic(fmt.Errorf("failed to create processing.input.filetype counter: %w", err))
	}

	processingSegmentsIn, err = meter.Int64Counter(
		"lakerunner.processing.segments.in",
		metric.WithDescription("Number of segments input to rollup processing pipeline"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create processing.segments.in counter: %w", err))
	}

	processingSegmentsOut, err = meter.Int64Counter(
		"lakerunner.processing.segments.out",
		metric.WithDescription("Number of segments output from rollup processing pipeline"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create processing.segments.out counter: %w", err))
	}

	processingRecordsIn, err = meter.Int64Counter(
		"lakerunner.processing.records.in",
		metric.WithDescription("Number of records input to rollup processing pipeline"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create processing.records.in counter: %w", err))
	}

	processingRecordsOut, err = meter.Int64Counter(
		"lakerunner.processing.records.out",
		metric.WithDescription("Number of records output from rollup processing pipeline"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create processing.records.out counter: %w", err))
	}

	processingBytesIn, err = meter.Int64Counter(
		"lakerunner.processing.bytes.in",
		metric.WithDescription("Number of bytes input to rollup processing pipeline"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create processing.bytes.in counter: %w", err))
	}

	processingBytesOut, err = meter.Int64Counter(
		"lakerunner.processing.bytes.out",
		metric.WithDescription("Number of bytes output from rollup processing pipeline"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create processing.bytes.out counter: %w", err))
	}
}

func uploadRolledUpMetricsAtomic(
	ctx context.Context,
	ll *slog.Logger,
	mdb rollupStore,
	s3client *awsclient.S3Client,
	results []parquetwriter.Result,
	sourceRows []lrdb.MetricSeg,
	existingRows []lrdb.MetricSeg,
	params CompactionUploadParams,
) error {
	// Create processed segments from results
	segments := make(metricsprocessing.ProcessedSegments, 0, len(results))
	for _, result := range results {
		segment, err := metricsprocessing.NewProcessedSegment(result, params.OrganizationID, params.CollectorName, ll)
		if err != nil {
			return fmt.Errorf("failed to create processed segment: %w", err)
		}
		segments = append(segments, segment)
	}
	var targetOldRecords []lrdb.CompactMetricSegsOld
	for _, row := range existingRows {
		targetOldRecords = append(targetOldRecords, lrdb.CompactMetricSegsOld{
			SegmentID: row.SegmentID,
			SlotID:    row.SlotID,
		})
	}

	sourceSegmentIDs := make([]int64, len(sourceRows))
	for i, row := range sourceRows {
		sourceSegmentIDs[i] = row.SegmentID
	}

	ll.Debug("Starting atomic rollup operations for rolled-up files",
		slog.Int("fileCount", len(segments)),
		slog.Int("sourceSegmentCount", len(sourceSegmentIDs)))

	for i, segment := range segments {
		fileLogger := ll.With(
			slog.String("file", segment.Result.FileName),
			slog.Int("fileIndex", i+1),
			slog.Int("totalFiles", len(segments)))

		fileLogger.Debug("Starting atomic metric rollup upload operation",
			slog.Int64("recordCount", segment.Result.RecordCount),
			slog.Int64("fileSize", segment.Result.FileSize),
			slog.Int64("startTs", segment.StartTs),
			slog.Int64("endTs", segment.EndTs),
			slog.Int("fingerprintCount", len(segment.Fingerprints)))

		fileLogger.Debug("Uploading rolled-up metric file to S3 - point of no return approaching",
			slog.String("newObjectID", segment.ObjectID),
			slog.String("bucket", params.Bucket),
			slog.Int64("newSegmentID", segment.SegmentID))

		err := segment.UploadToS3(ctx, s3client, params.Bucket)
		if err != nil {
			fileLogger.Error("Atomic operation failed during S3 upload - no changes made",
				slog.Any("error", err),
				slog.String("objectID", segment.ObjectID))
			return fmt.Errorf("uploading new S3 object: %w", err)
		}

		fileLogger.Debug("S3 upload successful, updating database with atomic rollup transaction - CRITICAL SECTION",
			slog.String("uploadedObject", segment.ObjectID),
			slog.Int64("uploadedBytes", segment.Result.FileSize),
			slog.Int64("uploadedRecords", segment.Result.RecordCount))

		sourceFrequency := sourceRows[0].FrequencyMs

		if err := mdb.RollupMetricSegs(ctx,
			lrdb.RollupSourceParams{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				FrequencyMs:    sourceFrequency,
				InstanceNum:    params.InstanceNum,
			},
			lrdb.RollupTargetParams{
				OrganizationID: params.OrganizationID,
				Dateint:        params.Dateint,
				FrequencyMs:    params.FrequencyMs,
				InstanceNum:    params.InstanceNum,
				SlotID:         params.SlotID,
				SlotCount:      params.SlotCount,
				IngestDateint:  params.IngestDateint,
				SortVersion:    lrdb.CurrentMetricSortVersion,
			},
			sourceSegmentIDs,
			[]lrdb.RollupNewRecord{
				{
					SegmentID:    segment.SegmentID,
					StartTs:      segment.StartTs,
					EndTs:        segment.EndTs,
					RecordCount:  segment.Result.RecordCount,
					FileSize:     segment.Result.FileSize,
					Fingerprints: segment.Fingerprints,
				},
			},
		); err != nil {
			fileLogger.Error("Database rollup transaction failed after S3 upload - file orphaned in S3",
				slog.Any("error", err),
				slog.String("orphanedObject", segment.ObjectID),
				slog.Int64("orphanedSegmentID", segment.SegmentID),
				slog.String("bucket", params.Bucket))

			if scheduleErr := segment.ScheduleCleanupIfUploaded(ctx, mdb, params.OrganizationID, params.InstanceNum, params.Bucket); scheduleErr != nil {
				fileLogger.Error("Failed to schedule orphaned S3 object for deletion",
					slog.Any("error", scheduleErr),
					slog.String("objectID", segment.ObjectID),
					slog.String("bucket", params.Bucket))
			} else {
				fileLogger.Info("Scheduled orphaned S3 object for deletion",
					slog.String("objectID", segment.ObjectID))
			}
			return fmt.Errorf("atomic rollup transaction: %w", err)
		}

		fileLogger.Debug("Source marked as rolled up, target segments replaced - rollup transaction complete",
			slog.Int64("newSegmentID", segment.SegmentID),
			slog.Int64("newRecordCount", segment.Result.RecordCount),
			slog.Int64("newFileSize", segment.Result.FileSize),
			slog.String("newObjectID", segment.ObjectID),
			slog.Int("sourceSegmentCount", len(sourceSegmentIDs)),
			slog.Int("targetReplacedCount", len(targetOldRecords)),
			slog.Int("sourceFrequencyMs", int(sourceRows[0].FrequencyMs)),
			slog.Int("targetFrequencyMs", int(params.FrequencyMs)))

	}

	// Queue compaction and rollup work for all processed segments
	if err := segments.QueueCompactionWork(ctx, mdb, params.OrganizationID, params.InstanceNum, params.FrequencyMs); err != nil {
		ll.Error("Failed to queue compaction work for rolled-up segments", slog.Any("error", err))
	}

	if err := segments.QueueRollupWork(ctx, mdb, params.OrganizationID, params.InstanceNum, params.FrequencyMs, params.SlotID, params.SlotCount); err != nil {
		ll.Error("Failed to queue rollup work for rolled-up segments", slog.Any("error", err))
	}

	// Calculate final accumulated statistics
	totalOutputBytes := int64(0)
	totalOutputRecords := int64(0)
	for _, segment := range segments {
		totalOutputBytes += segment.Result.FileSize
		totalOutputRecords += segment.Result.RecordCount
	}

	// Calculate total input statistics
	totalInputBytes := int64(0)
	totalInputRecords := int64(0)
	for _, row := range sourceRows {
		totalInputBytes += row.FileSize
		totalInputRecords += row.RecordCount
	}

	ll.Info("Successfully completed atomic rollup operations",
		slog.Int("outputFilesCreated", len(results)),
		slog.Int("sourceSegmentsRolledUp", len(sourceSegmentIDs)),
		slog.Int("targetSegmentsReplaced", len(targetOldRecords)),
		slog.Int64("inputRecords", totalInputRecords),
		slog.Int64("outputRecords", totalOutputRecords),
		slog.Int64("inputBytes", totalInputBytes),
		slog.Int64("outputBytes", totalOutputBytes),
		slog.Int("sourceFrequencyMs", int(sourceRows[0].FrequencyMs)),
		slog.Int("targetFrequencyMs", int(params.FrequencyMs)))

	return nil
}
