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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metriccompaction"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-metrics",
		Short: "Roll up metrics",
		RunE: func(_ *cobra.Command, _ []string) error {
			if os.Getenv("LAKERUNNER_METRICS_COMPACT_OLDPATH") != "" {
				return oldCompactMetricsCommand().RunE(nil, []string{})
			}

			helpers.SetupTempDir()

			servicename := "lakerunner-compact-metrics"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
				attribute.String("action", "compact"),
			)
			doneCtx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(doneCtx)

			loop, err := NewRunqueueLoopContext(doneCtx, "metrics", "compact", servicename)
			if err != nil {
				return fmt.Errorf("failed to create runqueue loop context: %w", err)
			}

			return RunqueueLoop(loop, compactRollupItem, nil)
		},
	}

	rootCmd.AddCommand(cmd)
}

func compactRollupItem(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	awsmanager *awsclient.Manager,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	inf lockmgr.Workable,
	rpfEstimate int64,
	args any,
) (WorkResult, error) {
	if !helpers.IsWantedFrequency(inf.FrequencyMs()) {
		ll.Debug("Skipping compaction for unwanted frequency", slog.Int("frequencyMs", int(inf.FrequencyMs())))
		return WorkResultSuccess, nil
	}

	profile, err := sp.GetStorageProfileForOrganizationAndInstance(ctx, inf.OrganizationID(), inf.InstanceNum())
	if err != nil {
		ll.Error("Failed to get storage profile", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		ll.Error("Failed to get S3 client", slog.Any("error", err))
		return WorkResultTryAgainLater, err
	}

	ll.Debug("Starting metric compaction",
		slog.String("organizationID", inf.OrganizationID().String()),
		slog.Int("instanceNum", int(inf.InstanceNum())),
		slog.Int("dateint", int(inf.Dateint())),
		slog.Int("frequencyMs", int(inf.FrequencyMs())),
		slog.Int64("workQueueID", inf.ID()))

	return compactMetricSegments(ctx, ll, mdb, tmpdir, inf, profile, s3client, rpfEstimate)
}

func compactMetricSegments(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rpfEstimate int64,
) (WorkResult, error) {
	st, et, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		return WorkResultSuccess, fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	const maxRowsLimit = 1000
	totalBatchesProcessed := 0
	totalSegmentsProcessed := 0
	cursorCreatedAt := time.Time{}
	cursorSegmentID := int64(0)

	for {
		if ctx.Err() != nil {
			ll.Info("Context cancelled, stopping compaction loop - will retry to continue",
				slog.Int("processedBatches", totalBatchesProcessed),
				slog.Int("processedSegments", totalSegmentsProcessed),
				slog.Any("error", ctx.Err()))
			return WorkResultTryAgainLater, nil
		}

		ll.Debug("Querying for metric segments to compact",
			slog.Int("batch", totalBatchesProcessed+1),
			slog.Time("cursorCreatedAt", cursorCreatedAt),
			slog.Int64("cursorSegmentID", cursorSegmentID))

		inRows, err := mdb.GetMetricSegsForCompaction(ctx, lrdb.GetMetricSegsForCompactionParams{
			OrganizationID:  inf.OrganizationID(),
			Dateint:         inf.Dateint(),
			FrequencyMs:     inf.FrequencyMs(),
			InstanceNum:     inf.InstanceNum(),
			SlotID:          inf.SlotId(),
			StartTs:         st.Time.UTC().UnixMilli(),
			EndTs:           et.Time.UTC().UnixMilli(),
			MaxFileSize:     constants.TargetFileSizeBytes * 9 / 10,
			CursorCreatedAt: cursorCreatedAt,
			CursorSegmentID: cursorSegmentID,
			Maxrows:         maxRowsLimit,
		})
		if err != nil {
			ll.Error("Failed to get current metric segments", slog.Any("error", err))
			return WorkResultTryAgainLater, err
		}

		if len(inRows) == 0 {
			if totalBatchesProcessed == 0 {
				ll.Debug("No input rows to compact, skipping work item")
			} else {
				ll.Debug("Finished processing all compaction batches",
					slog.Int("totalBatches", totalBatchesProcessed),
					slog.Int("totalSegments", totalSegmentsProcessed))
			}
			return WorkResultSuccess, nil
		}

		// Calculate total bytes and records in this batch
		totalBytes := int64(0)
		totalRecords := int64(0)
		for _, row := range inRows {
			totalBytes += row.FileSize
			totalRecords += row.RecordCount
		}

		// Estimate number of output files
		estimatedOutputFiles := int64(0)
		if rpfEstimate > 0 {
			estimatedOutputFiles = (totalRecords + rpfEstimate - 1) / rpfEstimate // Ceiling division
		}

		ll.Debug("Processing compaction batch",
			slog.Int("segmentCount", len(inRows)),
			slog.Int("batch", totalBatchesProcessed+1),
			slog.Int64("totalBytes", totalBytes),
			slog.Int64("totalRecords", totalRecords),
			slog.Int64("estimatedRecordsPerFile", rpfEstimate),
			slog.Int64("estimatedOutputFiles", estimatedOutputFiles))

		if len(inRows) > 0 {
			lastRow := inRows[len(inRows)-1]
			cursorCreatedAt = lastRow.CreatedAt
			cursorSegmentID = lastRow.SegmentID
		}

		if !metriccompaction.ShouldCompactMetrics(inRows) {
			ll.Debug("No need to compact metrics in this batch", slog.Int("rowCount", len(inRows)))

			if len(inRows) < maxRowsLimit {
				if totalBatchesProcessed == 0 {
					ll.Debug("No segments need compaction")
				}
				return WorkResultSuccess, nil
			}
			totalBatchesProcessed++
			continue
		}

		err = compactMetricInterval(ctx, ll, mdb, tmpdir, inf, profile, s3client, inRows, rpfEstimate)
		if err != nil {
			ll.Error("Failed to compact interval", slog.Any("error", err))
			return WorkResultTryAgainLater, err
		}

		totalBatchesProcessed++
		totalSegmentsProcessed += len(inRows)

		if len(inRows) < maxRowsLimit {
			ll.Debug("Completed all compaction batches",
				slog.Int("totalBatches", totalBatchesProcessed),
				slog.Int("totalSegments", totalSegmentsProcessed))
			return WorkResultSuccess, nil
		}

		ll.Debug("Batch completed, checking for more segments",
			slog.Int("processedSegments", len(inRows)),
			slog.Time("nextCursorCreatedAt", cursorCreatedAt),
			slog.Int64("nextCursorSegmentID", cursorSegmentID))
	}
}

func compactMetricInterval(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	inf lockmgr.Workable,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rows []lrdb.MetricSeg,
	rpfEstimate int64,
) error {
	st, _, ok := helpers.RangeBounds(inf.TsRange())
	if !ok {
		ll.Error("Invalid time range in work item", slog.Any("tsRange", inf.TsRange()))
		return fmt.Errorf("invalid time range in work item: %v", inf.TsRange())
	}

	var readers []filereader.Reader
	var downloadedFiles []string

	for _, row := range rows {
		dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())
		objectID := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")

		fn, _, is404, err := s3helper.DownloadS3Object(ctx, tmpdir, s3client, profile.Bucket, objectID)
		if err != nil {
			ll.Error("Failed to download S3 object", slog.String("objectID", objectID), slog.Any("error", err))
			return err
		}
		if is404 {
			ll.Info("S3 object not found, skipping", slog.String("bucket", profile.Bucket), slog.String("objectID", objectID))
			continue
		}

		file, err := os.Open(fn)
		if err != nil {
			ll.Error("Failed to open parquet file", slog.String("file", fn), slog.Any("error", err))
			return fmt.Errorf("opening parquet file %s: %w", fn, err)
		}

		stat, err := file.Stat()
		if err != nil {
			file.Close()
			ll.Error("Failed to stat parquet file", slog.String("file", fn), slog.Any("error", err))
			return fmt.Errorf("statting parquet file %s: %w", fn, err)
		}

		reader, err := filereader.NewPreorderedParquetRawReader(file, stat.Size(), 1000)
		if err != nil {
			file.Close()
			ll.Error("Failed to create parquet reader", slog.String("file", fn), slog.Any("error", err))
			return fmt.Errorf("creating parquet reader for %s: %w", fn, err)
		}

		// Wrap with disk-based sorting reader to ensure data is sorted by [name, tid, timestamp]
		// This is required by the aggregating reader downstream
		sortingReader, err := filereader.NewDiskSortingReader(reader,
			filereader.MetricNameTidTimestampSortKeyFunc(),
			filereader.MetricNameTidTimestampSortFunc(), 1000)
		if err != nil {
			reader.Close()
			file.Close()
			ll.Error("Failed to create disk sorting reader", slog.String("file", fn), slog.Any("error", err))
			return fmt.Errorf("creating disk sorting reader for %s: %w", fn, err)
		}

		readers = append(readers, sortingReader)
		downloadedFiles = append(downloadedFiles, fn)
	}

	if len(readers) == 0 {
		ll.Debug("No files to compact, skipping work item")
		return nil
	}

	defer func() {
		for _, reader := range readers {
			if err := reader.Close(); err != nil {
				ll.Error("Failed to close reader", slog.Any("error", err))
			}
		}
	}()

	var finalReader filereader.Reader
	if len(readers) == 1 {
		ll.Debug("Using single reader for compaction")
		finalReader = readers[0]
	} else {
		ll.Debug("Creating multi-source reader", slog.Int("readerCount", len(readers)))
		selector := metricsprocessing.MetricsOrderedSelector()
		multiReader, err := filereader.NewPreorderedMultisourceReader(readers, selector, 1000)
		if err != nil {
			ll.Error("Failed to create preordered multi-source reader", slog.Any("error", err))
			return fmt.Errorf("creating preordered multi-source reader: %w", err)
		}
		finalReader = multiReader
		defer multiReader.Close()
	}

	// Wrap with aggregating reader to merge duplicates during compaction
	frequencyMs := int64(inf.FrequencyMs())
	ll.Debug("Creating aggregating metrics reader", slog.Int64("frequencyMs", frequencyMs))
	aggregatingReader, err := filereader.NewAggregatingMetricsReader(finalReader, frequencyMs, 1000)
	if err != nil {
		ll.Error("Failed to create aggregating metrics reader", slog.Any("error", err))
		return fmt.Errorf("creating aggregating metrics reader: %w", err)
	}
	defer aggregatingReader.Close()

	recordsPerFile := rpfEstimate
	if recordsPerFile <= 0 {
		recordsPerFile = 10_000
	}

	slotID := inf.SlotId()
	baseName := fmt.Sprintf("compacted_metrics_%s_%d_%d", inf.OrganizationID().String(), time.Now().Unix(), slotID)
	writer, err := factories.NewMetricsWriter(baseName, tmpdir, constants.WriterTargetSizeBytesMetrics, recordsPerFile)
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

		n := 0
		if batch != nil {
			n = batch.Len()
		}


		if err != nil && !errors.Is(err, io.EOF) {
			ll.Error("Failed to read from aggregating reader", slog.Any("error", err))
			return fmt.Errorf("reading from aggregating reader: %w", err)
		}

		if batch == nil || batch.Len() == 0 {
			break
		}

		// Create a new batch for normalized rows
		normalizedBatch := pipeline.GetBatch()

		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			if err := normalizeRowForParquetWrite(row); err != nil {
				ll.Error("Failed to normalize row", slog.Any("error", err))
				pipeline.ReturnBatch(normalizedBatch)
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
				return fmt.Errorf("writing batch: %w", err)
			}
		}

		pipeline.ReturnBatch(normalizedBatch)

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
		slog.Int64("recordsPerFile", recordsPerFile),
		slog.Int64("inputBytes", inputBytes),
		slog.Int64("outputBytes", outputBytes),
		slog.Float64("compressionRatio", compressionRatio))

	// If we produced 0 output files, log source S3 paths for debugging and skip database updates
	if len(results) == 0 {
		ll.Warn("Produced 0 output files from aggregating reader - logging source S3 paths for debugging")
		for _, row := range rows {
			dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())
			objectID := helpers.MakeDBObjectID(inf.OrganizationID(), profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
			s3Path := fmt.Sprintf("s3://%s/%s", profile.Bucket, objectID)
			ll.Warn("Source file for debugging",
				slog.String("s3Path", s3Path),
				slog.Int64("segmentID", row.SegmentID),
				slog.Int64("fileSize", row.FileSize),
				slog.Int64("recordCount", row.RecordCount))
		}
		ll.Debug("Skipping database updates since no output files were created")
		return nil
	}

	compactionParams := metricsprocessing.CompactionUploadParams{
		OrganizationID: inf.OrganizationID().String(),
		InstanceNum:    inf.InstanceNum(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    inf.FrequencyMs(),
		SlotID:         inf.SlotId(),
		IngestDateint:  metricsprocessing.GetIngestDateint(rows),
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
	}

	err = metricsprocessing.UploadCompactedMetrics(ctx, ll, mdb, s3client, results, rows, compactionParams)
	if err != nil {
		return fmt.Errorf("failed to upload compacted metrics: %w", err)
	}

	metricsprocessing.ScheduleOldFileCleanup(ctx, ll, mdb, rows, profile)

	return nil
}

// normalizeRowForParquetWrite ensures row fields are in the correct type for parquet writing.
// Specifically converts sketch field from string to []byte to match parquet schema.
func normalizeRowForParquetWrite(row filereader.Row) error {
	sketch := row[wkk.RowKeySketch]
	if sketch == nil {
		return nil
	}

	if _, ok := sketch.([]byte); ok {
		return nil
	}

	if str, ok := sketch.(string); ok {
		row[wkk.RowKeySketch] = []byte(str)
		return nil
	}

	return fmt.Errorf("unexpected sketch type for parquet writing: %T", sketch)
}
