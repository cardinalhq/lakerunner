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
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metriccompaction"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-metrics",
		Short: "Roll up metrics",
		RunE: func(_ *cobra.Command, _ []string) error {
			if os.Getenv("LAKERUNNER_COMPACT_OLDPATH") != "" {
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
			StartTs:         st.Time.UTC().UnixMilli(),
			EndTs:           et.Time.UTC().UnixMilli(),
			MaxFileSize:     1_100_000 * 9 / 10, // Only include files < 90% of target
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

		ll.Debug("Processing compaction batch",
			slog.Int("segmentCount", len(inRows)),
			slog.Int("batch", totalBatchesProcessed+1))

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

	// Download all files and create readers
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

		// Create parquet reader for this file
		reader, err := filereader.ReaderForFile(fn, filereader.SignalTypeMetrics)
		if err != nil {
			ll.Error("Failed to create parquet reader", slog.String("file", fn), slog.Any("error", err))
			return fmt.Errorf("creating parquet reader for %s: %w", fn, err)
		}

		readers = append(readers, reader)
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

	// Create OrderedReader to merge sort the parquet files using metrics ordering
	orderedReader, err := filereader.NewOrderedReader(readers, metricsprocessing.MetricsOrderedSelector())
	if err != nil {
		ll.Error("Failed to create ordered reader", slog.Any("error", err))
		return fmt.Errorf("creating ordered reader: %w", err)
	}
	defer orderedReader.Close()

	// Use records per file estimate directly
	recordsPerFile := rpfEstimate
	if recordsPerFile <= 0 {
		recordsPerFile = 10_000 // Default when no estimate available
	}

	// Create metrics writer using the factory
	baseName := fmt.Sprintf("compacted_metrics_%d", time.Now().Unix())
	writer, err := factories.NewMetricsWriter(baseName, tmpdir, 1_100_000, recordsPerFile)
	if err != nil {
		ll.Error("Failed to create metrics writer", slog.Any("error", err))
		return fmt.Errorf("creating metrics writer: %w", err)
	}
	defer writer.Abort()

	// Process all data through the ordered reader and writer
	const batchSize = 1000
	rowsBatch := make([]filereader.Row, batchSize)
	totalRows := int64(0)

	for {
		n, err := orderedReader.Read(rowsBatch)
		if err != nil && !errors.Is(err, io.EOF) {
			ll.Error("Failed to read from ordered reader", slog.Any("error", err))
			return fmt.Errorf("reading from ordered reader: %w", err)
		}

		if n == 0 {
			break
		}

		// Write batch to metrics writer
		for i := range n {
			if err := writer.Write(rowsBatch[i]); err != nil {
				ll.Error("Failed to write row", slog.Any("error", err))
				return fmt.Errorf("writing row: %w", err)
			}
			totalRows++
		}

		if errors.Is(err, io.EOF) {
			break
		}
	}

	// Finish writing and get results
	results, err := writer.Close(ctx)
	if err != nil {
		ll.Error("Failed to finish writing", slog.Any("error", err))
		return fmt.Errorf("finishing writer: %w", err)
	}

	ll.Debug("Compaction completed",
		slog.Int64("totalRows", totalRows),
		slog.Int("outputFiles", len(results)),
		slog.Int("inputFiles", len(downloadedFiles)),
		slog.Int64("recordsPerFile", recordsPerFile))

	// Upload results and update database
	compactionParams := metricsprocessing.CompactionUploadParams{
		OrganizationID: inf.OrganizationID().String(),
		InstanceNum:    inf.InstanceNum(),
		Dateint:        inf.Dateint(),
		FrequencyMs:    inf.FrequencyMs(),
		IngestDateint:  metricsprocessing.GetIngestDateint(rows),
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
	}

	err = metricsprocessing.UploadCompactedMetrics(ctx, ll, mdb, s3client, results, rows, compactionParams)
	if err != nil {
		return fmt.Errorf("failed to upload compacted metrics: %w", err)
	}

	// Schedule cleanup of old files
	metricsprocessing.ScheduleOldFileCleanup(ctx, ll, mdb, rows, profile)

	return nil
}
