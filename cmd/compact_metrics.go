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

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/debugging"
	"github.com/cardinalhq/lakerunner/internal/estimator"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/healthcheck"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing/compaction"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "compact-metrics",
		Short: "Roll up metrics",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-compact-metrics"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
				attribute.String("action", "compact"),
			)
			ctx, doneFx, err := setupTelemetry(servicename, &addlAttrs)
			if err != nil {
				return fmt.Errorf("failed to setup telemetry: %w", err)
			}

			defer func() {
				if err := doneFx(); err != nil {
					slog.Error("Error shutting down telemetry", slog.Any("error", err))
				}
			}()

			go diskUsageLoop(ctx)

			// Start pprof server
			go debugging.RunPprof(ctx)

			// Start health check server
			healthConfig := healthcheck.GetConfigFromEnv()
			healthServer := healthcheck.NewServer(healthConfig)

			go func() {
				if err := healthServer.Start(ctx); err != nil {
					slog.Error("Health check server stopped", slog.Any("error", err))
				}
			}()

			mdb, err := dbopen.LRDBStore(ctx)
			if err != nil {
				return fmt.Errorf("failed to open LRDB store: %w", err)
			}

			cdb, err := dbopen.ConfigDBStore(ctx)
			if err != nil {
				return fmt.Errorf("failed to open ConfigDB store: %w", err)
			}

			awsmanager, err := awsclient.NewManager(ctx, awsclient.WithAssumeRoleSessionName(servicename))
			if err != nil {
				return fmt.Errorf("failed to create AWS manager: %w", err)
			}

			sp := storageprofile.NewStorageProfileProvider(cdb)

			metricEst, err := estimator.NewMetricEstimator(ctx, mdb)
			if err != nil {
				return fmt.Errorf("failed to create metric estimator: %w", err)
			}

			config := compaction.GetConfigFromEnv()
			manager := compaction.NewManager(mdb, myInstanceID, config)

			// Mark as healthy once components are initialized
			healthServer.SetStatus(healthcheck.StatusHealthy)

			return runMetricCompactionLoop(ctx, manager, mdb, sp, awsmanager, metricEst)
		},
	}

	rootCmd.AddCommand(cmd)
}

func runMetricCompactionLoop(
	ctx context.Context,
	manager *compaction.Manager,
	mdb lrdb.StoreFull,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
	metricEst estimator.MetricEstimator,
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

		err = processCompactionBatch(ctx, ll, mdb, sp, awsmanager, metricEst, claimedWork)
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

		gc()
	}
}

func processCompactionBatch(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
	metricEst estimator.MetricEstimator,
	claimedWork []lrdb.ClaimMetricCompactionWorkRow,
) error {
	if len(claimedWork) == 0 {
		return nil
	}

	// All work items in a batch should be from the same org/instance/dateint/frequency
	firstItem := claimedWork[0]

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

	rpfEstimate := metricEst.Get(firstItem.OrganizationID, firstItem.InstanceNum, int32(firstItem.FrequencyMs))
	if rpfEstimate <= 0 {
		rpfEstimate = 40_000
	}

	tmpdir, err := os.MkdirTemp("", "lakerunner-compaction-*")
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
	segments := compaction.ConvertToMetricSegs(claimedWork)

	return compactMetricSegments(ctx, ll, mdb, tmpdir, firstItem, profile, s3client, segments, rpfEstimate)
}

func compactMetricSegments(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
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

	if !metricsprocessing.ShouldCompactMetrics(rows) {
		ll.Debug("No need to compact metrics in this batch", slog.Int("rowCount", len(rows)))
		return nil
	}

	// Check for context cancellation before starting compaction
	if ctx.Err() != nil {
		ll.Info("Context cancelled before starting compaction",
			slog.Int("segmentCount", len(rows)),
			slog.Any("error", ctx.Err()))
		return NewWorkerInterrupted("context cancelled before compaction")
	}

	return compactMetricInterval(ctx, ll, mdb, tmpdir, workItem, profile, s3client, rows, rpfEstimate)
}

func compactMetricInterval(
	ctx context.Context,
	ll *slog.Logger,
	mdb lrdb.StoreFull,
	tmpdir string,
	workItem lrdb.ClaimMetricCompactionWorkRow,
	profile storageprofile.StorageProfile,
	s3client *awsclient.S3Client,
	rows []lrdb.MetricSeg,
	rpfEstimate int64,
) error {
	st, _, ok := helpers.RangeBounds(workItem.TsRange)
	if !ok {
		ll.Error("Invalid time range in work item", slog.Any("tsRange", workItem.TsRange))
		return fmt.Errorf("invalid time range in work item: %v", workItem.TsRange)
	}

	config := metricsprocessing.ReaderStackConfig{
		FileSortedCounter: fileSortedCounter,
		CommonAttributes:  commonAttributes,
	}

	readerStack, err := metricsprocessing.CreateReaderStack(
		ctx, ll, tmpdir, s3client, workItem.OrganizationID, profile, st.Time.UTC().UnixMilli(), rows, config, "")
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
			if err := normalizeRowForParquetWrite(row); err != nil {
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
		ll.Warn("Produced 0 output files from aggregating reader - logging source S3 paths for debugging")
		for _, row := range rows {
			dateint, hour := helpers.MSToDateintHour(st.Time.UTC().UnixMilli())
			objectID := helpers.MakeDBObjectID(workItem.OrganizationID, profile.CollectorName, dateint, hour, row.SegmentID, "metrics")
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

	// Final interruption check before critical section (S3 uploads + DB updates)
	if ctx.Err() != nil {
		ll.Info("Context cancelled before critical section - safe to abort",
			slog.Int("resultCount", len(results)),
			slog.Any("error", ctx.Err()))
		return NewWorkerInterrupted("context cancelled before metrics upload phase")
	}

	compactionParams := metricsprocessing.CompactionUploadParams{
		OrganizationID: workItem.OrganizationID.String(),
		InstanceNum:    workItem.InstanceNum,
		Dateint:        workItem.Dateint,
		FrequencyMs:    int32(workItem.FrequencyMs),
		SlotID:         0, // Default slot ID
		IngestDateint:  metricsprocessing.GetIngestDateint(rows),
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
	}

	// Use context without cancellation for critical section to ensure atomic completion
	criticalCtx := context.WithoutCancel(ctx)
	err = metricsprocessing.UploadCompactedMetrics(criticalCtx, ll, mdb, s3client, results, rows, compactionParams)
	if err != nil {
		return fmt.Errorf("failed to upload compacted metrics: %w", err)
	}

	metricsprocessing.ScheduleOldFileCleanup(criticalCtx, ll, mdb, rows, profile)

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
