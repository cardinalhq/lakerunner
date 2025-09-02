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
	"log/slog"
	"os"
	"strings"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

var (
	meter                = otel.Meter("github.com/cardinalhq/lakerunner/internal/metricsprocessing/ingestion")
	fileSortedCounter, _ = meter.Int64Counter("lakerunner.metric.ingest.file.sorted")
	commonAttributes     = attribute.NewSet(
		attribute.String("component", "metric-ingestion"),
	)
)

// coordinate handles the complete ingestion process for a batch of metric files
func coordinate(
	ctx context.Context,
	input input,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
	mdb lrdb.StoreFull,
) (*result, error) {
	if len(input.Items) == 0 {
		return nil, fmt.Errorf("empty batch")
	}

	input.Logger.Debug("Processing metrics batch", slog.Int("batchSize", len(input.Items)))

	firstItem := input.Items[0]

	// Get storage profile
	profile, err := getStorageProfileForIngestion(ctx, sp, firstItem)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage profile: %w", err)
	}

	input.Logger.Debug("Got storage profile for metrics ingestion",
		slog.String("cloudProvider", profile.CloudProvider),
		slog.String("bucket", profile.Bucket),
		slog.String("region", profile.Region))

	// Create cloud storage client
	storageClient, err := cloudstorage.NewClient(ctx, awsmanager, profile)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage client for provider %s: %w", profile.CloudProvider, err)
	}

	// Create writer manager
	wm := newMetricWriterManager(input.TmpDir, profile.OrganizationID.String(), input.IngestDateint, input.RPFEstimate, input.Logger)

	// Track total rows across all files
	var batchRowsRead, batchRowsProcessed, batchRowsErrored int64

	// Step 1: Download and validate files
	validFiles, err := downloadAndValidateFiles(ctx, input.Items, input.TmpDir, storageClient, profile, input.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to download files: %w", err)
	}

	if len(validFiles) == 0 {
		input.Logger.Warn("No valid files to process in batch")
		results, err := wm.closeAll(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to close writers: %w", err)
		}
		input.Logger.Debug("Batch processing summary", slog.Int("outputFiles", len(results)))
		return &result{Results: results, RowsRead: 0, RowsErrored: 0}, nil
	}

	// Process exemplars from all files if exemplar processor is available
	if input.ExemplarProcessor != nil && ShouldProcessExemplars() {
		input.Logger.Debug("Processing exemplars from all files", slog.Int("fileCount", len(validFiles)))

		for _, fileInfo := range validFiles {
			// Create a separate reader just for exemplar processing from this file
			exemplarReader, err := CreateMetricProtoReader(fileInfo.tmpfilename)
			if err != nil {
				input.Logger.Warn("Failed to create exemplar reader, skipping file",
					slog.String("objectID", fileInfo.item.ObjectID),
					slog.String("error", err.Error()))
				continue
			}

			if err := processExemplarsFromReader(ctx, exemplarReader, input.ExemplarProcessor, profile.OrganizationID.String(), mdb); err != nil {
				input.Logger.Warn("Failed to process exemplars from file",
					slog.String("objectID", fileInfo.item.ObjectID),
					slog.Any("error", err))
			}

			exemplarReader.Close()
		}
	}

	// Step 2: Create readers for each file
	readers, readersToClose, err := createReadersForFiles(validFiles, profile.OrganizationID.String(), input.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create readers: %w", err)
	}

	// Cleanup function for readers
	defer func() {
		for _, reader := range readersToClose {
			if closeErr := reader.Close(); closeErr != nil {
				input.Logger.Warn("Failed to close reader during cleanup", slog.Any("error", closeErr))
			}
		}
	}()

	if len(readers) == 0 {
		input.Logger.Warn("No valid readers created for batch")
		results, err := wm.closeAll(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to close writers: %w", err)
		}
		input.Logger.Debug("Batch processing summary", slog.Int("outputFiles", len(results)))
		return &result{Results: results, RowsRead: 0, RowsErrored: 0}, nil
	}

	input.Logger.Debug("Created readers for batch", slog.Int("readerCount", len(readers)))

	// Step 3: Set up unified reader pipeline
	finalReader, err := createUnifiedReader(ctx, readers)
	if err != nil {
		return nil, fmt.Errorf("failed to create unified reader: %w", err)
	}

	// Step 4: Process all rows from the unified pipeline
	for {
		batch, readErr := finalReader.Next(ctx)

		if readErr != nil && readErr != io.EOF {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			return nil, fmt.Errorf("failed to read from unified pipeline: %w", readErr)
		}

		// Process any rows we got (safe because either no error or EOF with final data)
		if batch != nil {
			processed, errored := wm.processBatch(batch)
			batchRowsProcessed += processed
			batchRowsErrored += errored
			pipeline.ReturnBatch(batch)
		}

		if readErr == io.EOF {
			break
		}
	}

	batchRowsRead = finalReader.TotalRowsReturned()

	input.Logger.Debug("Batch processing completed",
		slog.Int64("rowsRead", batchRowsRead),
		slog.Int64("rowsProcessed", batchRowsProcessed),
		slog.Int64("rowsErrored", batchRowsErrored))

	if batchRowsErrored > 0 {
		input.Logger.Warn("Some rows were dropped due to processing errors",
			slog.Int64("rowsErrored", batchRowsErrored))
	}

	// Close all writers and get results
	results, err := wm.closeAll(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to close writers: %w", err)
	}

	input.Logger.Debug("Batch processing summary",
		slog.Int64("inputRowsRead", batchRowsRead),
		slog.Int64("inputRowsProcessed", batchRowsProcessed),
		slog.Int64("inputRowsErrored", batchRowsErrored),
		slog.Int("outputFiles", len(results)))

	return &result{
		Results:     results,
		RowsRead:    batchRowsRead,
		RowsErrored: batchRowsErrored,
	}, nil
}

// downloadAndValidateFiles downloads and validates all files in the batch
func downloadAndValidateFiles(ctx context.Context, items []lrdb.Inqueue, tmpdir string, storageClient cloudstorage.Client, profile storageprofile.StorageProfile, ll *slog.Logger) ([]fileInfo, error) {
	var validFiles []fileInfo

	for _, inf := range items {
		// Skip database files (processed outputs, not inputs)
		if strings.HasPrefix(inf.ObjectID, "db/") {
			ll.Debug("Skipping database file", slog.String("objectID", inf.ObjectID))
			continue
		}

		// Skip unsupported file types - only process .binpb and .binpb.gz from otel-raw/
		if !IsSupportedMetricsFile(inf.ObjectID) {
			ll.Debug("Skipping unsupported metrics file type", slog.String("objectID", inf.ObjectID))
			continue
		}

		// Download file
		itemTmpdir := fmt.Sprintf("%s/item_%s", tmpdir, inf.ID.String())
		if err := os.MkdirAll(itemTmpdir, 0755); err != nil {
			return nil, fmt.Errorf("creating item tmpdir: %w", err)
		}

		ll.Debug("Downloading metrics object",
			slog.String("cloudProvider", profile.CloudProvider),
			slog.String("bucket", inf.Bucket),
			slog.String("objectID", inf.ObjectID))

		tmpfilename, _, is404, err := storageClient.DownloadObject(ctx, itemTmpdir, inf.Bucket, inf.ObjectID)
		if err != nil {
			return nil, fmt.Errorf("failed to download file %s from %s: %w", inf.ObjectID, profile.CloudProvider, err)
		}
		if is404 {
			ll.Warn("Object not found in cloud storage, skipping",
				slog.String("cloudProvider", profile.CloudProvider),
				slog.String("objectID", inf.ObjectID))
			continue
		}

		validFiles = append(validFiles, fileInfo{item: inf, tmpfilename: tmpfilename})
	}

	return validFiles, nil
}

// createReadersForFiles creates the reader stack for each file
func createReadersForFiles(validFiles []fileInfo, orgID string, ll *slog.Logger) ([]filereader.Reader, []filereader.Reader, error) {
	var readers []filereader.Reader
	var readersToClose []filereader.Reader

	for _, fileInfo := range validFiles {
		// Create stacked reader for this file: ProtoReader -> Translation -> Sorting
		var reader filereader.Reader
		var err error

		// Step 2a: Create base proto reader directly (only support binpb/binpb.gz for metrics)
		reader, err = CreateMetricProtoReader(fileInfo.tmpfilename)
		if err != nil {
			ll.Warn("Failed to create proto reader, skipping file",
				slog.String("objectID", fileInfo.item.ObjectID),
				slog.String("error", err.Error()))
			continue
		}

		// Step 2b: Add translation (adds TID and truncates timestamp)
		translator := &metricsprocessing.MetricTranslator{
			OrgID:    orgID,
			Bucket:   fileInfo.item.Bucket,
			ObjectID: fileInfo.item.ObjectID,
		}
		reader, err = filereader.NewTranslatingReader(reader, translator, 1000)
		if err != nil {
			reader.Close()
			ll.Warn("Failed to create translating reader, skipping file",
				slog.String("objectID", fileInfo.item.ObjectID),
				slog.String("error", err.Error()))
			continue
		}

		// Step 2c: Add disk-based sorting (after translation so TID is available)
		keyProvider := metricsprocessing.GetCurrentMetricSortKeyProvider()
		reader, err = filereader.NewDiskSortingReader(reader, keyProvider, 1000)
		if err != nil {
			reader.Close()
			ll.Warn("Failed to create sorting reader, skipping file",
				slog.String("objectID", fileInfo.item.ObjectID),
				slog.String("error", err.Error()))
			continue
		}

		// Record file format and input sorted status metrics after reader stack is complete
		fileFormat := getFileFormat(fileInfo.tmpfilename)
		attrs := append(commonAttributes.ToSlice(),
			attribute.String("format", fileFormat),
			attribute.Bool("input_sorted", false),
		)
		fileSortedCounter.Add(context.Background(), 1, metric.WithAttributes(attrs...))

		readers = append(readers, reader)
		readersToClose = append(readersToClose, reader)
	}

	return readers, readersToClose, nil
}

// createUnifiedReader creates a single reader from multiple readers
func createUnifiedReader(ctx context.Context, readers []filereader.Reader) (filereader.Reader, error) {
	var finalReader filereader.Reader

	if len(readers) == 1 {
		finalReader = readers[0]
	} else {
		keyProvider := metricsprocessing.GetCurrentMetricSortKeyProvider()
		multiReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
		if err != nil {
			return nil, fmt.Errorf("failed to create multi-source reader: %w", err)
		}
		finalReader = multiReader
	}

	finalReader, err := filereader.NewAggregatingMetricsReader(finalReader, 10000, 1000) // 10 seconds
	if err != nil {
		return nil, fmt.Errorf("failed to create aggregating reader: %w", err)
	}

	return finalReader, nil
}

// getFileFormat determines the file format from filename
func getFileFormat(filename string) string {
	if strings.HasSuffix(filename, ".binpb.gz") {
		return "binpb.gz"
	}
	if strings.HasSuffix(filename, ".binpb") {
		return "binpb"
	}
	return "unknown"
}

// processExemplarsFromReader processes exemplars from a metrics reader that supports OTEL
func processExemplarsFromReader(_ context.Context, reader filereader.Reader, processor *exemplar.Processor, orgID string, mdb lrdb.StoreFull) error {
	if otelProvider, ok := reader.(filereader.OTELMetricsProvider); ok {
		otelMetrics, err := otelProvider.GetOTELMetrics()
		if err != nil {
			return fmt.Errorf("failed to get OTEL metrics: %w", err)
		}

		if metrics, ok := otelMetrics.(*pmetric.Metrics); ok {
			if err := processExemplarsFromMetrics(metrics, processor, orgID); err != nil {
				return fmt.Errorf("failed to process exemplars from metrics: %w", err)
			}
		}
	}
	return nil
}

// processExemplarsFromMetrics processes exemplars from parsed pmetric.Metrics
func processExemplarsFromMetrics(metrics *pmetric.Metrics, processor *exemplar.Processor, customerID string) error {
	ctx := context.Background()
	if err := processor.ProcessMetrics(ctx, *metrics, customerID); err != nil {
		return fmt.Errorf("failed to process metrics through exemplar processor: %w", err)
	}
	return nil
}
