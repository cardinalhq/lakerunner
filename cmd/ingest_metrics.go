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
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel/attribute"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

func init() {
	cmd := &cobra.Command{
		Use:   "ingest-metrics",
		Short: "Ingest metrics from the inqueue table",
		RunE: func(_ *cobra.Command, _ []string) error {
			helpers.SetupTempDir()

			servicename := "lakerunner-ingest-metrics"
			addlAttrs := attribute.NewSet(
				attribute.String("signal", "metrics"),
				attribute.String("action", "ingest"),
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

			loop, err := NewIngestLoopContext(doneCtx, "metrics", servicename)
			if err != nil {
				return fmt.Errorf("failed to create ingest loop context: %w", err)
			}
			defer func() {
				if err := loop.Close(); err != nil {
					slog.Error("Error closing ingest loop context", slog.Any("error", err))
				}
			}()

			// Check if we should use the old implementation as a safety net
			if os.Getenv("LAKERUNNER_METRICS_INGEST_OLDPATH") != "" {
				return runOldMetricIngestion(doneCtx, slog.Default(), loop)
			}

			return IngestLoopWithBatch(loop, metricIngestItem, metricIngestBatch)
		},
	}

	rootCmd.AddCommand(cmd)
}

// minuteSlotKey uniquely identifies a writer for a specific 60-second boundary
type minuteSlotKey struct {
	dateint int32
	minute  int // minute within the day (0-1439)
	slot    int
}

// metricWriterManager manages parquet writers for metrics
type metricWriterManager struct {
	writers       map[minuteSlotKey]*parquetwriter.UnifiedWriter
	tmpdir        string
	orgID         string
	ingestDateint int32
	rpfEstimate   int64
	ll            *slog.Logger
}

func newMetricWriterManager(tmpdir, orgID string, ingestDateint int32, rpfEstimate int64, ll *slog.Logger) *metricWriterManager {
	return &metricWriterManager{
		writers:       make(map[minuteSlotKey]*parquetwriter.UnifiedWriter),
		tmpdir:        tmpdir,
		orgID:         orgID,
		ingestDateint: ingestDateint,
		rpfEstimate:   rpfEstimate,
		ll:            ll,
	}
}

// processRow processes a single metric row
func (wm *metricWriterManager) processRow(row filereader.Row) error {
	// Extract timestamp for 60-second boundary grouping
	ts, ok := row["_cardinalhq.timestamp"].(int64)
	if !ok {
		return fmt.Errorf("_cardinalhq.timestamp field is missing or not int64")
	}

	// Calculate 60-second boundary: dateint and minute within day
	dateint, minute := wm.timestampToMinuteBoundary(ts)
	slot := 0 // Always use slot 0 for metrics

	// Get or create writer for this 60-second boundary
	key := minuteSlotKey{dateint, minute, slot}
	writer, err := wm.getWriter(key)
	if err != nil {
		return fmt.Errorf("failed to get writer for key %v: %w", key, err)
	}

	return writer.Write(row)
}

// timestampToMinuteBoundary converts a timestamp to dateint and minute boundary
func (wm *metricWriterManager) timestampToMinuteBoundary(ts int64) (int32, int) {
	// Convert milliseconds to time
	t := time.Unix(ts/1000, (ts%1000)*1000000).UTC()

	// Calculate dateint (YYYYMMDD)
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())

	// Calculate minute within day, rounded down to 60-second boundary
	minute := t.Hour()*60 + t.Minute()

	return dateint, minute
}

// getWriter returns the writer for a specific minute boundary, creating it if necessary
func (wm *metricWriterManager) getWriter(key minuteSlotKey) (*parquetwriter.UnifiedWriter, error) {
	if writer, exists := wm.writers[key]; exists {
		return writer, nil
	}

	// Create new writer for this boundary
	writer, err := factories.NewMetricsWriter(
		fmt.Sprintf("metrics_%s_%d_%04d_%d", wm.orgID, key.dateint, key.minute, key.slot),
		wm.tmpdir,
		50*1024*1024, // 50MB
		wm.rpfEstimate,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics writer: %w", err)
	}

	wm.writers[key] = writer
	return writer, nil
}

// closeAll closes all writers and returns results
func (wm *metricWriterManager) closeAll(ctx context.Context) ([]parquetwriter.Result, error) {
	var allResults []parquetwriter.Result

	for key, writer := range wm.writers {
		results, err := writer.Close(ctx)
		if err != nil {
			return allResults, fmt.Errorf("failed to close writer %v: %w", key, err)
		}
		allResults = append(allResults, results...)
	}

	return allResults, nil
}

func metricIngestItem(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, inf lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {
	// Convert single item to batch and process
	return metricIngestBatch(ctx, ll, tmpdir, sp, mdb, awsmanager, []lrdb.Inqueue{inf}, ingest_dateint, rpfEstimate, loop)
}

func metricIngestBatch(ctx context.Context, ll *slog.Logger, tmpdir string, sp storageprofile.StorageProfileProvider, mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager, items []lrdb.Inqueue, ingest_dateint int32, rpfEstimate int64, loop *IngestLoopContext) error {

	if len(items) == 0 {
		return fmt.Errorf("empty batch")
	}

	ll.Debug("Processing metrics batch", slog.Int("batchSize", len(items)))

	// Get storage profile and S3 client
	firstItem := items[0]
	var profile storageprofile.StorageProfile
	var err error

	// TODO: Add support for finding storage profiles consistently for arbitrary prefixes at some point
	if collectorName := helpers.ExtractCollectorName(firstItem.ObjectID); collectorName != "" {
		profile, err = sp.GetStorageProfileForOrganizationAndCollector(ctx, firstItem.OrganizationID, collectorName)
		if err != nil {
			return fmt.Errorf("failed to get storage profile for collector %s: %w", collectorName, err)
		}
	} else {
		profile, err = sp.GetStorageProfileForOrganizationAndInstance(ctx, firstItem.OrganizationID, firstItem.InstanceNum)
		if err != nil {
			return fmt.Errorf("failed to get storage profile: %w", err)
		}
	}

	s3client, err := awsmanager.GetS3ForProfile(ctx, profile)
	if err != nil {
		return fmt.Errorf("failed to get S3 client: %w", err)
	}

	// Create writer manager
	wm := newMetricWriterManager(tmpdir, firstItem.OrganizationID.String(), ingest_dateint, rpfEstimate, ll)

	// Track total rows across all files
	var batchRowsRead, batchRowsProcessed, batchRowsErrored int64

	// Step 1: Download all files and collect valid ones
	type fileInfo struct {
		item        lrdb.Inqueue
		tmpfilename string
	}

	var validFiles []fileInfo
	for _, inf := range items {
		// Skip database files (processed outputs, not inputs)
		if strings.HasPrefix(inf.ObjectID, "db/") {
			ll.Debug("Skipping database file", slog.String("objectID", inf.ObjectID))
			continue
		}

		// Skip unsupported file types - only process .binpb and .binpb.gz from otel-raw/
		if !isSupportedMetricsFile(inf.ObjectID) {
			ll.Debug("Skipping unsupported metrics file type", slog.String("objectID", inf.ObjectID))
			continue
		}

		// Download file
		itemTmpdir := fmt.Sprintf("%s/item_%s", tmpdir, inf.ID.String())
		if err := os.MkdirAll(itemTmpdir, 0755); err != nil {
			return fmt.Errorf("creating item tmpdir: %w", err)
		}

		tmpfilename, _, is404, err := s3helper.DownloadS3Object(ctx, itemTmpdir, s3client, inf.Bucket, inf.ObjectID)
		if err != nil {
			return fmt.Errorf("failed to download file %s: %w", inf.ObjectID, err)
		}
		if is404 {
			ll.Warn("S3 object not found, skipping", slog.String("objectID", inf.ObjectID))
			continue
		}

		validFiles = append(validFiles, fileInfo{item: inf, tmpfilename: tmpfilename})
	}

	if len(validFiles) == 0 {
		ll.Warn("No valid files to process in batch")
		// Still need to close the writer manager and return results
		results, err := wm.closeAll(ctx)
		if err != nil {
			return fmt.Errorf("failed to close writers: %w", err)
		}
		// With no files processed, we skip the upload and work queueing
		ll.Debug("Batch processing summary", slog.Int("outputFiles", len(results)))
		return nil
	}

	// Step 2: Create individual readers for each file
	var readers []filereader.Reader
	var readersToClose []filereader.Reader // Keep track for cleanup

	for _, fileInfo := range validFiles {
		// Create stacked reader for this file: ProtoReader -> Translation -> Sorting
		var reader filereader.Reader

		// Step 2a: Create base proto reader directly (only support binpb/binpb.gz for metrics)
		reader, err = createMetricProtoReader(fileInfo.tmpfilename)
		if err != nil {
			ll.Warn("Failed to create proto reader, skipping file",
				slog.String("objectID", fileInfo.item.ObjectID),
				slog.String("error", err.Error()))
			continue
		}

		// Step 2b: Add translation (adds TID and truncates timestamp)
		translator := &metricsprocessing.MetricTranslator{
			OrgID:    firstItem.OrganizationID.String(),
			Bucket:   fileInfo.item.Bucket,
			ObjectID: fileInfo.item.ObjectID,
		}
		reader, err = filereader.NewTranslatingReader(reader, translator)
		if err != nil {
			reader.Close()
			ll.Warn("Failed to create translating reader, skipping file",
				slog.String("objectID", fileInfo.item.ObjectID),
				slog.String("error", err.Error()))
			continue
		}

		// Step 2c: Add disk-based sorting (after translation so TID is available)
		reader, err = filereader.NewDiskSortingReader(reader, filereader.MetricNameTidTimestampSortKeyFunc(), filereader.MetricNameTidTimestampSortFunc())
		if err != nil {
			reader.Close()
			ll.Warn("Failed to create sorting reader, skipping file",
				slog.String("objectID", fileInfo.item.ObjectID),
				slog.String("error", err.Error()))
			continue
		}

		readers = append(readers, reader)
		readersToClose = append(readersToClose, reader)
	}

	// Cleanup function for readers
	defer func() {
		for _, reader := range readersToClose {
			if closeErr := reader.Close(); closeErr != nil {
				ll.Warn("Failed to close reader during cleanup", slog.Any("error", closeErr))
			}
		}
	}()

	if len(readers) == 0 {
		ll.Warn("No valid readers created for batch")
		results, err := wm.closeAll(ctx)
		if err != nil {
			return fmt.Errorf("failed to close writers: %w", err)
		}
		ll.Debug("Batch processing summary", slog.Int("outputFiles", len(results)))
		return nil
	}

	ll.Debug("Created readers for batch", slog.Int("readerCount", len(readers)))

	// Step 3: Set up multi-source reader to merge sorted streams
	var finalReader filereader.Reader

	if len(readers) == 1 {
		// Single reader - no need for multi-source reader
		finalReader = readers[0]
	} else {
		// Multiple readers - use PreorderedMultisourceReader to merge sorted streams
		selector := createMetricOrderSelector()
		multiReader, err := filereader.NewPreorderedMultisourceReader(readers, selector)
		if err != nil {
			return fmt.Errorf("failed to create multi-source reader: %w", err)
		}
		finalReader = multiReader
	}

	// Step 4: Add top-level aggregation for cross-file aggregation
	finalReader, err = filereader.NewAggregatingMetricsReader(finalReader, 10000) // 10 seconds
	if err != nil {
		return fmt.Errorf("failed to create aggregating reader: %w", err)
	}

	// Process exemplars if available (from first file for now)
	if loop.exemplarProcessor != nil {
		if len(validFiles) > 0 {
			// Create a separate reader just for exemplar processing from first file
			exemplarReader, err := createMetricProtoReader(validFiles[0].tmpfilename)
			if err == nil {
				if err := processExemplarsFromReader(ctx, exemplarReader, loop.exemplarProcessor, firstItem.OrganizationID.String(), mdb); err != nil {
					ll.Warn("Failed to process exemplars", slog.Any("error", err))
				}
				exemplarReader.Close()
			}
		}
	}

	// Step 5: Process all rows from the unified pipeline
	rows := make([]filereader.Row, 100)
	for i := range rows {
		rows[i] = make(filereader.Row)
	}

	for {
		n, readErr := finalReader.Read(rows)

		if readErr != nil && readErr != io.EOF {
			return fmt.Errorf("failed to read from unified pipeline: %w", readErr)
		}

		// Process any rows we got (safe because either no error or EOF with final data)
		for i := range n {
			if rows[i] == nil {
				continue
			}
			processErr := wm.processRow(rows[i])
			if processErr != nil {
				batchRowsErrored++
			} else {
				batchRowsProcessed++
			}
		}

		if readErr == io.EOF {
			break
		}
	}

	batchRowsRead = finalReader.TotalRowsReturned()

	ll.Debug("Batch processing completed",
		slog.Int64("rowsRead", batchRowsRead),
		slog.Int64("rowsProcessed", batchRowsProcessed),
		slog.Int64("rowsErrored", batchRowsErrored))

	if batchRowsErrored > 0 {
		ll.Warn("Some rows were dropped due to processing errors",
			slog.Int64("rowsErrored", batchRowsErrored))
	}

	// Close all writers and get results
	results, err := wm.closeAll(ctx)
	if err != nil {
		return fmt.Errorf("failed to close writers: %w", err)
	}

	ll.Debug("Batch processing summary",
		slog.Int64("inputRowsRead", batchRowsRead),
		slog.Int64("inputRowsProcessed", batchRowsProcessed),
		slog.Int64("inputRowsErrored", batchRowsErrored),
		slog.Int("outputFiles", len(results)))

	// Upload results and update database
	uploadParams := metricsprocessing.UploadParams{
		OrganizationID: firstItem.OrganizationID.String(),
		InstanceNum:    firstItem.InstanceNum,
		Dateint:        0,     // Will be calculated from timestamps
		FrequencyMs:    10000, // 10 second blocks
		IngestDateint:  ingest_dateint,
		CollectorName:  firstItem.CollectorName,
		Bucket:         firstItem.Bucket,
		CreatedBy:      lrdb.CreatedByIngest,
	}

	if err := metricsprocessing.UploadMetricResults(ctx, ll, s3client, mdb, results, uploadParams); err != nil {
		return fmt.Errorf("failed to upload results: %w", err)
	}

	var totalOutputRecords int64
	for _, result := range results {
		totalOutputRecords += result.RecordCount
	}

	if len(results) == 0 {
		ll.Warn("No output files generated despite reading rows",
			slog.Int64("rowsRead", batchRowsRead),
			slog.Int64("rowsErrored", batchRowsErrored))
		return nil
	}

	if batchRowsErrored > 0 {
		ll.Warn("Some input rows were dropped due to processing errors",
			slog.Int64("totalDropped", batchRowsErrored),
			slog.Float64("dropRate", float64(batchRowsErrored)/float64(batchRowsRead)*100))
	}

	// Queue compaction and rollup for each time range represented in the results
	if err := queueMetricWorkForResults(ctx, mdb, firstItem, results); err != nil {
		return fmt.Errorf("failed to queue metric work: %w", err)
	}

	return nil
}

// queueMetricWorkForResults queues compaction and rollup work for the time ranges in results
func queueMetricWorkForResults(ctx context.Context, mdb lrdb.StoreFull, inf lrdb.Inqueue, results []parquetwriter.Result) error {
	// The new path writes 60s files, but we need to queue work for 10s frequency
	// so compaction can group multiple 10s logical blocks within the 60s boundary
	const frequency10s = int32(10000) // 10 seconds - the base frequency we're ingesting

	// Collect all 10-second blocks covered by our results
	blocksToQueue := make(map[int64]bool)

	for _, result := range results {
		if stats, ok := result.Metadata.(factories.MetricsFileStats); ok {
			// Calculate which 10-second blocks this file covers
			startBlock := stats.FirstTS / int64(frequency10s)
			endBlock := stats.LastTS / int64(frequency10s)

			// Mark all 10-second blocks that need queueing
			for block := startBlock; block <= endBlock; block++ {
				blocksToQueue[block] = true
			}
		}
	}

	// Queue compaction and rollup for each unique 10-second block
	for block := range blocksToQueue {
		blockStartTS := block * int64(frequency10s)
		qmcData := qmcFromInqueue(inf, frequency10s, blockStartTS)

		// Queue compaction for 10s frequency (will compact within 60s boundary)
		if err := queueMetricCompaction(ctx, mdb, qmcData); err != nil {
			return fmt.Errorf("queueing metric compaction for 10s block %d: %w", block, err)
		}

		// Queue rollup for 60s frequency (will rollup 10s data to 60s)
		if err := queueMetricRollup(ctx, mdb, qmcData); err != nil {
			return fmt.Errorf("queueing metric rollup for 10s block %d: %w", block, err)
		}
	}

	return nil
}

// isSupportedMetricsFile checks if the file is a supported metrics file type
func isSupportedMetricsFile(objectID string) bool {
	// Only support .binpb and .binpb.gz files from otel-raw/ path structure
	if !strings.HasPrefix(objectID, "otel-raw/") {
		return false
	}

	return strings.HasSuffix(objectID, ".binpb") || strings.HasSuffix(objectID, ".binpb.gz")
}

// createMetricOrderSelector creates a selector function for metrics that orders by [metric_name, tid, timestamp]
func createMetricOrderSelector() filereader.SelectFunc {
	return func(rows []filereader.Row) int {
		if len(rows) == 0 {
			return 0
		}

		bestIdx := 0
		bestRow := rows[0]
		if bestRow == nil {
			return bestIdx
		}

		// Extract comparison values from the best row
		bestName, _ := bestRow["_cardinalhq.name"].(string)
		bestTid, _ := bestRow["_cardinalhq.tid"].(int64)
		bestTs, _ := bestRow["_cardinalhq.timestamp"].(int64)

		for i := 1; i < len(rows); i++ {
			if rows[i] == nil {
				continue
			}

			// Extract comparison values from current row
			name, _ := rows[i]["_cardinalhq.name"].(string)
			tid, _ := rows[i]["_cardinalhq.tid"].(int64)
			ts, _ := rows[i]["_cardinalhq.timestamp"].(int64)

			// Compare by [name, tid, timestamp] in ascending order
			if name < bestName ||
				(name == bestName && tid < bestTid) ||
				(name == bestName && tid == bestTid && ts < bestTs) {
				bestIdx = i
				bestName = name
				bestTid = tid
				bestTs = ts
			}
		}

		return bestIdx
	}
}

// createMetricProtoReader creates a protocol buffer reader for metrics files
func createMetricProtoReader(filename string) (filereader.Reader, error) {
	// Only support .binpb and .binpb.gz files
	switch {
	case strings.HasSuffix(filename, ".binpb.gz"):
		return createMetricProtoBinaryGzReader(filename)
	case strings.HasSuffix(filename, ".binpb"):
		return createMetricProtoBinaryReader(filename)
	default:
		return nil, fmt.Errorf("unsupported metrics file type: %s (only .binpb and .binpb.gz are supported)", filename)
	}
}

// createMetricProtoBinaryReader creates a metrics proto reader for a protobuf file
func createMetricProtoBinaryReader(filename string) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf file: %w", err)
	}

	reader, err := filereader.NewIngestProtoMetricsReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create metrics proto reader: %w", err)
	}

	return reader, nil
}

// createMetricProtoBinaryGzReader creates a metrics proto reader for a gzipped protobuf file
func createMetricProtoBinaryGzReader(filename string) (filereader.Reader, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open protobuf.gz file: %w", err)
	}

	gzipReader, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}

	reader, err := filereader.NewIngestProtoMetricsReader(gzipReader)
	if err != nil {
		gzipReader.Close()
		file.Close()
		return nil, fmt.Errorf("failed to create metrics proto reader: %w", err)
	}

	return reader, nil
}
