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
			if os.Getenv("LAKERUNNER_METRIC_OLDPATH") != "" {
				return runOldMetricIngestion(doneCtx, slog.Default(), loop)
			}

			return IngestLoopWithBatch(loop, metricIngestItem, metricIngestBatch)
		},
	}

	rootCmd.AddCommand(cmd)
}

// MetricTranslator adds resource metadata to metric rows
type MetricTranslator struct {
	orgID    string
	bucket   string
	objectID string
}

// TranslateRow adds resource fields to each row
// Assumes all other metric fields (including sketches) are properly set by the proto reader
func (t *MetricTranslator) TranslateRow(row *filereader.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Only set the specific required fields - assume all other fields are properly set
	(*row)["resource.bucket.name"] = t.bucket
	(*row)["resource.file.name"] = "./" + t.objectID
	(*row)["_cardinalhq.customer_id"] = t.orgID
	(*row)["_cardinalhq.telemetry_type"] = "metrics"

	return nil
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
		float64(wm.rpfEstimate),
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

	// Process each file in the batch
	for _, inf := range items {

		// Skip database files (processed outputs, not inputs)
		if strings.HasPrefix(inf.ObjectID, "db/") {
			ll.Debug("Skipping database file", slog.String("objectID", inf.ObjectID))
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

		// Create appropriate reader for the file type
		var reader filereader.Reader

		reader, err = createMetricReader(tmpfilename)
		if err == nil {
			// Add translator for metrics
			translator := &MetricTranslator{
				orgID:    firstItem.OrganizationID.String(),
				bucket:   inf.Bucket,
				objectID: inf.ObjectID,
			}
			reader, err = filereader.NewTranslatingReader(reader, translator)

			// Process exemplars if available
			if loop.exemplarProcessor != nil {
				if err := processExemplarsFromReader(ctx, reader, loop.exemplarProcessor, firstItem.OrganizationID.String(), mdb); err != nil {
					ll.Warn("Failed to process exemplars", slog.Any("error", err))
				}
			}
		}

		if err != nil {
			ll.Warn("Unsupported or problematic file type, skipping",
				slog.String("objectID", inf.ObjectID),
				slog.String("error", err.Error()))
			continue
		}

		// Process all rows from the file
		rows := make([]filereader.Row, 100)
		for i := range rows {
			rows[i] = make(filereader.Row)
		}
		var processedCount, errorCount int64
		for {
			n, err := reader.Read(rows)

			// Process any rows we got, even if EOF
			for i := range n {
				if rows[i] == nil {
					continue
				}
				err := wm.processRow(rows[i])
				if err != nil {
					errorCount++
					// Continue processing other rows instead of failing the entire batch
				} else {
					processedCount++
				}
			}

			// Break after processing if we hit EOF or other errors
			if err == io.EOF {
				break
			}
			if err != nil {
				if closeErr := reader.Close(); closeErr != nil {
					ll.Warn("Failed to close reader after read error", slog.String("objectID", inf.ObjectID), slog.Any("error", closeErr))
				}
				return fmt.Errorf("failed to read from file %s: %w", inf.ObjectID, err)
			}
		}

		// Get total rows read from the reader
		fileRowsRead := reader.RowCount()

		ll.Debug("File processing completed",
			slog.String("objectID", inf.ObjectID),
			slog.Int64("rowsRead", fileRowsRead),
			slog.Int64("rowsProcessed", processedCount),
			slog.Int64("rowsErrored", errorCount))

		if errorCount > 0 {
			ll.Warn("Some rows were dropped due to processing errors",
				slog.String("objectID", inf.ObjectID),
				slog.Int64("droppedRows", errorCount),
				slog.Float64("dropRate", float64(errorCount)/float64(fileRowsRead)*100))
		}
		if closeErr := reader.Close(); closeErr != nil {
			ll.Warn("Failed to close reader", slog.String("objectID", inf.ObjectID), slog.Any("error", closeErr))
		}

		// Update batch totals
		batchRowsRead += fileRowsRead
		batchRowsProcessed += processedCount
		batchRowsErrored += errorCount

		ll.Debug("Completed processing file", slog.String("objectID", inf.ObjectID))

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
	var totalOutputRecords int64
	for _, result := range results {
		if err := uploadMetricResultToS3AndDB(ctx, ll, result, firstItem, s3client, mdb, ingest_dateint); err != nil {
			return fmt.Errorf("failed to upload result to S3: %w", err)
		}
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

	return nil
}

// createMetricReader creates the appropriate filereader for metrics based on file type
func createMetricReader(filename string) (filereader.Reader, error) {
	return filereader.ReaderForFile(filename, filereader.SignalTypeMetrics)
}

// uploadMetricResultToS3AndDB uploads a result file to S3 and updates the database
func uploadMetricResultToS3AndDB(ctx context.Context, ll *slog.Logger, result parquetwriter.Result, inf lrdb.Inqueue, s3client *awsclient.S3Client, mdb lrdb.StoreFull, ingest_dateint int32) error {
	// Safety check: should never get empty results from the splitter
	if result.RecordCount == 0 {
		ll.Error("Received empty result from writer - this should not happen",
			slog.String("fileName", result.FileName),
			slog.Int64("recordCount", result.RecordCount))
		return fmt.Errorf("received empty result file with 0 records")
	}
	// Generate segment ID and object ID
	segmentID := s3helper.GenerateID()

	// Use ingest dateint for now - would extract from data in full implementation
	dateint := ingest_dateint
	hour := int16(0) // Simplified for now

	objID := helpers.MakeDBObjectID(inf.OrganizationID, inf.CollectorName, dateint, hour, segmentID, "metrics")

	// Upload to S3
	if err := s3helper.UploadS3Object(ctx, s3client, inf.Bucket, objID, result.FileName); err != nil {
		return fmt.Errorf("uploading file to S3: %w", err)
	}

	// Use slot 0 for metrics for now
	slotID := 0

	// Extract fingerprints and timestamps from result metadata
	var fingerprints []int64
	var startTs, endTs int64
	if stats, ok := result.Metadata.(factories.MetricsFileStats); ok {
		fingerprints = stats.Fingerprints
		startTs = stats.FirstTS
		// Database expects start-inclusive, end-exclusive range [start, end)
		// So endTs should be LastTS + 1 to include the last timestamp
		endTs = stats.LastTS + 1

		// Validate timestamp range
		if startTs == 0 || stats.LastTS == 0 || startTs > stats.LastTS {
			ll.Error("Invalid timestamp range in metrics file stats",
				slog.Int64("startTs", startTs),
				slog.Int64("lastTs", stats.LastTS),
				slog.Int64("endTs", endTs),
				slog.Int64("recordCount", result.RecordCount))
			return fmt.Errorf("invalid timestamp range: startTs=%d, lastTs=%d", startTs, stats.LastTS)
		}

		ll.Debug("Metric segment stats",
			slog.Int("fingerprintCount", len(fingerprints)),
			slog.Int64("startTs", startTs),
			slog.Int64("endTs", endTs))
	} else {
		ll.Error("Failed to extract MetricsFileStats from result metadata",
			slog.String("metadataType", fmt.Sprintf("%T", result.Metadata)))
		return fmt.Errorf("missing or invalid MetricsFileStats in result metadata")
	}

	// Insert segment record
	err := mdb.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: inf.OrganizationID,
		FrequencyMs:    10000, // 10 second blocks
		Dateint:        dateint,
		IngestDateint:  ingest_dateint,
		TidPartition:   0,
		SegmentID:      segmentID,
		InstanceNum:    inf.InstanceNum,
		SlotID:         int32(slotID),
		StartTs:        startTs,
		EndTs:          endTs,
		RecordCount:    result.RecordCount,
		FileSize:       result.FileSize,
		Published:      true,
		CreatedBy:      lrdb.CreatedByIngest,
		Fingerprints:   fingerprints,
	})
	if err != nil {
		// Clean up uploaded file on database error
		if err2 := s3helper.DeleteS3Object(ctx, s3client, inf.Bucket, objID); err2 != nil {
			ll.Error("Failed to delete S3 object after insertion failure", slog.Any("error", err2))
		}
		return fmt.Errorf("inserting metric segment: %w", err)
	}

	return nil
}
