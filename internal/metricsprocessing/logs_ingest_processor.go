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

package metricsprocessing

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/internal/workqueue"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/configservice"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// DateintBin represents a file group containing logs for a specific dateint
type DateintBin struct {
	Dateint int32 // The dateint for this bin
	Writer  parquetwriter.ParquetWriter
	Results []parquetwriter.Result // Results after writer is closed (can be multiple files)
}

// DateintBinManager manages multiple file groups, one per dateint
type DateintBinManager struct {
	bins        map[int32]*DateintBin // Key is dateint
	tmpDir      string
	rpfEstimate int64
	schema      *filereader.ReaderSchema
	backendType parquetwriter.BackendType
	streamField string // Field to use for stream identification
}

// LogIngestProcessor implements the Processor interface for raw log ingestion
type LogIngestProcessor struct {
	store                    LogIngestStore
	storageProvider          storageprofile.StorageProfileProvider
	cmgr                     cloudstorage.ClientProvider
	kafkaProducer            fly.Producer
	exemplarProcessor        *exemplars.Processor
	fingerprintTenantManager *fingerprint.TenantManager
	config                   *config.Config
}

// newLogIngestProcessor creates a new log ingest processor instance
func newLogIngestProcessor(
	cfg *config.Config,
	store LogIngestStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, kafkaProducer fly.Producer) *LogIngestProcessor {
	exemplarProcessor := exemplars.NewProcessor(exemplars.DefaultConfig())
	exemplarProcessor.SetLogsCallback(func(ctx context.Context, organizationID uuid.UUID, rows []pipeline.Row) error {
		return processLogsExemplarsDirect(ctx, organizationID, rows, store)
	})

	fingerprintTenantManager := fingerprint.NewTenantManager(0.5)

	return &LogIngestProcessor{
		store:                    store,
		storageProvider:          storageProvider,
		cmgr:                     cmgr,
		kafkaProducer:            kafkaProducer,
		exemplarProcessor:        exemplarProcessor,
		fingerprintTenantManager: fingerprintTenantManager,
		config:                   cfg,
	}
}

// validateLogIngestMessages validates the key and messages for consistency
func validateLogIngestMessages(key messages.IngestKey, msgs []*messages.ObjStoreNotificationMessage) error {
	if len(msgs) == 0 {
		return &GroupValidationError{
			Field:   "message_count",
			Message: "message list cannot be empty",
		}
	}

	// Get expected values from the key
	expectedOrg := key.OrganizationID
	expectedInstance := key.InstanceNum

	// Validate each message against the expected values
	for i, msg := range msgs {
		if msg.OrganizationID != expectedOrg {
			return &GroupValidationError{
				Field:    "organization_id",
				Expected: expectedOrg,
				Got:      msg.OrganizationID,
				Message:  fmt.Sprintf("message %d has inconsistent organization ID", i),
			}
		}

		if msg.InstanceNum != expectedInstance {
			return &GroupValidationError{
				Field:    "instance_num",
				Expected: expectedInstance,
				Got:      msg.InstanceNum,
				Message:  fmt.Sprintf("message %d has inconsistent instance number", i),
			}
		}
	}

	return nil
}

// Process implements the Processor interface and performs raw log ingestion
func (p *LogIngestProcessor) ProcessBundle(ctx context.Context, key messages.IngestKey, msgs []*messages.ObjStoreNotificationMessage, partition int32, offset int64) error {
	ll := logctx.FromContext(ctx).With(
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("instanceNum", int(key.InstanceNum)))

	ll.Info("Starting log ingestion",
		slog.Int("messageCount", len(msgs)))

	if err := validateLogIngestMessages(key, msgs); err != nil {
		return fmt.Errorf("message validation failed: %w", err)
	}

	// Create temporary directory for this ingestion run
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			ll.Warn("Failed to cleanup temporary directory", slog.String("tmpDir", tmpDir), slog.Any("error", cleanupErr))
		}
	}()

	ctx, storageSpan := boxerTracer.Start(ctx, "logs.ingest.setup_storage", trace.WithAttributes(
		attribute.String("organization_id", key.OrganizationID.String()),
		attribute.Int("instance_num", int(key.InstanceNum)),
	))

	srcProfile, err := p.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, key.OrganizationID, key.InstanceNum)
	if err != nil {
		storageSpan.RecordError(err)
		storageSpan.SetStatus(codes.Error, "failed to get storage profile")
		storageSpan.End()
		return fmt.Errorf("get storage profile: %w", err)
	}

	inputClient, err := cloudstorage.NewClient(ctx, p.cmgr, srcProfile)
	if err != nil {
		storageSpan.RecordError(err)
		storageSpan.SetStatus(codes.Error, "failed to create input client")
		storageSpan.End()
		return fmt.Errorf("create storage client: %w", err)
	}

	dstProfile := srcProfile
	if p.config.Logs.Ingestion.SingleInstanceMode {
		dstProfile, err = p.storageProvider.GetLowestInstanceStorageProfile(ctx, srcProfile.OrganizationID, srcProfile.Bucket)
		if err != nil {
			storageSpan.RecordError(err)
			storageSpan.SetStatus(codes.Error, "failed to get lowest instance profile")
			storageSpan.End()
			return fmt.Errorf("get lowest instance storage profile: %w", err)
		}
	}

	outputClient, err := cloudstorage.NewClient(ctx, p.cmgr, dstProfile)
	if err != nil {
		storageSpan.RecordError(err)
		storageSpan.SetStatus(codes.Error, "failed to create output client")
		storageSpan.End()
		return fmt.Errorf("create storage client: %w", err)
	}
	storageSpan.End()

	// Get stream field config for this organization
	streamField := configservice.Global().GetLogStreamConfig(ctx, key.OrganizationID).FieldName

	var readers []filereader.Reader
	var readersToClose []filereader.Reader
	var totalInputSize int64

	ctx, downloadSpan := boxerTracer.Start(ctx, "logs.ingest.download_files", trace.WithAttributes(
		attribute.Int("file_count", len(msgs)),
	))

	for _, msg := range msgs {

		ll.Debug("Processing raw log file",
			slog.String("objectID", msg.ObjectID),
			slog.Int64("fileSize", msg.FileSize))

		tmpFilename, _, is404, err := inputClient.DownloadObject(ctx, tmpDir, msg.Bucket, msg.ObjectID)
		if err != nil {
			ll.Error("Failed to download file", slog.String("objectID", msg.ObjectID), slog.Any("error", err))
			continue // Skip this file but continue with others
		}
		if is404 {
			ll.Warn("Object not found, skipping", slog.String("objectID", msg.ObjectID))
			continue
		}

		reader, err := p.createLogReaderStack(tmpFilename, msg.OrganizationID.String(), msg.Bucket, msg.ObjectID, streamField)
		if err != nil {
			ll.Error("Failed to create reader stack", slog.String("objectID", msg.ObjectID), slog.Any("error", err))
			continue
		}

		readers = append(readers, reader)
		readersToClose = append(readersToClose, reader)
		totalInputSize += msg.FileSize
	}

	downloadSpan.SetAttributes(
		attribute.Int("files_downloaded", len(readers)),
		attribute.Int64("total_input_size", totalInputSize),
	)
	downloadSpan.End()

	defer func() {
		for _, reader := range readersToClose {
			if closeErr := reader.Close(); closeErr != nil {
				ll.Warn("Failed to close reader during cleanup", slog.Any("error", closeErr))
			}
		}
	}()

	if len(readers) == 0 {
		ll.Info("No files processed successfully")
		return nil
	}

	ctx, readerSpan := boxerTracer.Start(ctx, "logs.ingest.create_unified_reader")
	finalReader, err := p.createUnifiedLogReader(ctx, readers, streamField)
	if err != nil {
		readerSpan.RecordError(err)
		readerSpan.SetStatus(codes.Error, "failed to create unified reader")
		readerSpan.End()
		return fmt.Errorf("failed to create unified reader: %w", err)
	}
	readerSpan.End()

	ctx, processSpan := boxerTracer.Start(ctx, "logs.ingest.process_rows")
	dateintBins, err := p.processRowsWithDateintBinning(ctx, finalReader, tmpDir, srcProfile)
	if err != nil {
		processSpan.RecordError(err)
		processSpan.SetStatus(codes.Error, "failed to process rows")
		processSpan.End()
		return fmt.Errorf("failed to process rows: %w", err)
	}
	processSpan.SetAttributes(attribute.Int("dateint_bins", len(dateintBins)))
	processSpan.End()

	if len(dateintBins) == 0 {
		ll.Info("No output files generated")
		return nil
	}

	// Get schema from reader for label name mapping
	schema := finalReader.GetSchema()

	ctx, uploadSpan := boxerTracer.Start(ctx, "logs.ingest.upload_segments")
	segmentParams, err := p.uploadAndCreateLogSegments(ctx, outputClient, dateintBins, schema, dstProfile)
	if err != nil {
		uploadSpan.RecordError(err)
		uploadSpan.SetStatus(codes.Error, "failed to upload segments")
		uploadSpan.End()
		return fmt.Errorf("failed to upload and create segments: %w", err)
	}
	uploadSpan.SetAttributes(attribute.Int("segment_count", len(segmentParams)))
	uploadSpan.End()

	criticalCtx := context.WithoutCancel(ctx)
	ctx, dbSpan := boxerTracer.Start(ctx, "logs.ingest.insert_segments", trace.WithAttributes(
		attribute.Int("segment_count", len(segmentParams)),
	))

	if err := p.store.InsertLogSegmentsBatch(criticalCtx, segmentParams); err != nil {
		segmentIDs := make([]int64, len(segmentParams))
		var totalRecords, totalSize int64
		for i, seg := range segmentParams {
			segmentIDs[i] = seg.SegmentID
			totalRecords += seg.RecordCount
			totalSize += seg.FileSize
		}

		dbSpan.RecordError(err)
		dbSpan.SetStatus(codes.Error, "failed to insert segments")
		dbSpan.End()

		ll.Error("Failed to insert log segments with Kafka offsets",
			slog.Any("error", err),
			slog.Int("segmentCount", len(segmentParams)),
			slog.Int64("totalRecords", totalRecords),
			slog.Int64("totalSize", totalSize),
			slog.Any("segment_ids", segmentIDs))

		return fmt.Errorf("failed to insert log segments with Kafka offsets: %w", err)
	}
	dbSpan.End()

	_, kafkaSpan := boxerTracer.Start(ctx, "logs.ingest.publish_compaction", trace.WithAttributes(
		attribute.Int("message_count", len(segmentParams)),
	))

	compactionTopic := p.config.TopicRegistry.GetTopic(config.TopicBoxerLogsCompact)

	for _, segParams := range segmentParams {
		compactionNotification := messages.LogCompactionMessage{
			Version:        1,
			OrganizationID: segParams.OrganizationID,
			DateInt:        segParams.Dateint,
			SegmentID:      segParams.SegmentID,
			InstanceNum:    segParams.InstanceNum,
			Records:        segParams.RecordCount,
			FileSize:       segParams.FileSize,
			StartTs:        segParams.StartTs,
			EndTs:          segParams.EndTs,
			QueuedAt:       time.Now(),
		}

		compactionMsgBytes, err := compactionNotification.Marshal()
		if err != nil {
			kafkaSpan.RecordError(err)
			kafkaSpan.SetStatus(codes.Error, "failed to marshal compaction message")
			kafkaSpan.End()
			return fmt.Errorf("failed to marshal log compaction notification: %w", err)
		}

		compactionMessage := fly.Message{
			Key:   fmt.Appendf(nil, "%s-%d-%d", segParams.OrganizationID.String(), segParams.InstanceNum, segParams.StartTs/300000),
			Value: compactionMsgBytes,
		}

		if err := p.kafkaProducer.Send(criticalCtx, compactionTopic, compactionMessage); err != nil {
			kafkaSpan.RecordError(err)
			kafkaSpan.SetStatus(codes.Error, "failed to send to kafka")
			kafkaSpan.End()
			return fmt.Errorf("failed to send log compaction notification to Kafka: %w", err)
		} else {
			ll.Debug("Sent log compaction notification", slog.Any("message", compactionNotification))
		}
		kafkaSpan.End()
	}

	// Calculate output metrics for telemetry
	var totalOutputRecords, totalOutputSize int64
	for _, params := range segmentParams {
		totalOutputRecords += params.RecordCount
		totalOutputSize += params.FileSize
	}

	reportTelemetry(ctx, "logs", "ingestion", int64(len(msgs)), int64(len(segmentParams)), 0, totalOutputRecords, totalInputSize, totalOutputSize)

	ll.Info("Log ingestion completed successfully",
		slog.Int("inputFiles", len(msgs)),
		slog.Int64("totalFileSize", totalInputSize),
		slog.Int("outputSegments", len(segmentParams)))

	return nil
}

// ProcessBundleFromQueue implements the BundleProcessor interface for work queue integration
func (p *LogIngestProcessor) ProcessBundleFromQueue(ctx context.Context, workItem workqueue.Workable) error {
	ll := logctx.FromContext(ctx)

	// Extract bundle from work item spec
	var bundle messages.LogIngestBundle
	specBytes, err := json.Marshal(workItem.Spec())
	if err != nil {
		return fmt.Errorf("failed to marshal work item spec: %w", err)
	}

	if err := json.Unmarshal(specBytes, &bundle); err != nil {
		return fmt.Errorf("failed to unmarshal log ingest bundle: %w", err)
	}

	if len(bundle.Messages) == 0 {
		ll.Info("Skipping empty bundle")
		return nil
	}

	// Extract key from first message
	firstMsg := bundle.Messages[0]
	key := firstMsg.GroupingKey().(messages.IngestKey)

	// Call the existing ProcessBundle with 0 for partition and offset (not needed anymore)
	return p.ProcessBundle(ctx, key, bundle.Messages, 0, 0)
}

// GetTargetRecordCount returns the target file size limit (5MB) for accumulation
func (p *LogIngestProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.IngestKey) int64 {
	return 5 * 1024 * 1024 // 5MB file size limit instead of record count
}

// ShouldEmitImmediately returns false - log ingest processing always uses normal grouping.
func (p *LogIngestProcessor) ShouldEmitImmediately(msg *messages.ObjStoreNotificationMessage) bool {
	return false
}

// LogIngestionResult contains the results of processing log files
type LogIngestionResult struct {
	OutputFiles      []parquetwriter.Result // Parquet files generated
	TotalRecords     int64                  // Total number of log records processed
	TotalInputBytes  int64                  // Total size of input files
	TotalOutputBytes int64                  // Total size of output files
	SegmentParams    []lrdb.InsertLogSegmentParams
}

// ProcessLogFiles performs end-to-end log ingestion on a set of files.
// This function encapsulates the complete ingestion pipeline and can be called
// from both the production processor and benchmarks.
//
// Parameters:
//   - ctx: Context for cancellation and logging
//   - filePaths: List of file paths to process (can be local files)
//   - orgID: Organization ID for metadata
//   - bucket: Bucket name for metadata
//   - outputDir: Directory to write output Parquet files
//   - rpfEstimate: Rows-per-file estimate for Parquet writer sizing
//   - fingerprintManager: Optional fingerprint manager (can be nil for benchmarks)
//   - backendType: Parquet backend type (empty string defaults to go-parquet)
//   - streamField: Field to use for stream identification (empty = use default priority)
//
// Returns:
//   - LogIngestionResult with statistics and output file information
//   - error if any step fails
func ProcessLogFiles(
	ctx context.Context,
	filePaths []string,
	orgID string,
	bucket string,
	outputDir string,
	rpfEstimate int64,
	fingerprintManager *fingerprint.TenantManager,
	backendType parquetwriter.BackendType,
	streamField string,
) (*LogIngestionResult, error) {
	ll := logctx.FromContext(ctx)

	var readers []filereader.Reader
	var totalInputSize int64

	// Create readers for all files
	for _, filePath := range filePaths {
		stat, err := os.Stat(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to stat file %s: %w", filePath, err)
		}
		totalInputSize += stat.Size()

		reader, err := createLogReaderStackStandalone(filePath, orgID, bucket, filePath, fingerprintManager, streamField)
		if err != nil {
			// Close any readers we've already created
			for _, r := range readers {
				_ = r.Close()
			}
			return nil, fmt.Errorf("failed to create reader for %s: %w", filePath, err)
		}
		readers = append(readers, reader)
	}

	defer func() {
		for _, reader := range readers {
			if closeErr := reader.Close(); closeErr != nil {
				ll.Warn("Failed to close reader during cleanup", slog.Any("error", closeErr))
			}
		}
	}()

	// Create unified reader - always use MergesortReader to ensure proper sorting
	keyProvider := filereader.NewLogSortKeyProvider(streamField)
	finalReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to create multi-source reader: %w", err)
	}

	// Get schema from reader (includes all transformed columns)
	schema := finalReader.GetSchema()

	// Add chq_id column (injected by FileSplitter when writing rows)
	schema.AddColumn(wkk.RowKeyCID, wkk.RowKeyCID, filereader.DataTypeString, true)

	// Create dateint bin manager
	binManager := &DateintBinManager{
		bins:        make(map[int32]*DateintBin),
		tmpDir:      outputDir,
		rpfEstimate: rpfEstimate,
		schema:      schema,
		backendType: backendType,
		streamField: streamField,
	}

	var totalRowsProcessed int64

	// Process all rows from the reader
	for {
		batch, readErr := finalReader.Next(ctx)
		if readErr != nil && readErr != io.EOF {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			return nil, fmt.Errorf("failed to read from unified pipeline: %w", readErr)
		}

		if batch != nil {
			// Process each row in the batch
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				if row == nil {
					continue
				}

				ts, ok := row[wkk.RowKeyCTimestamp].(int64)
				if !ok {
					ll.Warn("Row missing timestamp, skipping", slog.Int("rowIndex", i))
					continue
				}

				dateint, _ := helpers.MSToDateintHour(ts)
				bin, err := binManager.getOrCreateBin(ctx, dateint)
				if err != nil {
					ll.Error("Failed to get/create dateint bin", slog.Int("dateint", int(dateint)), slog.Any("error", err))
					continue
				}

				// Add fingerprint to row if manager is available
				if fingerprintManager != nil {
					message, ok := row[wkk.RowKeyCMessage].(string)
					if ok && message != "" {
						tenant := fingerprintManager.GetTenant(orgID)
						trieClusterManager := tenant.GetTrieClusterManager()
						fp, _, err := fingerprinter.Fingerprint(message, trieClusterManager)
						if err == nil {
							row[wkk.RowKeyCFingerprint] = fp
						}
					}
				}

				takenRow := batch.TakeRow(i)
				if takenRow == nil {
					continue
				}
				singleRowBatch := pipeline.GetBatch()
				singleRowBatch.AppendRow(takenRow)

				if err := bin.Writer.WriteBatch(singleRowBatch); err != nil {
					ll.Error("Failed to write row to dateint bin",
						slog.Int("dateint", int(dateint)),
						slog.Any("error", err))
				} else {
					totalRowsProcessed++
				}

				pipeline.ReturnBatch(singleRowBatch)
			}
			pipeline.ReturnBatch(batch)
		}

		if readErr == io.EOF {
			break
		}
	}

	ll.Info("Log binning completed",
		slog.Int64("rowsProcessed", totalRowsProcessed),
		slog.Int("dateintBinsCreated", len(binManager.bins)))

	// Close all writers and collect results
	var allResults []parquetwriter.Result
	var totalOutputBytes int64

	for binDateint, bin := range binManager.bins {
		results, err := bin.Writer.Close(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to close writer for bin %d: %w", binDateint, err)
		}

		if len(results) > 0 {
			bin.Results = append(bin.Results, results...)
			allResults = append(allResults, results...)
			for _, res := range results {
				totalOutputBytes += res.FileSize
			}
		}
	}

	return &LogIngestionResult{
		OutputFiles:      allResults,
		TotalRecords:     totalRowsProcessed,
		TotalInputBytes:  totalInputSize,
		TotalOutputBytes: totalOutputBytes,
	}, nil
}

// createLogReaderStackStandalone creates a reader stack for standalone use (benchmarks, tests)
// Returns DiskSorter(Translation(LogReader(file))) - raw files are unsorted.
func createLogReaderStackStandalone(filename, orgID, bucket, objectID string, fingerprintManager *fingerprint.TenantManager, streamField string) (filereader.Reader, error) {
	options := filereader.ReaderOptions{
		SignalType: filereader.SignalTypeLogs,
		BatchSize:  1000,
		OrgID:      orgID,
	}

	reader, err := filereader.ReaderForFileWithOptions(filename, options)
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(filename, ".parquet") {
		reader = NewParquetLogTranslatingReader(reader, orgID, bucket, objectID)
	} else {
		reader = NewLogTranslatingReader(reader, orgID, bucket, objectID, fingerprintManager)
	}

	// Raw input files are unsorted - wrap in DiskSortingReader to sort before mergesort
	keyProvider := filereader.NewLogSortKeyProvider(streamField)
	sortingReader, err := filereader.NewDiskSortingReader(reader, keyProvider, 1000)
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("failed to create sorting reader: %w", err)
	}

	return sortingReader, nil
}

// createLogReaderStack creates a reader stack: DiskSorter(Translation(LogReader(file)))
// Raw input files are unsorted, so each must be sorted before feeding to mergesort.
func (p *LogIngestProcessor) createLogReaderStack(tmpFilename, orgID, bucket, objectID, streamField string) (filereader.Reader, error) {
	// Determine file type from extension for logging
	var fileType string
	switch {
	case strings.HasSuffix(tmpFilename, ".parquet"):
		fileType = "parquet"
	case strings.HasSuffix(tmpFilename, ".json.gz"):
		fileType = "json.gz"
	case strings.HasSuffix(tmpFilename, ".json"):
		fileType = "json"
	case strings.HasSuffix(tmpFilename, ".binpb.gz"):
		fileType = "binpb.gz"
	case strings.HasSuffix(tmpFilename, ".binpb"):
		fileType = "binpb"
	case strings.HasSuffix(tmpFilename, ".csv.gz"):
		fileType = "csv.gz"
	case strings.HasSuffix(tmpFilename, ".csv"):
		fileType = "csv"
	default:
		fileType = "unknown"
	}

	slog.Info("Reading log file",
		"fileType", fileType,
		"objectID", objectID,
		"bucket", bucket)

	reader, err := p.createLogReader(tmpFilename, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to create log reader: %w", err)
	}

	if strings.HasSuffix(tmpFilename, ".parquet") {
		reader = NewParquetLogTranslatingReader(reader, orgID, bucket, objectID)
	} else {
		reader = NewLogTranslatingReader(reader, orgID, bucket, objectID, p.fingerprintTenantManager)
	}

	// Raw input files are unsorted - wrap in DiskSortingReader to sort before mergesort
	keyProvider := &filereader.LogSortKeyProvider{StreamField: streamField}
	sortingReader, err := filereader.NewDiskSortingReader(reader, keyProvider, 1000)
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("failed to create sorting reader: %w", err)
	}

	return sortingReader, nil
}

func (p *LogIngestProcessor) createLogReader(filename, orgId string) (filereader.Reader, error) {
	options := filereader.ReaderOptions{
		SignalType: filereader.SignalTypeLogs,
		BatchSize:  1000,
		OrgID:      orgId,
	}
	return filereader.ReaderForFileWithOptions(filename, options)
}

// createUnifiedLogReader creates a unified reader from multiple readers
// Always uses MergesortReader to ensure row normalization happens
func (p *LogIngestProcessor) createUnifiedLogReader(ctx context.Context, readers []filereader.Reader, streamField string) (filereader.Reader, error) {
	// Always use MergesortReader, even for single files
	// This ensures rows are normalized according to the schema before writing
	keyProvider := &filereader.LogSortKeyProvider{StreamField: streamField}
	multiReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to create multi-source reader: %w", err)
	}
	return multiReader, nil
}

// processRowsWithDateintBinning groups logs by dateint only (no aggregation, no time window)
func (p *LogIngestProcessor) processRowsWithDateintBinning(ctx context.Context, reader filereader.Reader, tmpDir string, storageProfile storageprofile.StorageProfile) (map[int32]*DateintBin, error) {
	ll := logctx.FromContext(ctx)

	// Get RPF estimate for this org/instance - use logs estimator logic
	rpfEstimate := p.store.GetLogEstimate(ctx, storageProfile.OrganizationID)

	// Get stream field config for this organization
	streamField := configservice.Global().GetLogStreamConfig(ctx, storageProfile.OrganizationID).FieldName

	// Create dateint bin manager
	// Get schema from reader (GetSchema returns a copy and includes all transformed columns)
	schema := reader.GetSchema()

	// Add chq_id column (injected by FileSplitter when writing rows)
	schema.AddColumn(wkk.RowKeyCID, wkk.RowKeyCID, filereader.DataTypeString, true)

	binManager := &DateintBinManager{
		bins:        make(map[int32]*DateintBin),
		tmpDir:      tmpDir,
		rpfEstimate: rpfEstimate,
		schema:      schema,
		backendType: parquetwriter.DefaultBackend,
		streamField: streamField,
	}

	var totalRowsProcessed int64

	// Process all rows from the reader
	for {
		batch, readErr := reader.Next(ctx)
		if readErr != nil && readErr != io.EOF {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			return nil, fmt.Errorf("failed to read from unified pipeline: %w", readErr)
		}

		if batch != nil {
			// Process each row in the batch
			for i := 0; i < batch.Len(); i++ {
				row := batch.Get(i)
				if row == nil {
					continue
				}

				ts, ok := row[wkk.RowKeyCTimestamp].(int64)
				if !ok {
					ll.Warn("Row missing timestamp, skipping", slog.Int("rowIndex", i))
					continue
				}

				dateint, _ := helpers.MSToDateintHour(ts)
				bin, err := binManager.getOrCreateBin(ctx, dateint)
				if err != nil {
					ll.Error("Failed to get/create dateint bin", slog.Int("dateint", int(dateint)), slog.Any("error", err))
					continue
				}

				// Add fingerprint to row
				message, ok := row[wkk.RowKeyCMessage].(string)
				if ok && message != "" {
					tenant := p.fingerprintTenantManager.GetTenant(storageProfile.OrganizationID.String())
					trieClusterManager := tenant.GetTrieClusterManager()
					fp, _, err := fingerprinter.Fingerprint(message, trieClusterManager)
					if err == nil {
						row[wkk.RowKeyCFingerprint] = fp
					}
				}

				// Process exemplar before taking the row
				if p.exemplarProcessor != nil {
					_ = p.exemplarProcessor.ProcessLogsFromRow(ctx, storageProfile.OrganizationID, row)
				}

				takenRow := batch.TakeRow(i)
				if takenRow == nil {
					continue
				}
				singleRowBatch := pipeline.GetBatch()
				singleRowBatch.AppendRow(takenRow)

				if err := bin.Writer.WriteBatch(singleRowBatch); err != nil {
					ll.Error("Failed to write row to dateint bin",
						slog.Int("dateint", int(dateint)),
						slog.Any("error", err))
				} else {
					totalRowsProcessed++
				}

				pipeline.ReturnBatch(singleRowBatch)
			}
			pipeline.ReturnBatch(batch)
		}

		if readErr == io.EOF {
			break
		}
	}

	ll.Info("Log binning completed",
		slog.Int64("rowsProcessed", totalRowsProcessed),
		slog.Int("dateintBinsCreated", len(binManager.bins)))

	// Close all writers and collect results
	for binDateint, bin := range binManager.bins {
		results, err := bin.Writer.Close(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to close writer for bin %d: %w", binDateint, err)
		}

		if len(results) > 0 {
			bin.Results = append(bin.Results, results...)
		}
	}

	return binManager.bins, nil
}

// getOrCreateBin gets or creates a dateint bin for the given dateint
func (manager *DateintBinManager) getOrCreateBin(_ context.Context, dateint int32) (*DateintBin, error) {
	if bin, exists := manager.bins[dateint]; exists {
		return bin, nil
	}

	// Create new writer for this dateint bin
	writer, err := factories.NewLogsWriter(manager.tmpDir, manager.schema, manager.rpfEstimate, manager.backendType, manager.streamField)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer for dateint bin: %w", err)
	}

	bin := &DateintBin{
		Dateint: dateint,
		Writer:  writer,
	}

	manager.bins[dateint] = bin
	return bin, nil
}

// uploadAndCreateLogSegments uploads dateint bins to S3 and creates segment parameters
func (p *LogIngestProcessor) uploadAndCreateLogSegments(ctx context.Context, storageClient cloudstorage.Client, dateintBins map[int32]*DateintBin, schema *filereader.ReaderSchema, storageProfile storageprofile.StorageProfile) ([]lrdb.InsertLogSegmentParams, error) {
	ll := logctx.FromContext(ctx)

	// Build label name map from schema once (used for all segments)
	schemaColumnMappings := make(map[string]string)
	for newKey, originalKey := range schema.GetAllOriginalNames() {
		schemaColumnMappings[wkk.RowKeyValue(newKey)] = wkk.RowKeyValue(originalKey)
	}
	labelNameMap := factories.BuildLabelNameMapFromSchema(schemaColumnMappings)

	var segmentParams []lrdb.InsertLogSegmentParams

	// First, collect all valid results to know how many IDs we need
	type validResult struct {
		dateint int32
		result  parquetwriter.Result
		stats   factories.LogsFileStats
	}
	var validResults []validResult

	// Collect all valid results from all bins
	for dateint, bin := range dateintBins {
		if len(bin.Results) == 0 {
			ll.Debug("Skipping empty dateint bin", slog.Int("dateint", int(dateint)))
			continue
		}

		// Process each result from this bin
		for _, result := range bin.Results {
			if result.RecordCount == 0 {
				continue
			}

			// Extract file stats from parquetwriter result
			stats, ok := result.Metadata.(factories.LogsFileStats)
			if !ok {
				return nil, fmt.Errorf("expected LogsFileStats metadata, got %T", result.Metadata)
			}

			validResults = append(validResults, validResult{
				dateint: dateint,
				result:  result,
				stats:   stats,
			})
		}
	}

	// Generate unique batch IDs for all valid results to avoid collisions
	batchSegmentIDs := idgen.GenerateBatchIDs(len(validResults))

	// Get stream field for agg_fields
	streamField := configservice.Global().GetLogStreamConfig(ctx, storageProfile.OrganizationID).FieldName

	for i, valid := range validResults {
		dateint := valid.dateint
		result := valid.result
		stats := valid.stats

		segmentID := batchSegmentIDs[i]

		uploadPath := helpers.MakeDBObjectID(
			storageProfile.OrganizationID,
			storageProfile.CollectorName,
			dateint,
			helpers.HourFromMillis(stats.FirstTS),
			segmentID,
			"logs",
		)

		uploadErr := storageClient.UploadObject(ctx, storageProfile.Bucket, uploadPath, result.FileName)
		if uploadErr != nil {
			return nil, fmt.Errorf("failed to upload file %s to %s: %w", result.FileName, uploadPath, uploadErr)
		}

		ll.Debug("Uploaded log segment",
			slog.String("uploadPath", uploadPath),
			slog.Int64("segmentID", segmentID),
			slog.Int64("recordCount", result.RecordCount),
			slog.Int64("fileSize", result.FileSize))

		// Emit metric for rows missing stream identification field
		if stats.MissingStreamFieldCount > 0 {
			factories.StreamFieldMissingCounter.Add(ctx, stats.MissingStreamFieldCount)
		}

		// Write and upload aggregation parquet file
		var aggFields []string
		if len(stats.AggCounts) > 0 {
			aggFilename := fmt.Sprintf("%s/agg_%d.parquet", os.TempDir(), segmentID)
			aggSize, aggWriteErr := factories.WriteAggParquet(ctx, aggFilename, stats.AggCounts)
			if aggWriteErr != nil {
				ll.Warn("Failed to write agg parquet, continuing without",
					slog.Int64("segmentID", segmentID),
					slog.Any("error", aggWriteErr))
			} else {
				defer func() { _ = os.Remove(aggFilename) }()

				aggPath := helpers.MakeAggDBObjectID(
					storageProfile.OrganizationID,
					storageProfile.CollectorName,
					dateint,
					helpers.HourFromMillis(stats.FirstTS),
					segmentID,
					"logs",
				)

				aggUploadErr := storageClient.UploadObject(ctx, storageProfile.Bucket, aggPath, aggFilename)
				if aggUploadErr != nil {
					ll.Warn("Failed to upload agg parquet, continuing without",
						slog.Int64("segmentID", segmentID),
						slog.String("aggPath", aggPath),
						slog.Any("error", aggUploadErr))
				} else {
					aggFields = factories.GetAggFields(streamField)
					factories.RecordAggFileWritten(ctx, storageProfile.OrganizationID, storageProfile.InstanceNum)
					ll.Debug("Uploaded agg parquet",
						slog.String("aggPath", aggPath),
						slog.Int64("aggSize", aggSize),
						slog.Int("aggBuckets", len(stats.AggCounts)))
				}
			}
		}

		// Create segment parameters using stats from parquet writer
		params := lrdb.InsertLogSegmentParams{
			OrganizationID: storageProfile.OrganizationID,
			Dateint:        dateint,
			SegmentID:      segmentID,
			InstanceNum:    storageProfile.InstanceNum,
			StartTs:        stats.FirstTS,
			EndTs:          stats.LastTS + 1, // end is exclusive
			RecordCount:    result.RecordCount,
			FileSize:       result.FileSize,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   stats.Fingerprints,
			Published:      true,  // Mark ingested segments as published
			Compacted:      false, // New segments are not compacted
			LabelNameMap:   labelNameMap,
			StreamIds:      stats.StreamValues,
			StreamIDField:  stats.StreamIdField,
			SortVersion:    lrdb.CurrentLogSortVersion,
			AggFields:      aggFields,
		}

		segmentParams = append(segmentParams, params)
	}

	ll.Info("Log segment upload completed", slog.Int("totalSegments", len(segmentParams)))

	return segmentParams, nil
}
