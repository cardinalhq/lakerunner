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
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/exemplars"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
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
}

// LogIngestProcessor implements the Processor interface for raw log ingestion
type LogIngestProcessor struct {
	store             LogIngestStore
	storageProvider   storageprofile.StorageProfileProvider
	cmgr              cloudstorage.ClientProvider
	kafkaProducer     fly.Producer
	exemplarProcessor *exemplars.Processor
	config            *config.Config
}

// newLogIngestProcessor creates a new log ingest processor instance
func newLogIngestProcessor(
	cfg *config.Config,
	store LogIngestStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, kafkaProducer fly.Producer) *LogIngestProcessor {
	exemplarProcessor := exemplars.NewProcessor(exemplars.DefaultConfig())
	exemplarProcessor.SetLogsCallback(func(ctx context.Context, organizationID string, exemplars []*exemplars.ExemplarData) error {
		return processLogsExemplarsDirect(ctx, organizationID, exemplars, store)
	})

	return &LogIngestProcessor{
		store:             store,
		storageProvider:   storageProvider,
		cmgr:              cmgr,
		kafkaProducer:     kafkaProducer,
		exemplarProcessor: exemplarProcessor,
		config:            cfg,
	}
}

// validateLogIngestGroupConsistency ensures all messages in a log ingest group have consistent fields
func validateLogIngestGroupConsistency(group *accumulationGroup[messages.IngestKey]) error {
	if len(group.Messages) == 0 {
		return &GroupValidationError{
			Field:   "message_count",
			Message: "group cannot be empty",
		}
	}

	// Get expected values from the group key
	expectedOrg := group.Key.OrganizationID
	expectedInstance := group.Key.InstanceNum

	// Validate each message against the expected values
	for i, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.ObjStoreNotificationMessage)
		if !ok {
			return &GroupValidationError{
				Field:   "message_type",
				Message: fmt.Sprintf("message %d is not an ObjStoreNotificationMessage", i),
			}
		}

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
func (p *LogIngestProcessor) Process(ctx context.Context, group *accumulationGroup[messages.IngestKey], kafkaOffsets []lrdb.KafkaOffsetInfo) error {
	ll := logctx.FromContext(ctx)

	// Calculate group age from Hunter timestamp
	groupAge := time.Since(group.CreatedAt)

	ll.Info("Starting log ingestion",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge))

	if err := validateLogIngestGroupConsistency(group); err != nil {
		return fmt.Errorf("group validation failed: %w", err)
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

	srcProfile, err := p.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, group.Key.OrganizationID, group.Key.InstanceNum)
	if err != nil {
		return fmt.Errorf("get storage profile: %w", err)
	}

	inputClient, err := cloudstorage.NewClient(ctx, p.cmgr, srcProfile)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	dstProfile := srcProfile
	if p.config.Logs.Ingestion.SingleInstanceMode {
		dstProfile, err = p.storageProvider.GetLowestInstanceStorageProfile(ctx, srcProfile.OrganizationID, srcProfile.Bucket)
		if err != nil {
			return fmt.Errorf("get lowest instance storage profile: %w", err)
		}
	}

	outputClient, err := cloudstorage.NewClient(ctx, p.cmgr, dstProfile)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	var readers []filereader.Reader
	var readersToClose []filereader.Reader
	var totalInputSize int64

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.ObjStoreNotificationMessage)
		if !ok {
			continue // Skip non-ObjStoreNotificationMessage messages
		}

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

		reader, err := p.createLogReaderStack(tmpFilename, msg.OrganizationID.String(), msg.Bucket, msg.ObjectID)
		if err != nil {
			ll.Error("Failed to create reader stack", slog.String("objectID", msg.ObjectID), slog.Any("error", err))
			continue
		}

		readers = append(readers, reader)
		readersToClose = append(readersToClose, reader)
		totalInputSize += msg.FileSize
	}

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

	finalReader, err := p.createUnifiedLogReader(ctx, readers)
	if err != nil {
		return fmt.Errorf("failed to create unified reader: %w", err)
	}

	dateintBins, err := p.processRowsWithDateintBinning(ctx, finalReader, tmpDir, srcProfile)
	if err != nil {
		return fmt.Errorf("failed to process rows: %w", err)
	}

	if len(dateintBins) == 0 {
		ll.Info("No output files generated")
		return nil
	}

	segmentParams, err := p.uploadAndCreateLogSegments(ctx, outputClient, dateintBins, dstProfile)
	if err != nil {
		return fmt.Errorf("failed to upload and create segments: %w", err)
	}

	criticalCtx := context.WithoutCancel(ctx)
	if err := p.store.InsertLogSegmentsBatch(criticalCtx, segmentParams, kafkaOffsets); err != nil {
		// Log detailed segment information for debugging
		segmentIDs := make([]int64, len(segmentParams))
		var totalRecords, totalSize int64
		for i, seg := range segmentParams {
			segmentIDs[i] = seg.SegmentID
			totalRecords += seg.RecordCount
			totalSize += seg.FileSize
		}

		ll.Error("Failed to insert log segments with Kafka offsets",
			slog.Any("error", err),
			slog.String("organization_id", group.Key.OrganizationID.String()),
			slog.Int("instance_num", int(group.Key.InstanceNum)),
			slog.Int("segmentCount", len(segmentParams)),
			slog.Int64("totalRecords", totalRecords),
			slog.Int64("totalSize", totalSize),
			slog.Any("segment_ids", segmentIDs))

		return fmt.Errorf("failed to insert log segments with Kafka offsets: %w", err)
	}

	// Send compaction notifications to Kafka topic
	if p.kafkaProducer != nil {
		compactionTopic := p.config.TopicRegistry.GetTopic(config.TopicBoxerLogsCompact)

		for _, segParams := range segmentParams {
			// Create log compaction message
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

			// Marshal compaction message
			compactionMsgBytes, err := compactionNotification.Marshal()
			if err != nil {
				return fmt.Errorf("failed to marshal log compaction notification: %w", err)
			}

			compactionMessage := fly.Message{
				Key:   fmt.Appendf(nil, "%s-%d-%d", segParams.OrganizationID.String(), segParams.InstanceNum, segParams.StartTs/300000),
				Value: compactionMsgBytes,
			}

			// Send to compaction topic
			if err := p.kafkaProducer.Send(criticalCtx, compactionTopic, compactionMessage); err != nil {
				return fmt.Errorf("failed to send log compaction notification to Kafka: %w", err)
			} else {
				ll.Debug("Sent log compaction notification", slog.Any("message", compactionNotification))
			}
		}
	}

	// Calculate output metrics for telemetry
	var totalOutputRecords, totalOutputSize int64
	for _, params := range segmentParams {
		totalOutputRecords += params.RecordCount
		totalOutputSize += params.FileSize
	}

	// Report telemetry - ingestion transforms files into segments
	reportTelemetry(ctx, "logs", "ingestion", int64(len(group.Messages)), int64(len(segmentParams)), 0, totalOutputRecords, totalInputSize, totalOutputSize)

	ll.Info("Log ingestion completed successfully",
		slog.Int("inputFiles", len(group.Messages)),
		slog.Int64("totalFileSize", totalInputSize),
		slog.Int("outputSegments", len(segmentParams)))

	return nil
}

// GetTargetRecordCount returns the target file size limit (5MB) for accumulation
func (p *LogIngestProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.IngestKey) int64 {
	return 5 * 1024 * 1024 // 5MB file size limit instead of record count
}

// createLogReaderStack creates a reader stack: Translation(LogReader(file))
func (p *LogIngestProcessor) createLogReaderStack(tmpFilename, orgID, bucket, objectID string) (filereader.Reader, error) {
	reader, err := p.createLogReader(tmpFilename, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to create log reader: %w", err)
	}

	// Check if the file is a Parquet file to determine which translator to use
	var translator filereader.RowTranslator
	if strings.HasSuffix(tmpFilename, ".parquet") {
		// Use specialized Parquet translator that handles timestamp detection and fingerprinting
		translator = &ParquetLogTranslator{
			OrgID:             orgID,
			Bucket:            bucket,
			ObjectID:          objectID,
			ExemplarProcessor: p.exemplarProcessor,
		}
	} else {
		// Use standard translator for other formats (json, binpb, etc.)
		translator = &LogTranslator{
			orgID:    orgID,
			bucket:   bucket,
			objectID: objectID,
		}
	}

	reader, err = filereader.NewTranslatingReader(reader, translator, 1000)
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("failed to create translating reader: %w", err)
	}

	return reader, nil
}

func (p *LogIngestProcessor) createLogReader(filename, orgId string) (filereader.Reader, error) {
	options := filereader.ReaderOptions{
		SignalType:        filereader.SignalTypeLogs,
		BatchSize:         1000,
		ExemplarProcessor: p.exemplarProcessor,
		OrgID:             orgId,
	}
	return filereader.ReaderForFileWithOptions(filename, options)
}

// createUnifiedLogReader creates a unified reader from multiple readers
func (p *LogIngestProcessor) createUnifiedLogReader(ctx context.Context, readers []filereader.Reader) (filereader.Reader, error) {
	var finalReader filereader.Reader

	if len(readers) == 1 {
		finalReader = readers[0]
	} else {
		keyProvider := &filereader.TimestampSortKeyProvider{}
		multiReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
		if err != nil {
			return nil, fmt.Errorf("failed to create multi-source reader: %w", err)
		}
		finalReader = multiReader
	}

	return finalReader, nil
}

// processRowsWithDateintBinning groups logs by dateint only (no aggregation, no time window)
func (p *LogIngestProcessor) processRowsWithDateintBinning(ctx context.Context, reader filereader.Reader, tmpDir string, storageProfile storageprofile.StorageProfile) (map[int32]*DateintBin, error) {
	ll := logctx.FromContext(ctx)

	// Get RPF estimate for this org/instance - use logs estimator logic
	rpfEstimate := p.store.GetLogEstimate(ctx, storageProfile.OrganizationID)

	// Create dateint bin manager
	binManager := &DateintBinManager{
		bins:        make(map[int32]*DateintBin),
		tmpDir:      tmpDir,
		rpfEstimate: rpfEstimate,
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

				// Extract timestamp to determine which bin this row belongs to
				ts, ok := row[wkk.RowKeyCTimestamp].(int64)
				if !ok {
					ll.Warn("Row missing timestamp, skipping", slog.Int("rowIndex", i))
					continue
				}

				// Group logs by dateint only - no time aggregation
				dateint, _ := helpers.MSToDateintHour(ts)

				// Get or create dateint bin
				bin, err := binManager.getOrCreateBin(ctx, dateint)
				if err != nil {
					ll.Error("Failed to get/create dateint bin", slog.Int("dateint", int(dateint)), slog.Any("error", err))
					continue
				}

				// Create a single-row batch for this bin
				singleRowBatch := pipeline.GetBatch()
				newRow := singleRowBatch.AddRow()
				for k, v := range row {
					newRow[k] = v
				}

				// Write to the bin's writer
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
	writer, err := factories.NewLogsWriter(manager.tmpDir, manager.rpfEstimate)
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
func (p *LogIngestProcessor) uploadAndCreateLogSegments(ctx context.Context, storageClient cloudstorage.Client, dateintBins map[int32]*DateintBin, storageProfile storageprofile.StorageProfile) ([]lrdb.InsertLogSegmentParams, error) {
	ll := logctx.FromContext(ctx)

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
		}

		segmentParams = append(segmentParams, params)
	}

	ll.Info("Log segment upload completed",
		slog.Int("totalSegments", len(segmentParams)))

	return segmentParams, nil
}

// LogTranslator adds resource metadata to log rows
type LogTranslator struct {
	orgID    string
	bucket   string
	objectID string
}

// NewLogTranslator creates a new LogTranslator with the specified metadata
func NewLogTranslator(orgID, bucket, objectID string) *LogTranslator {
	return &LogTranslator{
		orgID:    orgID,
		bucket:   bucket,
		objectID: objectID,
	}
}

// TranslateRow adds resource fields to each row
func (t *LogTranslator) TranslateRow(row *filereader.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Only set the specific required fields - assume all other fields are properly set
	(*row)[wkk.NewRowKey("resource.bucket.name")] = t.bucket
	(*row)[wkk.NewRowKey("resource.file.name")] = "./" + t.objectID
	(*row)[wkk.NewRowKey("resource.file.type")] = helpers.GetFileType(t.objectID)

	// Ensure required CardinalhQ fields are set
	(*row)[wkk.RowKeyCTelemetryType] = "logs"
	(*row)[wkk.RowKeyCName] = "log.events"
	(*row)[wkk.RowKeyCValue] = float64(1.0)

	return nil
}
