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
	"sync"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/exemplars"
	"github.com/cardinalhq/lakerunner/internal/workqueue"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
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

// TraceIDTimestampSortKey represents a sort key based on trace_id first, then timestamp
type TraceIDTimestampSortKey struct {
	TraceID   string
	Timestamp int64
	TraceOk   bool
	TsOk      bool
}

// Compare implements filereader.SortKey interface for TraceIDTimestampSortKey
func (k *TraceIDTimestampSortKey) Compare(other filereader.SortKey) int {
	o, ok := other.(*TraceIDTimestampSortKey)
	if !ok {
		panic("TraceIDTimestampSortKey.Compare: other key is not TraceIDTimestampSortKey")
	}

	// Handle missing trace IDs - put them at the end
	if !k.TraceOk || !o.TraceOk {
		if !k.TraceOk && !o.TraceOk {
			// Neither has trace ID, fall back to timestamp comparison
			if !k.TsOk || !o.TsOk {
				if !k.TsOk && !o.TsOk {
					return 0
				}
				if !k.TsOk {
					return 1
				}
				return -1
			}
			if k.Timestamp < o.Timestamp {
				return -1
			}
			if k.Timestamp > o.Timestamp {
				return 1
			}
			return 0
		}
		if !k.TraceOk {
			return 1
		}
		return -1
	}

	// Compare trace IDs first
	if k.TraceID < o.TraceID {
		return -1
	}
	if k.TraceID > o.TraceID {
		return 1
	}

	// Same trace ID, compare timestamps
	if !k.TsOk || !o.TsOk {
		if !k.TsOk && !o.TsOk {
			return 0
		}
		if !k.TsOk {
			return 1
		}
		return -1
	}

	if k.Timestamp < o.Timestamp {
		return -1
	}
	if k.Timestamp > o.Timestamp {
		return 1
	}
	return 0
}

// Release returns the TraceIDTimestampSortKey to the pool for reuse
func (k *TraceIDTimestampSortKey) Release() {
	putTraceIDTimestampSortKey(k)
}

// traceIDTimestampSortKeyPool is a pool of reusable TraceIDTimestampSortKey instances
var traceIDTimestampSortKeyPool = sync.Pool{
	New: func() any {
		return &TraceIDTimestampSortKey{}
	},
}

// getTraceIDTimestampSortKey gets a TraceIDTimestampSortKey from the pool
func getTraceIDTimestampSortKey() *TraceIDTimestampSortKey {
	return traceIDTimestampSortKeyPool.Get().(*TraceIDTimestampSortKey)
}

// putTraceIDTimestampSortKey returns a TraceIDTimestampSortKey to the pool after resetting it
func putTraceIDTimestampSortKey(key *TraceIDTimestampSortKey) {
	*key = TraceIDTimestampSortKey{}
	traceIDTimestampSortKeyPool.Put(key)
}

// TraceIDTimestampSortKeyProvider creates TraceIDTimestampSortKey instances from rows
type TraceIDTimestampSortKeyProvider struct{}

// MakeKey implements filereader.SortKeyProvider interface for trace ID + timestamp sorting
func (p *TraceIDTimestampSortKeyProvider) MakeKey(row pipeline.Row) filereader.SortKey {
	key := getTraceIDTimestampSortKey()

	// Get trace_id from the common keys
	traceIDKey := wkk.NewRowKey("trace_id")
	if traceIDValue, ok := row[traceIDKey]; ok {
		if traceIDStr, ok := traceIDValue.(string); ok {
			key.TraceID = traceIDStr
			key.TraceOk = true
		}
	}

	// Get timestamp from CardinalhQ timestamp
	key.Timestamp, key.TsOk = row[wkk.RowKeyCTimestamp].(int64)

	return key
}

// TraceDateintBin represents a file group containing traces for a specific dateint
type TraceDateintBin struct {
	Dateint int32 // The dateint for this bin
	Writer  parquetwriter.ParquetWriter
	Results []parquetwriter.Result // Results after writer is closed (can be multiple files)
}

// TraceDateintBinManager manages multiple file groups, one per dateint
type TraceDateintBinManager struct {
	bins        map[int32]*TraceDateintBin // Key is dateint
	tmpDir      string
	rpfEstimate int64
	schema      *filereader.ReaderSchema
}

// TraceIngestProcessor implements the Processor interface for raw trace ingestion
type TraceIngestProcessor struct {
	store             TraceIngestStore
	storageProvider   storageprofile.StorageProfileProvider
	cmgr              cloudstorage.ClientProvider
	kafkaProducer     fly.Producer
	exemplarProcessor *exemplars.Processor
	config            *config.Config
}

// newTraceIngestProcessor creates a new trace ingest processor instance
func newTraceIngestProcessor(
	cfg *config.Config,
	store TraceIngestStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider, kafkaProducer fly.Producer) *TraceIngestProcessor {
	exemplarProcessor := exemplars.NewProcessor(exemplars.DefaultConfig())
	exemplarProcessor.SetTracesCallback(func(ctx context.Context, organizationID uuid.UUID, rows []pipeline.Row) error {
		return processTracesExemplarsDirect(ctx, organizationID, rows, store)
	})

	return &TraceIngestProcessor{
		store:             store,
		storageProvider:   storageProvider,
		cmgr:              cmgr,
		kafkaProducer:     kafkaProducer,
		exemplarProcessor: exemplarProcessor,
		config:            cfg,
	}
}

// validateTraceIngestMessages validates the key and messages for consistency
func validateTraceIngestMessages(key messages.IngestKey, msgs []*messages.ObjStoreNotificationMessage) error {
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

// ProcessBundle implements the ProcessBundle pattern for raw trace ingestion
func (p *TraceIngestProcessor) ProcessBundle(ctx context.Context, key messages.IngestKey, msgs []*messages.ObjStoreNotificationMessage, partition int32, offset int64) error {
	ll := logctx.FromContext(ctx).With(
		slog.String("organizationID", key.OrganizationID.String()),
		slog.Int("instanceNum", int(key.InstanceNum)))

	ll.Info("Starting trace ingestion",
		slog.Int("messageCount", len(msgs)))

	if err := validateTraceIngestMessages(key, msgs); err != nil {
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

	// Setup storage
	ctx, setupSpan := boxerTracer.Start(ctx, "traces.ingest.setup_storage", trace.WithAttributes(
		attribute.String("organization_id", key.OrganizationID.String()),
		attribute.Int("instance_num", int(key.InstanceNum)),
	))

	srcProfile, err := p.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, key.OrganizationID, key.InstanceNum)
	if err != nil {
		setupSpan.RecordError(err)
		setupSpan.SetStatus(codes.Error, "failed to get storage profile")
		setupSpan.End()
		return fmt.Errorf("get storage profile: %w", err)
	}

	inputClient, err := cloudstorage.NewClient(ctx, p.cmgr, srcProfile)
	if err != nil {
		setupSpan.RecordError(err)
		setupSpan.SetStatus(codes.Error, "failed to create input storage client")
		setupSpan.End()
		return fmt.Errorf("create storage client: %w", err)
	}

	dstProfile := srcProfile
	if p.config.Traces.Ingestion.SingleInstanceMode {
		dstProfile, err = p.storageProvider.GetLowestInstanceStorageProfile(ctx, srcProfile.OrganizationID, srcProfile.Bucket)
		if err != nil {
			setupSpan.RecordError(err)
			setupSpan.SetStatus(codes.Error, "failed to get lowest instance storage profile")
			setupSpan.End()
			return fmt.Errorf("get lowest instance storage profile: %w", err)
		}
	}

	outputClient, err := cloudstorage.NewClient(ctx, p.cmgr, dstProfile)
	if err != nil {
		setupSpan.RecordError(err)
		setupSpan.SetStatus(codes.Error, "failed to create output storage client")
		setupSpan.End()
		return fmt.Errorf("create storage client: %w", err)
	}
	setupSpan.End()

	// Download files
	ctx, downloadSpan := boxerTracer.Start(ctx, "traces.ingest.download_files", trace.WithAttributes(
		attribute.Int("message_count", len(msgs)),
	))

	var readers []filereader.Reader
	var readersToClose []filereader.Reader
	var totalInputSize int64

	for _, msg := range msgs {
		ll.Debug("Processing raw trace file",
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

		reader, err := p.createTraceReaderStack(tmpFilename, msg.OrganizationID.String(), msg.Bucket, msg.ObjectID)
		if err != nil {
			ll.Error("Failed to create reader stack", slog.String("objectID", msg.ObjectID), slog.Any("error", err))
			continue
		}

		readers = append(readers, reader)
		readersToClose = append(readersToClose, reader)
		totalInputSize += msg.FileSize
	}
	downloadSpan.SetAttributes(
		attribute.Int("readers_created", len(readers)),
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

	// Create unified reader
	ctx, unifiedSpan := boxerTracer.Start(ctx, "traces.ingest.create_unified_reader", trace.WithAttributes(
		attribute.Int("reader_count", len(readers)),
	))

	finalReader, err := p.createUnifiedTraceReader(ctx, readers)
	if err != nil {
		unifiedSpan.RecordError(err)
		unifiedSpan.SetStatus(codes.Error, "failed to create unified reader")
		unifiedSpan.End()
		return fmt.Errorf("failed to create unified reader: %w", err)
	}
	unifiedSpan.End()

	// Process rows
	ctx, processSpan := boxerTracer.Start(ctx, "traces.ingest.process_rows")

	dateintBins, totalInputRecords, err := p.processRowsWithDateintBinning(ctx, finalReader, tmpDir, srcProfile)
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

	// Upload segments
	ctx, uploadSpan := boxerTracer.Start(ctx, "traces.ingest.upload_segments", trace.WithAttributes(
		attribute.Int("dateint_bins", len(dateintBins)),
	))

	segmentParams, err := p.uploadAndCreateTraceSegments(ctx, outputClient, dateintBins, dstProfile)
	if err != nil {
		uploadSpan.RecordError(err)
		uploadSpan.SetStatus(codes.Error, "failed to upload segments")
		uploadSpan.End()
		return fmt.Errorf("failed to upload and create segments: %w", err)
	}
	uploadSpan.SetAttributes(attribute.Int("segments_created", len(segmentParams)))
	uploadSpan.End()

	// Insert segments into database
	ctx, insertSpan := boxerTracer.Start(ctx, "traces.ingest.insert_segments", trace.WithAttributes(
		attribute.Int("segment_count", len(segmentParams)),
	))

	criticalCtx := context.WithoutCancel(ctx)
	if err := p.store.InsertTraceSegmentsBatch(criticalCtx, segmentParams); err != nil {
		insertSpan.RecordError(err)
		insertSpan.SetStatus(codes.Error, "failed to insert segments")
		insertSpan.End()

		// Log detailed segment information for debugging
		segmentIDs := make([]int64, len(segmentParams))
		var totalRecords, totalSize int64
		for i, seg := range segmentParams {
			segmentIDs[i] = seg.SegmentID
			totalRecords += seg.RecordCount
			totalSize += seg.FileSize
		}

		ll.Error("Failed to insert trace segments with Kafka offsets",
			slog.Any("error", err),
			slog.String("organization_id", key.OrganizationID.String()),
			slog.Int("instance_num", int(key.InstanceNum)),
			slog.Int("segmentCount", len(segmentParams)),
			slog.Int64("totalRecords", totalRecords),
			slog.Int64("totalSize", totalSize),
			slog.Any("segment_ids", segmentIDs))

		return fmt.Errorf("failed to insert trace segments with Kafka offsets: %w", err)
	}
	insertSpan.End()

	// Send compaction notifications to Kafka topic
	ctx, publishSpan := boxerTracer.Start(ctx, "traces.ingest.publish_kafka", trace.WithAttributes(
		attribute.Int("segment_count", len(segmentParams)),
	))

	if p.kafkaProducer != nil {
		compactionTopic := p.config.TopicRegistry.GetTopic(config.TopicBoxerTracesCompact)

		for _, segParams := range segmentParams {
			// Create trace compaction message
			compactionNotification := messages.TraceCompactionMessage{
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
				publishSpan.RecordError(err)
				publishSpan.SetStatus(codes.Error, "failed to marshal compaction notification")
				publishSpan.End()
				return fmt.Errorf("failed to marshal trace compaction notification: %w", err)
			}

			compactionMessage := fly.Message{
				Key:   fmt.Appendf(nil, "%s-%d-%d", segParams.OrganizationID.String(), segParams.InstanceNum, segParams.StartTs/300000),
				Value: compactionMsgBytes,
			}

			// Send to compaction topic
			if err := p.kafkaProducer.Send(criticalCtx, compactionTopic, compactionMessage); err != nil {
				publishSpan.RecordError(err)
				publishSpan.SetStatus(codes.Error, "failed to send compaction notification")
				publishSpan.End()
				return fmt.Errorf("failed to send trace compaction notification to Kafka: %w", err)
			} else {
				ll.Debug("Sent trace compaction notification", slog.Any("message", compactionNotification))
			}
		}
	}
	publishSpan.End()

	// Calculate output metrics for telemetry
	var totalOutputRecords, totalOutputSize int64
	for _, params := range segmentParams {
		totalOutputRecords += params.RecordCount
		totalOutputSize += params.FileSize
	}

	// Report telemetry - ingestion transforms files into segments
	reportTelemetry(ctx, "traces", "ingestion", int64(len(msgs)), int64(len(segmentParams)), totalInputRecords, totalOutputRecords, totalInputSize, totalOutputSize)

	ll.Info("Trace ingestion completed successfully",
		slog.Int("inputFiles", len(msgs)),
		slog.Int64("totalFileSize", totalInputSize),
		slog.Int("outputSegments", len(segmentParams)))

	return nil
}

// ProcessBundleFromQueue implements the BundleProcessor interface for work queue integration
func (p *TraceIngestProcessor) ProcessBundleFromQueue(ctx context.Context, workItem workqueue.Workable) error {
	ll := logctx.FromContext(ctx)

	// Extract bundle from work item spec
	var bundle messages.TraceIngestBundle
	specBytes, err := json.Marshal(workItem.Spec())
	if err != nil {
		return fmt.Errorf("failed to marshal work item spec: %w", err)
	}

	if err := json.Unmarshal(specBytes, &bundle); err != nil {
		return fmt.Errorf("failed to unmarshal trace ingest bundle: %w", err)
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
func (p *TraceIngestProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.IngestKey) int64 {
	return 5 * 1024 * 1024 // 5MB file size limit instead of record count
}

// ShouldEmitImmediately returns false - trace ingest always uses normal grouping.
func (p *TraceIngestProcessor) ShouldEmitImmediately(msg *messages.ObjStoreNotificationMessage) bool {
	return false
}

// createTraceReaderStack creates a reader stack: Translation(TraceReader(file))
func (p *TraceIngestProcessor) createTraceReaderStack(tmpFilename, orgID, bucket, objectID string) (filereader.Reader, error) {
	// Determine file type from extension for logging
	var fileType string
	switch {
	case strings.HasSuffix(tmpFilename, ".binpb.gz"):
		fileType = "binpb.gz"
	case strings.HasSuffix(tmpFilename, ".binpb"):
		fileType = "binpb"
	default:
		fileType = "unknown"
	}

	slog.Info("Reading trace file",
		"fileType", fileType,
		"objectID", objectID,
		"bucket", bucket)

	reader, err := p.createTraceReader(tmpFilename, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to create trace reader: %w", err)
	}

	translator := &TraceTranslator{
		orgID:    orgID,
		bucket:   bucket,
		objectID: objectID,
	}
	translatedReader, err := filereader.NewTranslatingReader(reader, translator, 1000)
	if err != nil {
		_ = reader.Close()
		return nil, fmt.Errorf("failed to create translating reader: %w", err)
	}

	return translatedReader, nil
}

func (p *TraceIngestProcessor) createTraceReader(filename, orgID string) (filereader.Reader, error) {
	options := filereader.ReaderOptions{
		SignalType: filereader.SignalTypeTraces,
		BatchSize:  1000,
		OrgID:      orgID,
	}
	return filereader.ReaderForFileWithOptions(filename, options)
}

// createUnifiedTraceReader creates a unified reader from multiple readers
func (p *TraceIngestProcessor) createUnifiedTraceReader(ctx context.Context, readers []filereader.Reader) (filereader.Reader, error) {
	var finalReader filereader.Reader

	if len(readers) == 1 {
		finalReader = readers[0]
	} else {
		keyProvider := &TraceIDTimestampSortKeyProvider{}
		multiReader, err := filereader.NewMergesortReader(ctx, readers, keyProvider, 1000)
		if err != nil {
			return nil, fmt.Errorf("failed to create multi-source reader: %w", err)
		}
		finalReader = multiReader
	}

	return finalReader, nil
}

// processRowsWithDateintBinning groups traces by dateint only (no aggregation, no time window)
func (p *TraceIngestProcessor) processRowsWithDateintBinning(ctx context.Context, reader filereader.Reader, tmpDir string, storageProfile storageprofile.StorageProfile) (map[int32]*TraceDateintBin, int64, error) {
	ll := logctx.FromContext(ctx)

	// Get schema from reader (GetSchema returns a copy)
	schema := reader.GetSchema()

	// Add columns that will be injected by TraceTranslator
	// These columns are added to every row but aren't in the OTEL schema
	schema.AddColumn(wkk.RowKeyCTelemetryType, wkk.RowKeyCTelemetryType, filereader.DataTypeString, true)
	schema.AddColumn(wkk.RowKeyCID, wkk.RowKeyCID, filereader.DataTypeString, true)

	rpfEstimate := p.store.GetTraceEstimate(ctx, storageProfile.OrganizationID)

	binManager := &TraceDateintBinManager{
		bins:        make(map[int32]*TraceDateintBin),
		tmpDir:      tmpDir,
		rpfEstimate: rpfEstimate,
		schema:      schema,
	}

	var totalRowsRead, totalRowsProcessed int64

	// Process all rows from the reader
	for {
		batch, readErr := reader.Next(ctx)
		if readErr != nil && readErr != io.EOF {
			if batch != nil {
				pipeline.ReturnBatch(batch)
			}
			return nil, 0, fmt.Errorf("failed to read from unified pipeline: %w", readErr)
		}

		if batch != nil {
			totalRowsRead += int64(batch.Len())
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

				dateint, _ := helpers.MSToDateintHour(ts)
				bin, err := binManager.getOrCreateBin(dateint)
				if err != nil {
					ll.Error("Failed to get/create dateint bin", slog.Int("dateint", int(dateint)), slog.Any("error", err))
					continue
				}

				// Process exemplar before taking the row
				if p.exemplarProcessor != nil {
					_ = p.exemplarProcessor.ProcessTracesFromRow(ctx, storageProfile.OrganizationID, row)
				}

				// Now take the row to avoid copying
				takenRow := batch.TakeRow(i)
				if takenRow == nil {
					continue
				}

				// Add unique row ID for traces
				takenRow[wkk.RowKeyCID] = idgen.NextBase32ID()

				// Create a single-row batch with the taken row
				singleRowBatch := pipeline.GetBatch()
				singleRowBatch.AppendRow(takenRow)

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

	ll.Info("Trace slot binning completed",
		slog.Int64("rowsRead", totalRowsRead),
		slog.Int64("rowsProcessed", totalRowsProcessed),
		slog.Int("slotBinsCreated", len(binManager.bins)))

	// Close all writers and collect results
	for slot, bin := range binManager.bins {
		results, err := bin.Writer.Close(ctx)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to close writer for slot %d: %w", slot, err)
		}

		if len(results) > 0 {
			// Store all results - writer may emit multiple files when data exceeds RecordsPerFile
			bin.Results = append(bin.Results, results...)
		}
	}

	return binManager.bins, totalRowsRead, nil
}

// getOrCreateBin gets or creates a dateint bin for the given dateint
func (manager *TraceDateintBinManager) getOrCreateBin(dateint int32) (*TraceDateintBin, error) {
	if bin, exists := manager.bins[dateint]; exists {
		return bin, nil
	}

	// Create new writer for this dateint bin
	writer, err := factories.NewTracesWriter(manager.tmpDir, manager.schema, manager.rpfEstimate, parquetwriter.DefaultBackend)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer for dateint bin: %w", err)
	}

	bin := &TraceDateintBin{
		Dateint: dateint,
		Writer:  writer,
	}

	manager.bins[dateint] = bin
	return bin, nil
}

// uploadAndCreateTraceSegments uploads dateint bins to S3 and creates segment parameters
func (p *TraceIngestProcessor) uploadAndCreateTraceSegments(ctx context.Context, storageClient cloudstorage.Client, dateintBins map[int32]*TraceDateintBin, storageProfile storageprofile.StorageProfile) ([]lrdb.InsertTraceSegmentParams, error) {
	ll := logctx.FromContext(ctx)

	var segmentParams []lrdb.InsertTraceSegmentParams

	// First, collect all valid results to know how many IDs we need
	type validBin struct {
		dateint int32
		result  parquetwriter.Result
		stats   factories.TracesFileStats
	}
	var validBins []validBin

	for dateint, bin := range dateintBins {
		if len(bin.Results) == 0 {
			ll.Debug("Skipping empty dateint bin", slog.Int("dateint", int(dateint)))
			continue
		}

		// Process each result separately - writer may have created multiple files
		for _, result := range bin.Results {
			if result.RecordCount == 0 {
				continue
			}

			// Extract file stats from parquetwriter result
			stats, ok := result.Metadata.(factories.TracesFileStats)
			if !ok {
				return nil, fmt.Errorf("expected TracesFileStats metadata, got %T", result.Metadata)
			}

			validBins = append(validBins, validBin{
				dateint: dateint,
				result:  result,
				stats:   stats,
			})
		}
	}

	// Generate unique batch IDs for all valid bins to avoid collisions
	batchSegmentIDs := idgen.GenerateBatchIDs(len(validBins))

	for i, validBin := range validBins {
		segmentID := batchSegmentIDs[i]

		// Generate upload path using helpers.MakeDBObjectID
		uploadKey := helpers.MakeDBObjectID(
			storageProfile.OrganizationID,
			storageProfile.CollectorName,
			validBin.dateint,
			helpers.HourFromMillis(validBin.stats.FirstTS),
			segmentID,
			"traces",
		)

		err := storageClient.UploadObject(ctx, storageProfile.Bucket, uploadKey, validBin.result.FileName)
		if err != nil {
			return nil, fmt.Errorf("failed to upload trace segment for dateint %d: %w", validBin.dateint, err)
		}

		ll.Info("Uploaded trace segment",
			slog.String("uploadKey", uploadKey),
			slog.Int64("segmentID", segmentID),
			slog.Int64("recordCount", validBin.result.RecordCount))

		// Create segment parameters using stats from parquet writer
		segmentParam := lrdb.InsertTraceSegmentParams{
			OrganizationID: storageProfile.OrganizationID,
			SegmentID:      segmentID,
			Dateint:        validBin.dateint,
			InstanceNum:    storageProfile.InstanceNum,
			StartTs:        validBin.stats.FirstTS,
			EndTs:          validBin.stats.LastTS + 1, // end is exclusive
			RecordCount:    validBin.result.RecordCount,
			FileSize:       validBin.result.FileSize,
			CreatedBy:      lrdb.CreatedByIngest,
			Fingerprints:   validBin.stats.Fingerprints,
			Published:      true,
			Compacted:      false,
			LabelNameMap:   validBin.stats.LabelNameMap,
		}

		segmentParams = append(segmentParams, segmentParam)
	}

	ll.Info("Trace segment upload completed",
		slog.Int("totalSegments", len(segmentParams)))

	return segmentParams, nil
}

// TraceTranslator adds resource metadata to trace rows
type TraceTranslator struct {
	orgID    string
	bucket   string
	objectID string
}

// NewTraceTranslator creates a new TraceTranslator with the specified metadata
func NewTraceTranslator(orgID, bucket, objectID string) *TraceTranslator {
	return &TraceTranslator{
		orgID:    orgID,
		bucket:   bucket,
		objectID: objectID,
	}
}

// TranslateRow adds resource fields to each row
func (t *TraceTranslator) TranslateRow(_ context.Context, row *pipeline.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Ensure required CardinalhQ fields are set
	(*row)[wkk.RowKeyCTelemetryType] = "traces"

	return nil
}
