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

package accumulation

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jellydator/ttlcache/v3"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// IngestStore defines database operations needed for ingestion
type IngestStore interface {
	InsertMetricSegmentBatchWithKafkaOffsets(ctx context.Context, batch lrdb.MetricSegmentBatch) error
	KafkaJournalGetLastProcessedWithOrgInstance(ctx context.Context, params lrdb.KafkaJournalGetLastProcessedWithOrgInstanceParams) (int64, error)
	GetMetricPackEstimateForOrg(ctx context.Context, params lrdb.GetMetricPackEstimateForOrgParams) ([]lrdb.MetricPackEstimate, error)
}

var (
	// Cache for RPF estimates with 5-minute TTL
	rpfEstimateCache     *ttlcache.Cache[string, int64]
	rpfEstimateCacheOnce sync.Once
)

// initRPFEstimateCache initializes the TTL cache for RPF estimates
func initRPFEstimateCache() {
	rpfEstimateCache = ttlcache.New(
		ttlcache.WithTTL[string, int64](5*time.Minute),
	)
	go rpfEstimateCache.Start() // Start background cleanup
}

// ReaderMetadata contains metadata about a file reader
type ReaderMetadata struct {
	ObjectID       string
	OrganizationID uuid.UUID
	InstanceNum    int16
	Bucket         string
	FileSize       int64
	IngestItem     ingest.IngestItem
}

// OrgInstanceKey represents the key for organizing data by org and instance
type OrgInstanceKey struct {
	OrganizationID uuid.UUID
	InstanceNum    int16
}

// AccumulatorManager manages multiple accumulators per org/instance
type AccumulatorManager struct {
	accumulators map[OrgInstanceKey]*IngestAccumulator
}

// IngestAccumulator accumulates readers for a specific org/instance
type IngestAccumulator struct {
	readers        []filereader.Reader
	readerMetadata []ReaderMetadata
	totalFileSize  int64
}



// MetricIngestProcessor implements the Processor interface for raw metric ingestion
type MetricIngestProcessor struct {
	store                IngestStore
	storageProvider      storageprofile.StorageProfileProvider
	cmgr                 cloudstorage.ClientProvider
	targetRecordMultiple int // multiply estimate by this for safety net (e.g., 2)
}

// NewMetricIngestProcessor creates a new metric ingest processor instance
func NewMetricIngestProcessor(store IngestStore, storageProvider storageprofile.StorageProfileProvider, cmgr cloudstorage.ClientProvider) *MetricIngestProcessor {
	return &MetricIngestProcessor{
		store:                store,
		storageProvider:      storageProvider,
		cmgr:                 cmgr,
		targetRecordMultiple: 2,
	}
}

// validateIngestGroupConsistency ensures all messages in an ingest group have consistent fields
func validateIngestGroupConsistency(group *AccumulationGroup[messages.IngestKey]) error {
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

// Process implements the Processor interface and performs raw metric ingestion
func (p *MetricIngestProcessor) Process(ctx context.Context, group *AccumulationGroup[messages.IngestKey], kafkaCommitData *KafkaCommitData, recordCountEstimate int64) error {
	ll := logctx.FromContext(ctx)

	// Calculate group age from Hunter timestamp
	groupAge := time.Since(group.CreatedAt)

	ll.Info("Starting metric ingestion",
		slog.String("organizationID", group.Key.OrganizationID.String()),
		slog.Int("instanceNum", int(group.Key.InstanceNum)),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge))

	// Step 0: Validate that all messages in the group have consistent fields
	if err := validateIngestGroupConsistency(group); err != nil {
		return fmt.Errorf("group validation failed: %w", err)
	}

	// Create temporary directory for this ingestion run
	tmpDir, err := os.MkdirTemp("", "ingest-*")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			ll.Warn("Failed to cleanup temporary directory", slog.String("tmpDir", tmpDir), slog.Any("error", cleanupErr))
		}
	}()

	// Step 1: Get storage profile  
	storageProfile, err := p.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, group.Key.OrganizationID, group.Key.InstanceNum)
	if err != nil {
		return fmt.Errorf("get storage profile: %w", err)
	}

	// Step 2: Create storage client
	storageClient, err := cloudstorage.NewClient(ctx, p.cmgr, storageProfile)
	if err != nil {
		return fmt.Errorf("create storage client: %w", err)
	}

	// Step 3: Create accumulator manager and process files
	manager := &AccumulatorManager{
		accumulators: make(map[OrgInstanceKey]*IngestAccumulator),
	}

	var totalInputSize int64
	var inputFileCount int

	for _, accMsg := range group.Messages {
		msg, ok := accMsg.Message.(*messages.ObjStoreNotificationMessage)
		if !ok {
			continue // Skip non-ObjStoreNotificationMessage messages
		}

		ll.Debug("Processing raw metric file",
			slog.String("objectID", msg.ObjectID),
			slog.Int64("fileSize", msg.FileSize))

		// Create ingest item
		ingestItem := ingest.IngestItem{
			OrganizationID: msg.OrganizationID,
			InstanceNum:    msg.InstanceNum,
			Bucket:         msg.Bucket,
			ObjectID:       msg.ObjectID,
			FileSize:       msg.FileSize,
		}

		// Process file to reader
		reader, metadata, err := p.processFileToSortedReader(ctx, ingestItem, tmpDir, storageClient, nil, p.store)
		if err != nil {
			ll.Error("Failed to process file to reader",
				slog.String("objectID", msg.ObjectID),
				slog.Any("error", err))
			continue // Skip this file but continue with others
		}

		// Add to appropriate accumulator
		orgInstanceKey := OrgInstanceKey{
			OrganizationID: msg.OrganizationID,
			InstanceNum:    msg.InstanceNum,
		}

		accumulator, exists := manager.accumulators[orgInstanceKey]
		if !exists {
			accumulator = &IngestAccumulator{}
			manager.accumulators[orgInstanceKey] = accumulator
		}

		accumulator.readers = append(accumulator.readers, reader)
		accumulator.readerMetadata = append(accumulator.readerMetadata, metadata)
		accumulator.totalFileSize += metadata.FileSize

		totalInputSize += msg.FileSize
		inputFileCount++
	}

	if len(manager.accumulators) == 0 {
		ll.Info("No files processed successfully")
		return nil
	}

	// Step 4: Process all accumulators
	return p.processAccumulatedBatch(ctx, manager, tmpDir, storageProfile, storageClient, kafkaCommitData)
}

// GetTargetRecordCount returns the target file size limit (20MB) for accumulation
func (p *MetricIngestProcessor) GetTargetRecordCount(ctx context.Context, groupingKey messages.IngestKey) int64 {
	return 20 * 1024 * 1024 // 20MB file size limit instead of record count
}

// processFileToSortedReader processes a single file to a sorted reader
func (p *MetricIngestProcessor) processFileToSortedReader(ctx context.Context, item ingest.IngestItem, tmpDir string, storageClient cloudstorage.Client, exemplarProcessor *exemplar.Processor, mdb IngestStore) (filereader.Reader, ReaderMetadata, error) {
	ll := logctx.FromContext(ctx)

	// Download file directly to the shared tmpDir
	tmpfilename, _, is404, err := storageClient.DownloadObject(ctx, tmpDir, item.Bucket, item.ObjectID)
	if err != nil {
		return nil, ReaderMetadata{}, fmt.Errorf("failed to download file %s: %w", item.ObjectID, err)
	}
	if is404 {
		return nil, ReaderMetadata{}, fmt.Errorf("object not found: %s", item.ObjectID)
	}

	ll.Debug("Downloaded input file",
		slog.String("objectID", item.ObjectID),
		slog.Int64("fileSize", item.FileSize))

	// Create proto reader
	reader, err := CreateMetricProtoReader(tmpfilename)
	if err != nil {
		return nil, ReaderMetadata{}, fmt.Errorf("failed to create proto reader: %w", err)
	}

	// Process exemplars from this reader if exemplar processor is provided (do this before wrapping with other readers)
	if exemplarProcessor != nil {
		if err := processExemplarsFromReader(ctx, reader, exemplarProcessor, item.OrganizationID.String(), mdb); err != nil {
			// Just log error and continue - don't fail the whole file
			ll.Warn("Failed to process exemplars from file",
				slog.String("objectID", item.ObjectID),
				slog.Any("error", err))
		}
	}

	// Add translation
	translator := &MetricTranslator{
		OrgID:    item.OrganizationID.String(),
		Bucket:   item.Bucket,
		ObjectID: item.ObjectID,
	}
	reader, err = filereader.NewTranslatingReader(reader, translator, 1000)
	if err != nil {
		reader.Close()
		return nil, ReaderMetadata{}, fmt.Errorf("failed to create translating reader: %w", err)
	}

	// Add disk-based sorting
	keyProvider := metricsprocessing.GetCurrentMetricSortKeyProvider()
	reader, err = filereader.NewDiskSortingReader(reader, keyProvider, 1000)
	if err != nil {
		reader.Close()
		return nil, ReaderMetadata{}, fmt.Errorf("failed to create sorting reader: %w", err)
	}

	metadata := ReaderMetadata{
		ObjectID:       item.ObjectID,
		OrganizationID: item.OrganizationID,
		InstanceNum:    item.InstanceNum,
		Bucket:         item.Bucket,
		FileSize:       item.FileSize,
		IngestItem:     item,
	}

	return reader, metadata, nil
}

// processAccumulatedBatch processes an accumulated batch of metrics with multiple readers
func (p *MetricIngestProcessor) processAccumulatedBatch(ctx context.Context, manager *AccumulatorManager, tmpDir string, storageProfile storageprofile.StorageProfile, storageClient cloudstorage.Client, kafkaCommitData *KafkaCommitData) error {
	ll := logctx.FromContext(ctx)

	if len(manager.accumulators) == 0 {
		ll.Debug("No data to process in accumulation manager")
		return nil
	}

	// Collect all segment parameters from all (org, instance) accumulators
	var allSegmentParams []lrdb.InsertMetricSegmentParams
	var totalInputSize, totalOutputSize int64
	var totalInputRows, totalOutputRows int64
	var inputFileCount int
	var maxSlotCount int32

	// Process each (org, instance) accumulator independently
	for key, accumulator := range manager.accumulators {
		if len(accumulator.readers) == 0 {
			continue
		}

		ll.Debug("Processing accumulator",
			slog.String("organizationID", key.OrganizationID.String()),
			slog.Int("instanceNum", int(key.InstanceNum)),
			slog.Int("readerCount", len(accumulator.readers)))

		// Get RPF estimate for this org/instance
		rpfEstimate := p.getRPFEstimate(ctx, key.OrganizationID)

		// Process this accumulator
		results, inputRows, err := p.flushAccumulator(ctx, accumulator, tmpDir, 0, rpfEstimate)
		if err != nil {
			return fmt.Errorf("failed to flush accumulator for %v: %w", key, err)
		}
		totalInputRows += inputRows

		if len(results) == 0 {
			ll.Warn("No output files generated for accumulator",
				slog.String("organizationID", key.OrganizationID.String()),
				slog.Int("instanceNum", int(key.InstanceNum)))
			continue
		}

		// Prepare upload parameters
		uploadParams := metricsprocessing.UploadParams{
			OrganizationID: storageProfile.OrganizationID.String(),
			InstanceNum:    storageProfile.InstanceNum,
			Dateint:        0,     // Will be calculated from timestamps
			FrequencyMs:    10000, // 10 second blocks
			IngestDateint:  dateint(time.Now()),
			CollectorName:  storageProfile.CollectorName,
			Bucket:         storageProfile.Bucket,
			CreatedBy:      lrdb.CreatedByIngest,
		}

		// Create and upload segments for this (org, instance)
		segmentParams, err := p.createAndUploadSegments(ctx, storageClient, results, uploadParams)
		if err != nil {
			return fmt.Errorf("failed to create segments for %v: %w", key, err)
		}

		allSegmentParams = append(allSegmentParams, segmentParams...)

		// Update metrics
		for _, metadata := range accumulator.readerMetadata {
			totalInputSize += metadata.FileSize
			inputFileCount++
		}
		for _, result := range results {
			totalOutputSize += result.FileSize
			totalOutputRows += result.RecordCount
		}
		// Track max slot count from segments
		for _, segParams := range segmentParams {
			if segParams.SlotCount > maxSlotCount {
				maxSlotCount = segParams.SlotCount
			}
		}
	}

	if len(allSegmentParams) == 0 {
		ll.Warn("No segments created from accumulated batch")
		return nil
	}

	// Calculate size reduction percentage (e.g., 0.5 for 50% reduction)
	var sizeReduction float64
	if totalInputSize > 0 {
		sizeReduction = 1.0 - (float64(totalOutputSize) / float64(totalInputSize))
	}

	ll.Info("Accumulated batch processing summary",
		slog.Int("accumulatorCount", len(manager.accumulators)),
		slog.Int("inputSegmentCount", inputFileCount),
		slog.Int("outputSegmentCount", len(allSegmentParams)),
		slog.Int64("totalInputBytes", totalInputSize),
		slog.Int64("totalOutputBytes", totalOutputSize),
		slog.Int64("inputRows", totalInputRows),
		slog.Int64("outputRows", totalOutputRows),
		slog.Float64("sizeReduction", sizeReduction),
		slog.Int("slotCount", int(maxSlotCount)))

	// Execute atomic transaction: insert all segments + update all Kafka offsets  
	batch := lrdb.MetricSegmentBatch{
		Segments:     allSegmentParams,
		KafkaOffsets: []lrdb.KafkaOffsetUpdate{}, // TODO: Handle Kafka offsets properly
	}

	criticalCtx := context.WithoutCancel(ctx)
	if err := p.store.InsertMetricSegmentBatchWithKafkaOffsets(criticalCtx, batch); err != nil {
		return fmt.Errorf("failed to insert metric segments with Kafka offsets: %w", err)
	}

	ll.Info("Metric ingestion completed successfully",
		slog.Int("inputFiles", inputFileCount),
		slog.Int64("totalFileSize", totalInputSize))

	return nil
}

// getRPFEstimate gets the RPF estimate for a specific org using metric_pack_estimate table with caching
func (p *MetricIngestProcessor) getRPFEstimate(ctx context.Context, orgID uuid.UUID) int64 {
	// Initialize cache once
	rpfEstimateCacheOnce.Do(initRPFEstimateCache)

	// Create cache key - use orgID since estimates are per-org, not per-instance
	cacheKey := fmt.Sprintf("%s:10000", orgID.String()) // 10000ms frequency

	// Check cache first
	if item := rpfEstimateCache.Get(cacheKey); item != nil {
		return item.Value()
	}

	// Cache miss - query the database
	estimates, err := p.store.GetMetricPackEstimateForOrg(ctx, lrdb.GetMetricPackEstimateForOrgParams{
		OrganizationID: orgID,
		FrequencyMs:    10000, // 10 second blocks
	})

	var result int64 = 40_000 // Default fallback

	if err == nil {
		// If we have results, prefer the specific org estimate over the default
		for _, estimate := range estimates {
			// First result should be the specific org if it exists (due to ORDER BY organization_id DESC)
			if estimate.OrganizationID == orgID && estimate.TargetRecords != nil {
				result = *estimate.TargetRecords
				break
			}
		}

		// If no specific org estimate but we have a default (all zeros)
		if result == 40_000 && len(estimates) > 0 && estimates[len(estimates)-1].TargetRecords != nil {
			result = *estimates[len(estimates)-1].TargetRecords // Last one should be default
		}
	}

	// Cache the result (even if it's the fallback value)
	rpfEstimateCache.Set(cacheKey, result, ttlcache.DefaultTTL)

	return result
}

// flushAccumulator processes readers into output files (simplified version)
func (p *MetricIngestProcessor) flushAccumulator(ctx context.Context, accumulator *IngestAccumulator, tmpDir string, ingestDateint int32, rpfEstimate int64) ([]parquetwriter.Result, int64, error) {
	// This is a placeholder implementation that would use the parquet writer
	// For now, return empty results to allow compilation
	return []parquetwriter.Result{}, 0, nil
}

// createAndUploadSegments creates segment metadata and uploads files (simplified version)
func (p *MetricIngestProcessor) createAndUploadSegments(ctx context.Context, storageClient cloudstorage.Client, results []parquetwriter.Result, uploadParams metricsprocessing.UploadParams) ([]lrdb.InsertMetricSegmentParams, error) {
	// This is a placeholder implementation that would handle the upload and metadata creation
	// For now, return empty results to allow compilation
	return []lrdb.InsertMetricSegmentParams{}, nil
}

// CreateMetricProtoReader creates a protocol buffer reader for metrics files (copied from ingestion package)
func CreateMetricProtoReader(filename string) (filereader.Reader, error) {
	// For now, this is a placeholder that would implement the reader creation logic
	// This should match the logic from internal/metricsprocessing/ingestion/readers.go
	return nil, fmt.Errorf("CreateMetricProtoReader not implemented yet")
}

// MetricTranslator adds resource metadata to metric rows (copied from ingestion package)
type MetricTranslator struct {
	OrgID    string
	Bucket   string
	ObjectID string
}

// TranslateRow adds resource fields to each row (copied from ingestion package)
func (t *MetricTranslator) TranslateRow(row *filereader.Row) error {
	// For now, this is a placeholder that would implement the translation logic
	// This should match the logic from internal/metricsprocessing/ingestion/translator.go
	return fmt.Errorf("MetricTranslator.TranslateRow not implemented yet")
}

// processExemplarsFromReader processes exemplars from reader (placeholder)
func processExemplarsFromReader(ctx context.Context, reader filereader.Reader, exemplarProcessor *exemplar.Processor, orgID string, mdb IngestStore) error {
	// For now, this is a placeholder that would implement the exemplar processing logic
	return nil
}

// dateint converts a time to a dateint format (YYYYMMDD)
func dateint(t time.Time) int32 {
	y, m, d := t.Date()
	return int32(y*10000 + int(m)*100 + d)
}

