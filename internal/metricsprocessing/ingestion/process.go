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
	"log/slog"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// ProcessBatch processes a batch of metric ingest items with atomic transaction including Kafka offset
func ProcessBatch(
	ctx context.Context,
	args ingest.ProcessBatchArgs,
	items []ingest.IngestItem,
	exemplarProcessor *exemplar.Processor,
	cfg Config,
) error {
	batchID := idgen.GenerateShortBase32ID()
	ll := logctx.FromContext(ctx).With(slog.String("batchID", batchID))

	objectIDs := make([]string, len(items))
	for i, item := range items {
		objectIDs[i] = item.ObjectID
	}
	ll.Debug("Processing ingestion batch",
		slog.Int("itemCount", len(items)),
		slog.Any("objectIDs", objectIDs))
	// Prepare input
	ingestionInput := input{
		Items:             items,
		TmpDir:            args.TmpDir,
		IngestDateint:     args.IngestDateint,
		RPFEstimate:       args.RPFEstimate,
		ExemplarProcessor: exemplarProcessor,
		Config:            cfg,
	}

	// Execute ingestion without database writes (we'll do batch transaction later)
	result, err := coordinate(ctx, ingestionInput, args.StorageProvider, args.CloudManager, nil)
	if err != nil {
		return fmt.Errorf("ingestion failed: %w", err)
	}

	if len(result.Results) == 0 {
		ll.Warn("No output files generated despite reading rows",
			slog.Int64("rowsRead", result.RowsRead),
			slog.Int64("rowsErrored", result.RowsErrored))
		// No segments to insert, just update Kafka offset
		if err := args.DB.KafkaJournalUpsert(ctx, lrdb.KafkaJournalUpsertParams{
			ConsumerGroup:       args.KafkaOffset.ConsumerGroup,
			Topic:               args.KafkaOffset.Topic,
			Partition:           args.KafkaOffset.Partition,
			LastProcessedOffset: args.KafkaOffset.Offset,
		}); err != nil {
			return fmt.Errorf("failed to update Kafka offset: %w", err)
		}
		return nil
	}

	if result.RowsErrored > 0 {
		ll.Warn("Some input rows were dropped due to processing errors",
			slog.Int64("totalDropped", result.RowsErrored),
			slog.Float64("dropRate", float64(result.RowsErrored)/float64(result.RowsRead)*100))
	}

	// Get storage profile and S3 client
	firstItem := items[0]
	profile, err := args.StorageProvider.GetStorageProfileForOrganizationAndInstance(ctx, firstItem.OrganizationID, firstItem.InstanceNum)
	if err != nil {
		return fmt.Errorf("failed to get storage profile: %w", err)
	}

	// Upload results to S3 and collect segment parameters
	storageClient, err := cloudstorage.NewClient(ctx, args.CloudManager, profile)
	if err != nil {
		return fmt.Errorf("failed to create storage client: %w", err)
	}

	uploadParams := metricsprocessing.UploadParams{
		OrganizationID: profile.OrganizationID.String(),
		InstanceNum:    profile.InstanceNum,
		Dateint:        0,     // Will be calculated from timestamps
		FrequencyMs:    10000, // 10 second blocks
		IngestDateint:  args.IngestDateint,
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
		CreatedBy:      lrdb.CreatedByIngest,
	}

	// Create segments and upload to S3, collecting segment parameters
	segmentParams, err := createAndUploadSegments(ctx, storageClient, result.Results, uploadParams)
	if err != nil {
		return fmt.Errorf("failed to create and upload segments: %w", err)
	}

	// Calculate input/output size metrics
	var totalInputSize int64
	for _, item := range items {
		totalInputSize += item.FileSize
	}

	var totalOutputSize int64
	for _, r := range result.Results {
		totalOutputSize += r.FileSize
	}

	var compressionRatio float64
	if totalInputSize > 0 {
		compressionRatio = (float64(totalOutputSize) / float64(totalInputSize)) * 100
	}

	ll.Debug("Metrics ingestion batch summary",
		slog.Int("inputFileCount", len(items)),
		slog.Int64("totalInputBytes", totalInputSize),
		slog.Int("outputFileCount", len(result.Results)),
		slog.Int64("totalOutputBytes", totalOutputSize),
		slog.Float64("compressionRatio", compressionRatio),
		slog.String("compressionRatioStr", fmt.Sprintf("%.1f%%", compressionRatio)))

	// Execute the atomic transaction: insert all segments + Kafka offsets
	batch := lrdb.MetricSegmentBatch{
		Segments:     segmentParams,
		KafkaOffsets: []lrdb.KafkaOffsetUpdate{args.KafkaOffset},
	}

	criticalCtx := context.WithoutCancel(ctx)
	if err := args.DB.InsertMetricSegmentBatchWithKafkaOffsets(criticalCtx, batch); err != nil {
		return fmt.Errorf("failed to insert metric segments with Kafka offsets: %w", err)
	}

	// Queue compaction work
	for _, segParams := range segmentParams {
		if err := args.DB.McqQueueWork(criticalCtx, lrdb.McqQueueWorkParams{
			OrganizationID: segParams.OrganizationID,
			Dateint:        segParams.Dateint,
			FrequencyMs:    segParams.FrequencyMs,
			SegmentID:      segParams.SegmentID,
			InstanceNum:    segParams.InstanceNum,
			RecordCount:    segParams.RecordCount,
		}); err != nil {
			return fmt.Errorf("failed to queue compaction work: %w", err)
		}
	}

	return nil
}

// createAndUploadSegments creates segments from parquet results, uploads them to S3, and returns segment parameters
func createAndUploadSegments(ctx context.Context, blobclient cloudstorage.Client, results []parquetwriter.Result, uploadParams metricsprocessing.UploadParams) ([]lrdb.InsertMetricSegmentParams, error) {
	ll := logctx.FromContext(ctx)

	orgUUID, err := uuid.Parse(uploadParams.OrganizationID)
	if err != nil {
		return nil, fmt.Errorf("invalid organization ID: %w", err)
	}

	segmentParams := make([]lrdb.InsertMetricSegmentParams, 0, len(results))

	for _, result := range results {
		// Safety check: should never get empty results from the writer
		if result.RecordCount == 0 {
			ll.Error("Received empty result from writer - this should not happen",
				slog.String("fileName", result.FileName),
				slog.Int64("recordCount", result.RecordCount))
			return nil, fmt.Errorf("received empty result file with 0 records")
		}

		segment, err := metricsprocessing.NewProcessedSegment(ctx, result, orgUUID, uploadParams.CollectorName)
		if err != nil {
			return nil, fmt.Errorf("failed to create processed segment: %w", err)
		}

		// Upload to S3
		if err := segment.UploadToS3(ctx, blobclient, uploadParams.Bucket); err != nil {
			return nil, fmt.Errorf("uploading file to S3: %w", err)
		}

		ll.Debug("Metric segment stats",
			slog.Int64("segmentID", segment.SegmentID),
			slog.Int64("recordCount", result.RecordCount),
			slog.Int64("fileSize", result.FileSize))

		// Create segment parameters for database insertion
		dateint, _ := segment.GetDateintHour()
		params := lrdb.InsertMetricSegmentParams{
			OrganizationID: orgUUID,
			Dateint:        dateint,
			IngestDateint:  uploadParams.IngestDateint,
			FrequencyMs:    int32(uploadParams.FrequencyMs),
			SegmentID:      segment.SegmentID,
			InstanceNum:    uploadParams.InstanceNum,
			SlotID:         0, // Will be calculated based on segment ID
			SlotCount:      1,
			StartTs:        segment.StartTs,
			EndTs:          segment.EndTs,
			RecordCount:    result.RecordCount,
			FileSize:       result.FileSize,
			CreatedBy:      uploadParams.CreatedBy,
			Published:      true,
			Compacted:      false,
			Fingerprints:   segment.Fingerprints,
			SortVersion:    lrdb.CurrentMetricSortVersion,
		}

		segmentParams = append(segmentParams, params)
	}

	return segmentParams, nil
}
