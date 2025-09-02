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

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/idgen"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// ProcessBatch processes a batch of metric ingest items
func ProcessBatch(
	ctx context.Context,
	ll *slog.Logger,
	tmpdir string,
	sp storageprofile.StorageProfileProvider,
	mdb lrdb.StoreFull,
	awsmanager *awsclient.Manager,
	items []lrdb.Inqueue,
	ingestDateint int32,
	rpfEstimate int64,
	exemplarProcessor *exemplar.Processor,
) error {
	batchID := idgen.GenerateShortBase32ID()
	ll = ll.With(slog.String("batchID", batchID))

	itemIDs := make([]string, len(items))
	for i, item := range items {
		itemIDs[i] = item.ID.String()
	}
	ll.Debug("Processing ingestion batch",
		slog.Int("itemCount", len(items)),
		slog.Any("itemIDs", itemIDs))
	// Prepare input
	ingestionInput := input{
		Items:             items,
		TmpDir:            tmpdir,
		IngestDateint:     ingestDateint,
		RPFEstimate:       rpfEstimate,
		ExemplarProcessor: exemplarProcessor,
		Logger:            ll,
	}

	// Execute ingestion
	result, err := coordinate(ctx, ingestionInput, sp, awsmanager, mdb)
	if err != nil {
		return fmt.Errorf("ingestion failed: %w", err)
	}

	if len(result.Results) == 0 {
		ll.Warn("No output files generated despite reading rows",
			slog.Int64("rowsRead", result.RowsRead),
			slog.Int64("rowsErrored", result.RowsErrored))
		return nil
	}

	if result.RowsErrored > 0 {
		ll.Warn("Some input rows were dropped due to processing errors",
			slog.Int64("totalDropped", result.RowsErrored),
			slog.Float64("dropRate", float64(result.RowsErrored)/float64(result.RowsRead)*100))
	}

	// Get storage profile for upload operations
	firstItem := items[0]
	profile, err := getStorageProfileForIngestion(ctx, sp, firstItem)
	if err != nil {
		return fmt.Errorf("failed to get storage profile: %w", err)
	}

	// Upload results and queue work
	return uploadAndQueue(ctx, ll, awsmanager, mdb, result.Results, profile, ingestDateint)
}

// uploadAndQueue uploads the results and queues follow-up work
func uploadAndQueue(
	ctx context.Context,
	ll *slog.Logger,
	awsmanager *awsclient.Manager,
	mdb lrdb.StoreFull,
	results []parquetwriter.Result,
	profile storageprofile.StorageProfile,
	ingestDateint int32,
) error {
	// Create cloud storage client based on profile
	storageClient, err := cloudstorage.NewClient(ctx, awsmanager, profile)
	if err != nil {
		return fmt.Errorf("failed to get cloud storage client: %w", err)
	}

	// Upload results and update database
	uploadParams := metricsprocessing.UploadParams{
		OrganizationID: profile.OrganizationID.String(),
		InstanceNum:    profile.InstanceNum,
		Dateint:        0,     // Will be calculated from timestamps
		FrequencyMs:    10000, // 10 second blocks
		IngestDateint:  ingestDateint,
		CollectorName:  profile.CollectorName,
		Bucket:         profile.Bucket,
		CreatedBy:      lrdb.CreatedByIngest,
	}

	// Use context without cancellation for critical section to ensure atomic completion
	criticalCtx := context.WithoutCancel(ctx)
	segments, err := metricsprocessing.UploadMetricResultsWithProcessedSegments(criticalCtx, ll, storageClient, mdb, results, uploadParams)
	if err != nil {
		return fmt.Errorf("failed to upload results: %w", err)
	}

	// Queue compaction and rollup for each uploaded segment
	if err := segments.QueueCompactionWork(criticalCtx, mdb, profile.OrganizationID, profile.InstanceNum, 10000); err != nil {
		return fmt.Errorf("failed to queue compaction work: %w", err)
	}

	if err := segments.QueueRollupWork(criticalCtx, mdb, profile.OrganizationID, profile.InstanceNum, 10000, 0, 1); err != nil {
		return fmt.Errorf("failed to queue rollup work: %w", err)
	}

	return nil
}

// ShouldProcessExemplars checks if exemplar processing should be enabled
// Returns false if LAKERUNNER_METRICS_EXEMPLARS is set to "false", "0", or "off"
func ShouldProcessExemplars() bool {
	return helpers.GetBoolEnv("LAKERUNNER_METRICS_EXEMPLARS", true)
}

// shouldUseSingleInstanceMode checks if single instance mode is enabled
// Returns true if LAKERUNNER_SINGLE_INSTANCE_NUM is set to "true" or "1"
func shouldUseSingleInstanceMode() bool {
	return helpers.GetBoolEnv("LAKERUNNER_SINGLE_INSTANCE_NUM", false)
}

// getStorageProfileForIngestion gets the appropriate storage profile for ingestion
// based on the LAKERUNNER_SINGLE_INSTANCE_NUM environment variable setting
func getStorageProfileForIngestion(
	ctx context.Context,
	sp storageprofile.StorageProfileProvider,
	firstItem lrdb.Inqueue,
) (storageprofile.StorageProfile, error) {
	if shouldUseSingleInstanceMode() {
		return sp.GetLowestInstanceStorageProfile(ctx, firstItem.OrganizationID, firstItem.Bucket)
	}

	if collectorName := helpers.ExtractCollectorName(firstItem.ObjectID); collectorName != "" {
		return sp.GetStorageProfileForOrganizationAndCollector(ctx, firstItem.OrganizationID, collectorName)
	}
	return sp.GetStorageProfileForOrganizationAndInstance(ctx, firstItem.OrganizationID, firstItem.InstanceNum)
}
