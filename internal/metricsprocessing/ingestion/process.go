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
	"os"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
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

	// Upload results and queue work
	firstItem := items[0]
	return uploadAndQueue(ctx, ll, sp, awsmanager, mdb, result.Results, firstItem, ingestDateint)
}

// uploadAndQueue uploads the results and queues follow-up work
func uploadAndQueue(
	ctx context.Context,
	ll *slog.Logger,
	sp storageprofile.StorageProfileProvider,
	awsmanager *awsclient.Manager,
	mdb lrdb.StoreFull,
	results []parquetwriter.Result,
	firstItem lrdb.Inqueue,
	ingestDateint int32,
) error {
	// Get storage profile
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

	// Upload results and update database
	uploadParams := metricsprocessing.UploadParams{
		OrganizationID: firstItem.OrganizationID.String(),
		InstanceNum:    firstItem.InstanceNum,
		Dateint:        0,     // Will be calculated from timestamps
		FrequencyMs:    10000, // 10 second blocks
		IngestDateint:  ingestDateint,
		CollectorName:  firstItem.CollectorName,
		Bucket:         firstItem.Bucket,
		CreatedBy:      lrdb.CreatedByIngest,
	}

	// Use context without cancellation for critical section to ensure atomic completion
	criticalCtx := context.WithoutCancel(ctx)
	if err := metricsprocessing.UploadMetricResults(criticalCtx, ll, s3client, mdb, results, uploadParams); err != nil {
		return fmt.Errorf("failed to upload results: %w", err)
	}

	// Queue compaction and rollup for each time range represented in the results
	if err := queueMetricWorkForResults(criticalCtx, mdb, firstItem, results); err != nil {
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

// ShouldProcessExemplars checks if exemplar processing should be enabled
// Returns false if LAKERUNNER_METRICS_EXEMPLARS is set to "false", "0", or "off"
func ShouldProcessExemplars() bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("LAKERUNNER_METRICS_EXEMPLARS")))
	switch env {
	case "false", "0", "off", "no":
		return false
	case "":
		// Default to true if not set
		return true
	default:
		return true
	}
}
