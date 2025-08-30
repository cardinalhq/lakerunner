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
	"time"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/jackc/pgx/v5/pgtype"
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
	uploadResults, err := metricsprocessing.UploadMetricResults(criticalCtx, ll, s3client, mdb, results, uploadParams)
	if err != nil {
		return fmt.Errorf("failed to upload results: %w", err)
	}

	// Queue compaction for each uploaded segment
	if err := queueMetricWorkForUploadResults(criticalCtx, mdb, firstItem, uploadResults); err != nil {
		return fmt.Errorf("failed to queue metric work: %w", err)
	}

	return nil
}

// queueMetricWorkForUploadResults queues compaction work for uploaded segments
func queueMetricWorkForUploadResults(ctx context.Context, mdb lrdb.StoreFull, inf lrdb.Inqueue, uploadResults []metricsprocessing.UploadResult) error {
	const frequency10s = int64(10000) // 10 seconds - frequency for compaction work

	// Queue compaction work for each uploaded segment
	for _, uploadResult := range uploadResults {
		// Convert timestamps from milliseconds to PostgreSQL timestamptz
		startTime := time.Unix(uploadResult.StartTs/1000, (uploadResult.StartTs%1000)*1000000).UTC()
		endTime := time.Unix(uploadResult.EndTs/1000, (uploadResult.EndTs%1000)*1000000).UTC()

		// Construct the timestamp range for PostgreSQL
		tsRange := pgtype.Range[pgtype.Timestamptz]{
			Lower:     pgtype.Timestamptz{Time: startTime, Valid: true},
			Upper:     pgtype.Timestamptz{Time: endTime, Valid: true},
			LowerType: pgtype.Inclusive,
			UpperType: pgtype.Exclusive,
			Valid:     true,
		}

		err := mdb.PutMetricCompactionWork(ctx, lrdb.PutMetricCompactionWorkParams{
			OrganizationID: inf.OrganizationID,
			Dateint:        uploadResult.DateInt,
			FrequencyMs:    frequency10s,
			SegmentID:      uploadResult.SegmentID,
			InstanceNum:    inf.InstanceNum,
			TsRange:        tsRange,
			RecordCount:    uploadResult.RecordCount,
			Priority:       1, // Default priority for metric compaction work
		})
		if err != nil {
			return fmt.Errorf("queueing metric compaction work for segment %d: %w", uploadResult.SegmentID, err)
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
