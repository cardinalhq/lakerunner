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

// Package metricsprocessing contains shared business logic for metrics ingestion and compaction
// using the new filereader/parquetwriter architecture.
package metricsprocessing

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/awsclient/s3helper"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// MetricTranslator adds resource metadata to metric rows
type MetricTranslator struct {
	OrgID    string
	Bucket   string
	ObjectID string
}

// TranslateRow adds resource fields to each row
// Assumes all other metric fields (including sketches) are properly set by the proto reader
func (t *MetricTranslator) TranslateRow(row *filereader.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Only set the specific required fields - assume all other fields are properly set
	(*row)[wkk.NewRowKey("resource.bucket.name")] = t.Bucket
	(*row)[wkk.NewRowKey("resource.file.name")] = "./" + t.ObjectID
	(*row)[wkk.RowKeyCCustomerID] = t.OrgID
	(*row)[wkk.RowKeyCTelemetryType] = "metrics"

	// Validate required timestamp field - drop row if missing or invalid
	timestamp, ok := (*row)[wkk.RowKeyCTimestamp].(int64)
	if !ok {
		return fmt.Errorf("_cardinalhq.timestamp field is missing or not int64")
	}

	// Truncate timestamp to nearest 10-second interval
	const tenSecondsMs = int64(10000)
	truncatedTimestamp := (timestamp / tenSecondsMs) * tenSecondsMs
	(*row)[wkk.RowKeyCTimestamp] = truncatedTimestamp

	// Compute and add TID field
	metricName, nameOk := (*row)[wkk.RowKeyCName].(string)
	if !nameOk {
		return fmt.Errorf("missing or invalid _cardinalhq.name field for TID computation")
	}

	tid := helpers.ComputeTID(metricName, pipeline.ToStringMap(*row))
	(*row)[wkk.RowKeyCTID] = tid

	return nil
}

// GetCurrentMetricSortKeyProvider returns the provider for creating sort keys
// for the current metric sort version. This is the single source of truth for metric sorting.
func GetCurrentMetricSortKeyProvider() filereader.SortKeyProvider {
	// This provider corresponds to lrdb.CurrentMetricSortVersion (SortVersionNameTidTimestamp)
	return &filereader.MetricSortKeyProvider{}
}

// MetricsOrderedSelector returns a SelectFunc for MergesortReader that orders by [metric name, TID].
// This ensures the same ordering used by the parquet writer during ingestion and compaction.
// TODO this should not generate keys on the fly like this, that will hurt GC.
// TODO the fix is likely to make callers of this track the key for the items it wants, and use .Compare() directly.
func MetricsOrderedSelector() filereader.SelectFunc {
	keyProvider := GetCurrentMetricSortKeyProvider()

	return func(rows []filereader.Row) int {
		if len(rows) == 0 {
			return -1
		}

		minIdx := 0
		minKey := keyProvider.MakeKey(rows[0])
		defer minKey.Release()

		for i := 1; i < len(rows); i++ {
			key := keyProvider.MakeKey(rows[i])
			if key.Compare(minKey) < 0 {
				minKey.Release()
				minIdx = i
				minKey = key
			} else {
				key.Release()
			}
		}

		return minIdx
	}
}

// GetMetricSortKey extracts the metric sort key [metric_name:tid] from a row.
func GetMetricSortKey(row map[string]any) string {
	name, nameOk := row["_cardinalhq.name"].(string)
	tid, tidOk := row["_cardinalhq.tid"].(int64)
	if nameOk && tidOk {
		return fmt.Sprintf("%s:%d", name, tid)
	}
	return ""
}

// GetStartEndTimes calculates the time range from a set of metric segments.
func GetStartEndTimes(rows []lrdb.MetricSeg) (int64, int64) {
	startTs := int64(math.MaxInt64)
	endTs := int64(math.MinInt64)
	for _, row := range rows {
		rowStartTs := row.TsRange.Lower.Int64
		rowEndTs := row.TsRange.Upper.Int64
		if rowStartTs < startTs {
			startTs = rowStartTs
		}
		if rowEndTs > endTs {
			endTs = rowEndTs
		}
	}
	return startTs, endTs
}

// GetIngestDateint returns the maximum ingest dateint from a set of metric segments.
func GetIngestDateint(rows []lrdb.MetricSeg) int32 {
	if len(rows) == 0 {
		return 0
	}
	ingestDateint := int32(0)
	for _, row := range rows {
		if row.IngestDateint > ingestDateint {
			ingestDateint = row.IngestDateint
		}
	}
	return ingestDateint
}

// UploadParams contains parameters for uploading metric files to S3 and database.
type UploadParams struct {
	OrganizationID string
	InstanceNum    int16
	Dateint        int32
	FrequencyMs    int32
	IngestDateint  int32
	CollectorName  string
	Bucket         string
	CreatedBy      lrdb.CreatedBy
}

// UploadMetricResults uploads parquet files to S3 and updates the database with segment records.
func UploadMetricResults(
	ctx context.Context,
	ll *slog.Logger,
	s3client *awsclient.S3Client,
	mdb lrdb.StoreFull,
	results []parquetwriter.Result,
	params UploadParams,
) error {
	for _, result := range results {
		if err := uploadSingleMetricResult(ctx, ll, s3client, mdb, result, params); err != nil {
			return fmt.Errorf("failed to upload result: %w", err)
		}
	}
	return nil
}

// uploadSingleMetricResult uploads a single parquet file result to S3 and database.
func uploadSingleMetricResult(
	ctx context.Context,
	ll *slog.Logger,
	s3client *awsclient.S3Client,
	mdb lrdb.StoreFull,
	result parquetwriter.Result,
	params UploadParams,
) error {
	// Safety check: should never get empty results from the writer
	if result.RecordCount == 0 {
		ll.Error("Received empty result from writer - this should not happen",
			slog.String("fileName", result.FileName),
			slog.Int64("recordCount", result.RecordCount))
		return fmt.Errorf("received empty result file with 0 records")
	}

	// Generate segment ID and object ID
	segmentID := s3helper.GenerateID()

	// Extract dateint and hour from actual timestamp data
	var dateint int32
	var hour int16
	if stats, ok := result.Metadata.(factories.MetricsFileStats); ok && stats.FirstTS > 0 {
		// Convert milliseconds to seconds, then extract date and hour
		t := time.Unix(stats.FirstTS/1000, 0).UTC()
		dateint = int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
		hour = int16(t.Hour())
	} else {
		// No valid timestamps - this shouldn't happen if validation worked correctly
		return fmt.Errorf("no valid timestamps in result metadata - cannot determine dateint/hour")
	}

	orgUUID, err := uuid.Parse(params.OrganizationID)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}
	objID := helpers.MakeDBObjectID(orgUUID, params.CollectorName, dateint, hour, segmentID, "metrics")

	// Upload to S3
	if err := s3helper.UploadS3Object(ctx, s3client, params.Bucket, objID, result.FileName); err != nil {
		return fmt.Errorf("uploading file to S3: %w", err)
	}

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
			slog.String("objectID", objID),
			slog.Int64("segmentID", segmentID),
			slog.Int("fingerprintCount", len(fingerprints)),
			slog.Int64("startTs", startTs),
			slog.Int64("endTs", endTs))
	} else {
		ll.Error("Failed to extract MetricsFileStats from result metadata",
			slog.String("metadataType", fmt.Sprintf("%T", result.Metadata)))
		return fmt.Errorf("missing or invalid MetricsFileStats in result metadata")
	}

	// Insert segment record
	err = mdb.InsertMetricSegment(ctx, lrdb.InsertMetricSegmentParams{
		OrganizationID: orgUUID,
		FrequencyMs:    params.FrequencyMs,
		Dateint:        dateint,
		IngestDateint:  params.IngestDateint,
		SegmentID:      segmentID,
		InstanceNum:    params.InstanceNum,
		SlotID:         0,
		SlotCount:      1,
		StartTs:        startTs,
		EndTs:          endTs,
		RecordCount:    result.RecordCount,
		FileSize:       result.FileSize,
		Published:      true,
		CreatedBy:      params.CreatedBy,
		Fingerprints:   fingerprints,
		SortVersion:    lrdb.CurrentMetricSortVersion, // Files from ingestion use current sort version
	})
	if err != nil {
		// Clean up uploaded file on database error
		if err2 := s3helper.DeleteS3Object(ctx, s3client, params.Bucket, objID); err2 != nil {
			ll.Error("Failed to delete S3 object after insertion failure", slog.Any("error", err2))
		}
		return fmt.Errorf("inserting metric segment: %w", err)
	}

	return nil
}

// NormalizeRowForParquetWrite normalizes sketch fields for parquet writing.
// Converts string sketches to []byte format required by parquet.
func NormalizeRowForParquetWrite(row filereader.Row) error {
	sketch := row[wkk.RowKeySketch]
	if sketch == nil {
		return nil
	}

	if _, ok := sketch.([]byte); ok {
		return nil
	}

	if str, ok := sketch.(string); ok {
		row[wkk.RowKeySketch] = []byte(str)
		return nil
	}

	return fmt.Errorf("unexpected sketch type for parquet writing: %T", sketch)
}
