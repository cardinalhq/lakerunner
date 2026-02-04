// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// metricProcessingParams contains the parameters needed for metric processing
type metricProcessingParams struct {
	TmpDir         string
	StorageClient  cloudstorage.Client
	OrganizationID uuid.UUID
	StorageProfile storageprofile.StorageProfile
	ActiveSegments []lrdb.MetricSeg
	FrequencyMs    int32
	MaxRecords     int64
}

// metricProcessingResult contains the output from DuckDB processing
type metricProcessingResult struct {
	Results           []parquetwriter.Result
	ProcessedSegments []lrdb.MetricSeg // Only segments that were successfully downloaded and processed
}

// processMetricsWithDuckDB performs metric aggregation using DuckDB SQL:
// 1. Read parquet files from local disk
// 2. Truncate timestamps to aggregation period
// 3. Group by (metric_name, chq_tid, truncated_timestamp)
// 4. Merge DDSketches using ddsketch_agg
// 5. Extract statistics from merged sketch
// 6. Write result to parquet
//
// Returns both the results and the list of segments that were successfully processed.
// Segments that failed to download are skipped and not included in ProcessedSegments.
func processMetricsWithDuckDB(ctx context.Context, params metricProcessingParams) (*metricProcessingResult, error) {
	span := trace.SpanFromContext(ctx)
	ll := logctx.FromContext(ctx)

	// Download parquet files to local temp directory
	downloaded, err := downloadMetricSegmentFiles(ctx, params)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("download segment files: %w", err)
	}
	defer cleanupLocalFiles(downloaded.localFiles)

	if len(downloaded.localFiles) == 0 {
		err := fmt.Errorf("no parquet files to process")
		span.RecordError(err)
		return nil, err
	}

	span.SetAttributes(
		attribute.Int("duckdb.input_file_count", len(downloaded.localFiles)),
		attribute.Int("duckdb.frequency_ms", int(params.FrequencyMs)),
		attribute.Int("duckdb.segment_count", len(params.ActiveSegments)),
		attribute.Int("duckdb.processed_segment_count", len(downloaded.downloadedSegments)),
	)

	ll.Info("Processing metrics with DuckDB",
		slog.Int("fileCount", len(downloaded.localFiles)),
		slog.Int("frequencyMs", int(params.FrequencyMs)),
		slog.Int("skippedSegments", len(params.ActiveSegments)-len(downloaded.downloadedSegments)))

	// Create a temporary file-based DuckDB database (isolated per work item)
	dbPath := filepath.Join(params.TmpDir, "metrics_rollup.duckdb")
	db, err := duckdbx.NewDB(duckdbx.WithDatabasePath(dbPath))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("create duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("get duckdb connection: %w", err)
	}
	defer release()

	// Load DDSketch extension
	if err := duckdbx.LoadDDSketchExtension(ctx, conn); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("load ddsketch extension: %w", err)
	}

	// Generate and execute aggregation SQL
	outputFile := filepath.Join(params.TmpDir, "rollup_output.parquet")
	if err := executeAggregationConn(ctx, conn, downloaded.localFiles, outputFile, params.FrequencyMs); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "aggregation query failed")
		return nil, fmt.Errorf("execute aggregation: %w", err)
	}

	// Get output file stats
	fileInfo, err := os.Stat(outputFile)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("stat output file: %w", err)
	}

	// Get record count from output file (the only thing we need to query)
	recordCount, err := getRecordCount(ctx, conn, outputFile)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("get record count: %w", err)
	}

	// Compute metadata only from successfully downloaded segments
	firstTS, lastTS, fingerprints, metricNames, metricTypes := computeStatsFromSegments(downloaded.downloadedSegments)

	result := parquetwriter.Result{
		FileName:    outputFile,
		FileSize:    fileInfo.Size(),
		RecordCount: recordCount,
		Metadata: factories.MetricsFileStats{
			FirstTS:      firstTS,
			LastTS:       lastTS,
			Fingerprints: fingerprints,
			MetricNames:  metricNames,
			MetricTypes:  metricTypes,
		},
	}

	span.SetAttributes(
		attribute.Int64("duckdb.output_record_count", recordCount),
		attribute.Int64("duckdb.output_file_size", fileInfo.Size()),
	)

	ll.Info("DuckDB aggregation completed",
		slog.Int64("recordCount", recordCount),
		slog.Int64("fileSize", fileInfo.Size()))

	return &metricProcessingResult{
		Results:           []parquetwriter.Result{result},
		ProcessedSegments: downloaded.downloadedSegments,
	}, nil
}

// downloadResult holds the result of downloading segment files.
type downloadResult struct {
	localFiles         []string
	downloadedSegments []lrdb.MetricSeg
}

// downloadMetricSegmentFiles downloads parquet files from cloud storage to local temp directory.
// Returns both local file paths and the segments that were successfully downloaded.
func downloadMetricSegmentFiles(ctx context.Context, params metricProcessingParams) (downloadResult, error) {
	ctx, span := boxerTracer.Start(ctx, "duckdb.download_segments", trace.WithAttributes(
		attribute.Int("segment_count", len(params.ActiveSegments)),
	))
	defer span.End()

	ll := logctx.FromContext(ctx)
	var result downloadResult

	for _, segment := range params.ActiveSegments {
		objectPath := helpers.MakeDBObjectID(
			params.OrganizationID,
			params.StorageProfile.CollectorName,
			segment.Dateint,
			int16((segment.TsRange.Lower.Int64/(1000*60*60))%24),
			segment.SegmentID,
			"metrics",
		)

		localPath, _, is404, err := params.StorageClient.DownloadObject(ctx, params.TmpDir, params.StorageProfile.Bucket, objectPath)
		if err != nil {
			ll.Warn("Failed to download segment file, skipping segment",
				slog.Int64("segmentID", segment.SegmentID),
				slog.Any("error", err))
			continue
		}
		if is404 {
			ll.Warn("Metric segment file not found, skipping segment",
				slog.Int64("segmentID", segment.SegmentID),
				slog.String("objectPath", objectPath))
			recordMetricSegmentDownload404(
				ctx,
				params.StorageProfile.Bucket,
				params.OrganizationID,
				params.StorageProfile.InstanceNum,
				segment.CreatedBy,
			)
			continue
		}

		result.localFiles = append(result.localFiles, localPath)
		result.downloadedSegments = append(result.downloadedSegments, segment)
	}

	span.SetAttributes(
		attribute.Int("downloaded_count", len(result.localFiles)),
		attribute.Int("skipped_count", len(params.ActiveSegments)-len(result.localFiles)),
	)
	return result, nil
}

// cleanupLocalFiles removes temporary local files.
func cleanupLocalFiles(files []string) {
	for _, f := range files {
		_ = os.Remove(f)
	}
}

// escapeSingleQuote escapes single quotes for safe inclusion in SQL string literals.
func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// executeAggregationConn runs the DuckDB aggregation query using the same
// table-based pattern as metric ingest: load files into table, aggregate, export.
func executeAggregationConn(ctx context.Context, conn *sql.Conn, inputFiles []string, outputFile string, frequencyMs int32) error {
	ctx, span := boxerTracer.Start(ctx, "duckdb.execute_aggregation", trace.WithAttributes(
		attribute.Int("input_file_count", len(inputFiles)),
		attribute.Int("frequency_ms", int(frequencyMs)),
	))
	defer span.End()

	// Step 1: Load parquet files into a table (same pattern as metric ingest)
	quotedFiles := make([]string, len(inputFiles))
	for i, f := range inputFiles {
		quotedFiles[i] = fmt.Sprintf("'%s'", escapeSingleQuote(f))
	}
	fileList := strings.Join(quotedFiles, ", ")

	createSQL := fmt.Sprintf("CREATE TABLE metrics_input AS SELECT * FROM read_parquet([%s], union_by_name=true)", fileList)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to create metrics_input table")
		return fmt.Errorf("create table: %w", err)
	}

	// Step 2: Discover schema from the table (handles union of all input schemas)
	schema, err := getTableSchema(ctx, conn, "metrics_input")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to get table schema")
		return fmt.Errorf("get table schema: %w", err)
	}
	span.SetAttributes(attribute.Int("schema_column_count", len(schema)))

	// Step 3: Build aggregation clauses using shared function
	selectClauses, groupByClauses := buildAggregationClauses(schema, int64(frequencyMs))

	// Step 4: Build and execute COPY query (same pattern as metric ingest)
	rollupExtraction := []string{
		"chq_stats.sketch AS chq_sketch",
		"chq_stats.count AS chq_rollup_count",
		"chq_stats.sum AS chq_rollup_sum",
		"chq_stats.avg AS chq_rollup_avg",
		"chq_stats.min AS chq_rollup_min",
		"chq_stats.max AS chq_rollup_max",
		"chq_stats.p25 AS chq_rollup_p25",
		"chq_stats.p50 AS chq_rollup_p50",
		"chq_stats.p75 AS chq_rollup_p75",
		"chq_stats.p90 AS chq_rollup_p90",
		"chq_stats.p95 AS chq_rollup_p95",
		"chq_stats.p99 AS chq_rollup_p99",
	}

	innerQuery := fmt.Sprintf(`
		SELECT %s
		FROM metrics_input
		GROUP BY %s`,
		strings.Join(selectClauses, ", "),
		strings.Join(groupByClauses, ", "))

	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT * EXCLUDE (chq_stats), %s
			FROM (%s) AS aggregated
			ORDER BY "metric_name", "chq_tid", "chq_tsns"
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		strings.Join(rollupExtraction, ", "),
		innerQuery,
		escapeSingleQuote(outputFile))

	if _, err := conn.ExecContext(ctx, copyQuery); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "aggregation query failed")
		return err
	}
	return nil
}

// getRecordCount queries the record count from a parquet file.
func getRecordCount(ctx context.Context, conn *sql.Conn, filePath string) (int64, error) {
	ctx, span := boxerTracer.Start(ctx, "duckdb.get_record_count")
	defer span.End()

	var count int64
	err := conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", escapeSingleQuote(filePath))).Scan(&count)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "count query failed")
		return 0, fmt.Errorf("count records: %w", err)
	}
	span.SetAttributes(attribute.Int64("record_count", count))
	return count, nil
}

// computeStatsFromSegments computes merged metadata from input segments.
func computeStatsFromSegments(segments []lrdb.MetricSeg) (firstTS, lastTS int64, fingerprints []int64, metricNames []string, metricTypes []int16) {
	firstTS = int64(^uint64(0) >> 1) // max int64
	lastTS = int64(0)

	fingerprintSet := mapset.NewSet[int64]()
	metricNameSet := mapset.NewSet[string]()
	metricTypeMap := make(map[string]int16)

	for _, seg := range segments {
		// Merge timestamp range
		if seg.TsRange.Lower.Int64 < firstTS {
			firstTS = seg.TsRange.Lower.Int64
		}
		if seg.TsRange.Upper.Int64 > lastTS {
			lastTS = seg.TsRange.Upper.Int64
		}

		// Merge fingerprints
		for _, fp := range seg.Fingerprints {
			fingerprintSet.Add(fp)
		}

		// Merge metric names and types
		for i, name := range seg.MetricNames {
			metricNameSet.Add(name)
			if _, exists := metricTypeMap[name]; !exists && i < len(seg.MetricTypes) {
				metricTypeMap[name] = seg.MetricTypes[i]
			}
		}
	}

	// Build sorted slices
	fingerprints = fingerprintSet.ToSlice()
	slices.Sort(fingerprints)

	metricNames = metricNameSet.ToSlice()
	slices.Sort(metricNames)

	metricTypes = make([]int16, len(metricNames))
	for i, name := range metricNames {
		metricTypes[i] = metricTypeMap[name]
	}

	return firstTS, lastTS, fingerprints, metricNames, metricTypes
}
