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
	pq "github.com/lib/pq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// logProcessingResult contains the output from DuckDB log processing.
type logProcessingResult struct {
	Results           []parquetwriter.Result
	ProcessedSegments []lrdb.LogSeg // Only segments that were successfully downloaded and processed
}

// logDownloadResult holds the result of downloading log segment files.
type logDownloadResult struct {
	localFiles         []string
	downloadedSegments []lrdb.LogSeg
}

// processLogsWithDuckDB performs log compaction using DuckDB:
// 1. Download parquet files from S3 to local disk
// 2. Load all files into a DuckDB table with union_by_name for schema merging
// 3. Sort by [service_identifier, chq_fingerprint, chq_timestamp]
// 4. Export sorted result to parquet
// 5. Merge metadata from input segments (timestamps, fingerprints, stream values)
// 6. Compute agg counts from DuckDB (10s bucket aggregation)
func processLogsWithDuckDB(ctx context.Context, params logProcessingParams) (*logProcessingResult, error) {
	span := trace.SpanFromContext(ctx)
	ll := logctx.FromContext(ctx)

	// Download parquet files to local temp directory
	downloaded, err := downloadLogSegmentFiles(ctx, params)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("download log segment files: %w", err)
	}
	defer cleanupLocalFiles(downloaded.localFiles)

	if len(downloaded.localFiles) == 0 {
		err := fmt.Errorf("no parquet files to process")
		span.RecordError(err)
		return nil, err
	}

	span.SetAttributes(
		attribute.Int("duckdb.input_file_count", len(downloaded.localFiles)),
		attribute.Int("duckdb.segment_count", len(params.ActiveSegments)),
		attribute.Int("duckdb.processed_segment_count", len(downloaded.downloadedSegments)),
	)

	ll.Info("Processing logs with DuckDB",
		slog.Int("fileCount", len(downloaded.localFiles)),
		slog.Int("skippedSegments", len(params.ActiveSegments)-len(downloaded.downloadedSegments)))

	// Create a temporary file-based DuckDB database (isolated per work item)
	dbPath := filepath.Join(params.TmpDir, "logs_compact.duckdb")
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

	// Load parquet files into table and export sorted result
	outputFile := filepath.Join(params.TmpDir, "logs_output.parquet")
	if err := executeLogCompaction(ctx, conn, downloaded.localFiles, outputFile, params.StreamField); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "log compaction query failed")
		return nil, fmt.Errorf("execute log compaction: %w", err)
	}

	// Get output file stats
	fileInfo, err := os.Stat(outputFile)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("stat output file: %w", err)
	}

	// Get record count from output file
	recordCount, err := getRecordCount(ctx, conn, outputFile)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("get record count: %w", err)
	}

	// Merge metadata from input segments (timestamps, fingerprints, stream values)
	metadata := computeLogStatsFromSegments(downloaded.downloadedSegments, params.StreamField)

	// Compute agg counts from DuckDB (10s bucket aggregation)
	aggCounts, err := extractAggCounts(ctx, conn, params.StreamField)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("extract agg counts: %w", err)
	}
	metadata.AggCounts = aggCounts

	result := parquetwriter.Result{
		FileName:    outputFile,
		FileSize:    fileInfo.Size(),
		RecordCount: recordCount,
		Metadata:    metadata,
	}

	span.SetAttributes(
		attribute.Int64("duckdb.output_record_count", recordCount),
		attribute.Int64("duckdb.output_file_size", fileInfo.Size()),
		attribute.Int("duckdb.agg_bucket_count", len(aggCounts)),
	)

	ll.Info("DuckDB log compaction completed",
		slog.Int64("recordCount", recordCount),
		slog.Int64("fileSize", fileInfo.Size()))

	return &logProcessingResult{
		Results:           []parquetwriter.Result{result},
		ProcessedSegments: downloaded.downloadedSegments,
	}, nil
}

// downloadLogSegmentFiles downloads log parquet files from cloud storage to local temp directory.
func downloadLogSegmentFiles(ctx context.Context, params logProcessingParams) (logDownloadResult, error) {
	ctx, span := boxerTracer.Start(ctx, "duckdb.download_log_segments", trace.WithAttributes(
		attribute.Int("segment_count", len(params.ActiveSegments)),
	))
	defer span.End()

	ll := logctx.FromContext(ctx)
	var result logDownloadResult

	for _, segment := range params.ActiveSegments {
		dateint, hour := helpers.MSToDateintHour(segment.TsRange.Lower.Int64)
		objectPath := helpers.MakeDBObjectID(
			params.OrganizationID,
			params.StorageProfile.CollectorName,
			dateint,
			hour,
			segment.SegmentID,
			"logs",
		)

		localPath, _, is404, err := params.StorageClient.DownloadObject(ctx, params.TmpDir, params.StorageProfile.Bucket, objectPath)
		if err != nil {
			ll.Warn("Failed to download log segment file, skipping segment",
				slog.Int64("segmentID", segment.SegmentID),
				slog.Any("error", err))
			continue
		}
		if is404 {
			ll.Warn("Log segment file not found, skipping segment",
				slog.Int64("segmentID", segment.SegmentID),
				slog.String("objectPath", objectPath))
			recordLogSegmentDownload404(
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

// executeLogCompaction loads parquet files into DuckDB and exports sorted result.
func executeLogCompaction(ctx context.Context, conn *sql.Conn, inputFiles []string, outputFile string, streamField string) error {
	ctx, span := boxerTracer.Start(ctx, "duckdb.execute_log_compaction", trace.WithAttributes(
		attribute.Int("input_file_count", len(inputFiles)),
	))
	defer span.End()

	// Step 1: Load parquet files into a table (union_by_name merges schemas)
	quotedFiles := make([]string, len(inputFiles))
	for i, f := range inputFiles {
		quotedFiles[i] = fmt.Sprintf("'%s'", escapeSingleQuote(f))
	}
	fileList := strings.Join(quotedFiles, ", ")

	createSQL := fmt.Sprintf("CREATE TABLE logs_input AS SELECT * FROM read_parquet([%s], union_by_name=true)", fileList)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to create logs_input table")
		return fmt.Errorf("create table: %w", err)
	}

	// Step 2: Get schema to check which columns exist
	schema, err := getTableSchema(ctx, conn, "logs_input")
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("get table schema: %w", err)
	}
	span.SetAttributes(attribute.Int("schema_column_count", len(schema)))

	// Step 3: Build ORDER BY clause based on available columns
	orderByClause := buildLogOrderByClause(schema, streamField)

	// Step 4: Export sorted result to parquet
	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT * FROM logs_input
			ORDER BY %s
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		orderByClause,
		escapeSingleQuote(outputFile))

	if _, err := conn.ExecContext(ctx, copyQuery); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "log export query failed")
		return fmt.Errorf("export parquet: %w", err)
	}

	return nil
}

// buildLogOrderByClause constructs the ORDER BY clause for log sorting.
// Sort order: [service_identifier, chq_fingerprint, chq_timestamp]
// where service_identifier = streamField > resource_customer_domain > resource_service_name
func buildLogOrderByClause(schema []string, streamField string) string {
	schemaSet := mapset.NewSet[string]()
	for _, col := range schema {
		schemaSet.Add(col)
	}

	var orderParts []string

	// Build service_identifier expression with priority fallback
	serviceIdentifierExpr := buildServiceIdentifierExpr(schemaSet, streamField)
	if serviceIdentifierExpr != "" {
		orderParts = append(orderParts, serviceIdentifierExpr)
	}

	// Add chq_fingerprint if present
	if schemaSet.Contains("chq_fingerprint") {
		orderParts = append(orderParts, pq.QuoteIdentifier("chq_fingerprint"))
	}

	// Add chq_timestamp if present
	if schemaSet.Contains("chq_timestamp") {
		orderParts = append(orderParts, pq.QuoteIdentifier("chq_timestamp"))
	}

	if len(orderParts) == 0 {
		// Fallback to just timestamp-like ordering if no sort columns found
		return "1" // No specific order
	}

	return strings.Join(orderParts, ", ")
}

// buildServiceIdentifierExpr builds a COALESCE expression for service identifier priority.
func buildServiceIdentifierExpr(schemaSet mapset.Set[string], streamField string) string {
	var candidates []string

	// Priority 1: configured stream field
	if streamField != "" && schemaSet.Contains(streamField) {
		candidates = append(candidates, fmt.Sprintf("NULLIF(%s, '')", pq.QuoteIdentifier(streamField)))
	}

	// Priority 2: resource_customer_domain
	if schemaSet.Contains("resource_customer_domain") {
		candidates = append(candidates, fmt.Sprintf("NULLIF(%s, '')", pq.QuoteIdentifier("resource_customer_domain")))
	}

	// Priority 3: resource_service_name
	if schemaSet.Contains("resource_service_name") {
		candidates = append(candidates, fmt.Sprintf("NULLIF(%s, '')", pq.QuoteIdentifier("resource_service_name")))
	}

	if len(candidates) == 0 {
		return ""
	}

	if len(candidates) == 1 {
		return candidates[0]
	}

	// Use COALESCE to fall through priorities
	return fmt.Sprintf("COALESCE(%s, '')", strings.Join(candidates, ", "))
}

// extractAggCounts queries for aggregation counts (10s buckets).
func extractAggCounts(ctx context.Context, conn *sql.Conn, streamField string) (map[factories.LogAggKey]int64, error) {
	// Check schema for available columns
	schema, err := getTableSchema(ctx, conn, "logs_input")
	if err != nil {
		return nil, err
	}

	schemaSet := mapset.NewSet[string]()
	for _, col := range schema {
		schemaSet.Add(col)
	}

	// Determine stream field to use
	var streamFieldToUse string
	if streamField != "" && schemaSet.Contains(streamField) {
		streamFieldToUse = streamField
	} else if schemaSet.Contains("resource_customer_domain") {
		streamFieldToUse = "resource_customer_domain"
	} else if schemaSet.Contains("resource_service_name") {
		streamFieldToUse = "resource_service_name"
	}

	// Build query based on available columns
	var selectParts []string
	var groupByParts []string

	// Bucket timestamp to 10s (10000ms) using integer division (//)
	if schemaSet.Contains("chq_timestamp") {
		selectParts = append(selectParts, "(chq_timestamp // 10000) * 10000 AS bucket_ts")
		groupByParts = append(groupByParts, "bucket_ts")
	} else {
		return nil, nil // No timestamp, no aggregation
	}

	// Log level
	if schemaSet.Contains("log_level") {
		selectParts = append(selectParts, "COALESCE(log_level, '') AS log_level_out")
		groupByParts = append(groupByParts, "log_level")
	} else {
		selectParts = append(selectParts, "'' AS log_level_out")
	}

	// Stream field
	if streamFieldToUse != "" {
		selectParts = append(selectParts, fmt.Sprintf("COALESCE(%s, '') AS stream_value", pq.QuoteIdentifier(streamFieldToUse)))
		groupByParts = append(groupByParts, "stream_value")
	} else {
		selectParts = append(selectParts, "'' AS stream_value")
	}

	selectParts = append(selectParts, "COUNT(*) AS cnt")

	query := fmt.Sprintf(
		"SELECT %s FROM logs_input GROUP BY %s",
		strings.Join(selectParts, ", "),
		strings.Join(groupByParts, ", "))

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query agg counts: %w", err)
	}
	defer func() { _ = rows.Close() }()

	aggCounts := make(map[factories.LogAggKey]int64)
	for rows.Next() {
		var bucketTS, count int64
		var logLevel, streamValue string
		if err := rows.Scan(&bucketTS, &logLevel, &streamValue, &count); err != nil {
			return nil, fmt.Errorf("scan agg row: %w", err)
		}

		key := factories.LogAggKey{
			TimestampBucket:  bucketTS,
			LogLevel:         logLevel,
			StreamFieldName:  streamFieldToUse,
			StreamFieldValue: streamValue,
		}
		aggCounts[key] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return aggCounts, nil
}

// computeLogStatsFromSegments merges metadata from input segments.
// This includes timestamps, fingerprints, stream values, and stream ID field.
func computeLogStatsFromSegments(segments []lrdb.LogSeg, streamField string) factories.LogsFileStats {
	var stats factories.LogsFileStats

	if len(segments) == 0 {
		return stats
	}

	// Initialize with first segment values
	stats.FirstTS = segments[0].TsRange.Lower.Int64
	stats.LastTS = segments[0].TsRange.Upper.Int64

	fingerprintSet := mapset.NewSet[int64]()
	streamValueSet := mapset.NewSet[string]()

	for _, seg := range segments {
		// Merge timestamp range
		if seg.TsRange.Lower.Int64 < stats.FirstTS {
			stats.FirstTS = seg.TsRange.Lower.Int64
		}
		if seg.TsRange.Upper.Int64 > stats.LastTS {
			stats.LastTS = seg.TsRange.Upper.Int64
		}

		// Merge fingerprints
		for _, fp := range seg.Fingerprints {
			fingerprintSet.Add(fp)
		}

		// Merge stream values
		for _, sv := range seg.StreamIds {
			streamValueSet.Add(sv)
		}

		// Take the first non-nil stream ID field
		if stats.StreamIdField == nil && seg.StreamIDField != nil {
			stats.StreamIdField = seg.StreamIDField
		}
	}

	// Build sorted slices
	fingerprints := fingerprintSet.ToSlice()
	slices.Sort(fingerprints)
	stats.Fingerprints = fingerprints

	stats.StreamValues = streamValueSet.ToSlice()

	// If streamField is configured but we didn't get it from segments, set it
	if stats.StreamIdField == nil && streamField != "" {
		stats.StreamIdField = &streamField
	}

	return stats
}
