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
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	pq "github.com/lib/pq"
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

// traceProcessingParams contains the parameters needed for trace processing.
type traceProcessingParams struct {
	TmpDir         string
	StorageClient  cloudstorage.Client
	OrganizationID uuid.UUID
	StorageProfile storageprofile.StorageProfile
	ActiveSegments []lrdb.TraceSeg
	MaxRecords     int64
}

// traceProcessingResult contains the output from DuckDB trace processing.
type traceProcessingResult struct {
	Results           []parquetwriter.Result
	ProcessedSegments []lrdb.TraceSeg
}

// traceDownloadResult holds the result of downloading trace segment files.
type traceDownloadResult struct {
	localFiles         []string
	downloadedSegments []lrdb.TraceSeg
}

// processTracesWithDuckDB performs trace compaction using DuckDB:
// 1. Download parquet files from S3 to local disk
// 2. Load all files into a DuckDB table with union_by_name for schema merging
// 3. Sort by [trace_id, chq_timestamp]
// 4. Export sorted result to parquet
// 5. Merge metadata from input segments (timestamps, fingerprints, label name map)
func processTracesWithDuckDB(ctx context.Context, params traceProcessingParams) (*traceProcessingResult, error) {
	span := trace.SpanFromContext(ctx)
	ll := logctx.FromContext(ctx)

	// Download parquet files to local temp directory
	downloaded, err := downloadTraceSegmentFiles(ctx, params)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("download trace segment files: %w", err)
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

	ll.Info("Processing traces with DuckDB",
		slog.Int("fileCount", len(downloaded.localFiles)),
		slog.Int("skippedSegments", len(params.ActiveSegments)-len(downloaded.downloadedSegments)))

	// Create a temporary file-based DuckDB database
	dbPath := filepath.Join(params.TmpDir, "traces_compact.duckdb")
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
	outputFile := filepath.Join(params.TmpDir, "traces_output.parquet")
	if err := executeTraceCompaction(ctx, conn, downloaded.localFiles, outputFile); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "trace compaction query failed")
		return nil, fmt.Errorf("execute trace compaction: %w", err)
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

	// Merge metadata from input segments (timestamps, fingerprints, label name map)
	metadata := computeTraceStatsFromSegments(downloaded.downloadedSegments)

	result := parquetwriter.Result{
		FileName:    outputFile,
		FileSize:    fileInfo.Size(),
		RecordCount: recordCount,
		Metadata:    metadata,
	}

	span.SetAttributes(
		attribute.Int64("duckdb.output_record_count", recordCount),
		attribute.Int64("duckdb.output_file_size", fileInfo.Size()),
	)

	ll.Info("DuckDB trace compaction completed",
		slog.Int64("recordCount", recordCount),
		slog.Int64("fileSize", fileInfo.Size()))

	return &traceProcessingResult{
		Results:           []parquetwriter.Result{result},
		ProcessedSegments: downloaded.downloadedSegments,
	}, nil
}

// downloadTraceSegmentFiles downloads trace parquet files from cloud storage.
func downloadTraceSegmentFiles(ctx context.Context, params traceProcessingParams) (traceDownloadResult, error) {
	ctx, span := boxerTracer.Start(ctx, "duckdb.download_trace_segments", trace.WithAttributes(
		attribute.Int("segment_count", len(params.ActiveSegments)),
	))
	defer span.End()

	ll := logctx.FromContext(ctx)
	var result traceDownloadResult

	for _, segment := range params.ActiveSegments {
		dateint, hour := helpers.MSToDateintHour(segment.TsRange.Lower.Int64)
		objectPath := helpers.MakeDBObjectID(
			params.OrganizationID,
			params.StorageProfile.CollectorName,
			dateint,
			hour,
			segment.SegmentID,
			"traces",
		)

		localPath, _, is404, err := params.StorageClient.DownloadObject(ctx, params.TmpDir, params.StorageProfile.Bucket, objectPath)
		if err != nil {
			ll.Warn("Failed to download trace segment file, skipping segment",
				slog.Int64("segmentID", segment.SegmentID),
				slog.Any("error", err))
			continue
		}
		if is404 {
			ll.Warn("Trace segment file not found, skipping segment",
				slog.Int64("segmentID", segment.SegmentID),
				slog.String("objectPath", objectPath))
			recordTraceSegmentDownload404(
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

// executeTraceCompaction loads parquet files into DuckDB and exports sorted result.
func executeTraceCompaction(ctx context.Context, conn *sql.Conn, inputFiles []string, outputFile string) error {
	ctx, span := boxerTracer.Start(ctx, "duckdb.execute_trace_compaction", trace.WithAttributes(
		attribute.Int("input_file_count", len(inputFiles)),
	))
	defer span.End()

	// Step 1: Load parquet files into a table (union_by_name merges schemas)
	quotedFiles := make([]string, len(inputFiles))
	for i, f := range inputFiles {
		quotedFiles[i] = fmt.Sprintf("'%s'", escapeSingleQuote(f))
	}
	fileList := strings.Join(quotedFiles, ", ")

	createSQL := fmt.Sprintf("CREATE TABLE traces_input AS SELECT * FROM read_parquet([%s], union_by_name=true)", fileList)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "failed to create traces_input table")
		return fmt.Errorf("create table: %w", err)
	}

	// Step 2: Get schema to check which columns exist
	schema, err := getTableSchema(ctx, conn, "traces_input")
	if err != nil {
		span.RecordError(err)
		return fmt.Errorf("get table schema: %w", err)
	}
	span.SetAttributes(attribute.Int("schema_column_count", len(schema)))

	// Step 3: Build ORDER BY clause based on available columns
	orderByClause := buildTraceOrderByClause(schema)

	// Step 4: Export sorted result to parquet
	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT * FROM traces_input
			ORDER BY %s
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		orderByClause,
		escapeSingleQuote(outputFile))

	if _, err := conn.ExecContext(ctx, copyQuery); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "trace export query failed")
		return fmt.Errorf("export parquet: %w", err)
	}

	return nil
}

// buildTraceOrderByClause constructs the ORDER BY clause for trace sorting.
// Sort order: [trace_id, chq_timestamp]
func buildTraceOrderByClause(schema []string) string {
	schemaSet := mapset.NewSet[string]()
	for _, col := range schema {
		schemaSet.Add(col)
	}

	var orderParts []string

	// Add trace_id if present
	if schemaSet.Contains("trace_id") {
		orderParts = append(orderParts, pq.QuoteIdentifier("trace_id"))
	}

	// Add chq_timestamp if present
	if schemaSet.Contains("chq_timestamp") {
		orderParts = append(orderParts, pq.QuoteIdentifier("chq_timestamp"))
	}

	if len(orderParts) == 0 {
		// Fallback if no sort columns found
		return "1"
	}

	return strings.Join(orderParts, ", ")
}

// computeTraceStatsFromSegments merges metadata from input segments.
// This includes timestamps, fingerprints, and label name map.
func computeTraceStatsFromSegments(segments []lrdb.TraceSeg) factories.TracesFileStats {
	var stats factories.TracesFileStats

	if len(segments) == 0 {
		return stats
	}

	// Initialize with first segment values
	stats.FirstTS = segments[0].TsRange.Lower.Int64
	stats.LastTS = segments[0].TsRange.Upper.Int64

	fingerprintSet := mapset.NewSet[int64]()
	labelNameSet := mapset.NewSet[string]()

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

		// Merge label names from LabelNameMap
		if len(seg.LabelNameMap) > 0 {
			var labelMap map[string]string
			if err := json.Unmarshal(seg.LabelNameMap, &labelMap); err == nil {
				for k := range labelMap {
					labelNameSet.Add(k)
				}
			}
		}
	}

	// Build sorted fingerprints slice
	fingerprints := fingerprintSet.ToSlice()
	slices.Sort(fingerprints)
	stats.Fingerprints = fingerprints

	// Build merged label name map
	if labelNameSet.Cardinality() > 0 {
		labelNames := labelNameSet.ToSlice()
		slices.Sort(labelNames)
		mergedMap := make(map[string]string, len(labelNames))
		for _, name := range labelNames {
			mergedMap[name] = name
		}
		if mapBytes, err := json.Marshal(mergedMap); err == nil {
			stats.LabelNameMap = mapBytes
		}
	}

	return stats
}
