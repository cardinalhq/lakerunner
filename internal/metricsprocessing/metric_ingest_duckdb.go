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
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/lib/pq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/lrdb"
)

const (
	// msPerDay is milliseconds per day (24 hours)
	msPerDay = int64(86400000)

	// aggregationWindowMs is the 10-second aggregation window in milliseconds
	aggregationWindowMs = int64(10000)
)

// MetricIngestDuckDBResult contains the results of DuckDB-based metric ingestion
type MetricIngestDuckDBResult struct {
	DateintBins map[int32]*DateintBinResult
	TotalRows   int64
	// FailedPartitions tracks dateint keys that failed processing with their errors.
	// Processing uses best-effort strategy: failures are recorded but don't stop
	// processing of other partitions. Callers should check this to detect partial results.
	FailedPartitions map[int64]error
}

// DateintBinResult contains the result for a single dateint partition
type DateintBinResult struct {
	Dateint     int32
	OutputFile  string // Local parquet file path
	RecordCount int64
	FileSize    int64
	Metadata    *MetricFileMetadata
}

// MetricFileMetadata contains metadata extracted from the parquet file
type MetricFileMetadata struct {
	StartTs      int64    // Inclusive start timestamp in ms
	EndTs        int64    // Exclusive end timestamp in ms (lastTs + 1)
	Hour         int16    // Hour of day for the first timestamp
	Fingerprints []int64  // Fingerprints for indexing
	MetricNames  []string // Unique metric names, sorted
	MetricTypes  []int16  // Metric types parallel to MetricNames
	LabelNameMap []byte   // JSON map of label columns
}

// processMetricIngestWithDuckDB processes binpb files using pure DuckDB SQL.
// This loads all files into a table, partitions by dateint, aggregates to 10s,
// and exports each partition to a separate parquet file.
func processMetricIngestWithDuckDB(
	ctx context.Context,
	binpbFiles []string,
	orgID string,
	tmpDir string,
) (*MetricIngestDuckDBResult, error) {
	span := trace.SpanFromContext(ctx)
	ll := logctx.FromContext(ctx)

	if len(binpbFiles) == 0 {
		return nil, fmt.Errorf("no binpb files provided")
	}

	span.SetAttributes(
		attribute.Int("duckdb.input_file_count", len(binpbFiles)),
		attribute.String("duckdb.org_id", orgID),
	)

	ll.Info("Processing metrics with pure DuckDB pipeline",
		slog.Int("fileCount", len(binpbFiles)))

	// Create a temporary file-based DuckDB database for memory efficiency
	dbPath := filepath.Join(tmpDir, "metrics_ingest.duckdb")
	db, err := duckdbx.NewDB(duckdbx.WithDatabasePath(dbPath))
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("create duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	if err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("get connection: %w", err)
	}
	defer release()

	// Load required extensions
	if err := duckdbx.LoadOtelBinpbExtension(ctx, conn); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("load otel_binpb extension: %w", err)
	}

	if err := duckdbx.LoadDDSketchExtension(ctx, conn); err != nil {
		span.RecordError(err)
		return nil, fmt.Errorf("load ddsketch extension: %w", err)
	}

	// Step 1: Load all binpb files into a table
	ctx, loadSpan := boxerTracer.Start(ctx, "duckdb.load_binpb_files")
	if err := loadBinpbFilesIntoTable(ctx, conn, binpbFiles, orgID); err != nil {
		loadSpan.RecordError(err)
		loadSpan.SetStatus(codes.Error, "failed to load binpb files")
		loadSpan.End()
		return nil, fmt.Errorf("load binpb files: %w", err)
	}
	loadSpan.End()

	// Step 2: Get distinct dateints
	ctx, dateintSpan := boxerTracer.Start(ctx, "duckdb.get_dateints")
	dateintKeys, err := getDistinctDateintKeys(ctx, conn)
	if err != nil {
		dateintSpan.RecordError(err)
		dateintSpan.SetStatus(codes.Error, "failed to get dateints")
		dateintSpan.End()
		return nil, fmt.Errorf("get dateints: %w", err)
	}
	dateintSpan.SetAttributes(attribute.Int("dateint_count", len(dateintKeys)))
	dateintSpan.End()

	if len(dateintKeys) == 0 {
		ll.Info("No data found in binpb files")
		return &MetricIngestDuckDBResult{
			DateintBins: make(map[int32]*DateintBinResult),
			TotalRows:   0,
		}, nil
	}

	ll.Info("Found dateint partitions", slog.Int("count", len(dateintKeys)))

	// Step 3: Get schema for building aggregation query
	ctx, schemaSpan := boxerTracer.Start(ctx, "duckdb.get_schema")
	schema, err := getTableSchema(ctx, conn, "metrics_raw")
	if err != nil {
		schemaSpan.RecordError(err)
		schemaSpan.SetStatus(codes.Error, "failed to get schema")
		schemaSpan.End()
		return nil, fmt.Errorf("get schema: %w", err)
	}
	schemaSpan.SetAttributes(attribute.Int("column_count", len(schema)))
	schemaSpan.End()

	// Step 4: Process each dateint partition
	result := &MetricIngestDuckDBResult{
		DateintBins:      make(map[int32]*DateintBinResult),
		FailedPartitions: make(map[int64]error),
	}

	for _, dateintKey := range dateintKeys {
		ctx, partSpan := boxerTracer.Start(ctx, "duckdb.process_partition",
			trace.WithAttributes(attribute.Int64("dateint_key", dateintKey)))

		binResult, err := processDateintPartition(ctx, conn, dateintKey, schema, tmpDir)
		if err != nil {
			partSpan.RecordError(err)
			partSpan.SetStatus(codes.Error, "failed to process partition")
			partSpan.End()
			ll.Error("Failed to process dateint partition",
				slog.Int64("dateintKey", dateintKey),
				slog.Any("error", err))
			result.FailedPartitions[dateintKey] = err
			continue // Continue with other partitions (best effort)
		}

		result.DateintBins[binResult.Dateint] = binResult
		result.TotalRows += binResult.RecordCount

		partSpan.SetAttributes(
			attribute.Int("dateint", int(binResult.Dateint)),
			attribute.Int64("record_count", binResult.RecordCount),
		)
		partSpan.End()

		ll.Debug("Processed dateint partition",
			slog.Int("dateint", int(binResult.Dateint)),
			slog.Int64("records", binResult.RecordCount))
	}

	span.SetAttributes(
		attribute.Int("dateint_bins", len(result.DateintBins)),
		attribute.Int64("total_rows", result.TotalRows),
		attribute.Int("failed_partitions", len(result.FailedPartitions)),
	)

	if len(result.FailedPartitions) > 0 {
		ll.Warn("DuckDB metric ingestion completed with failures",
			slog.Int("succeeded", len(result.DateintBins)),
			slog.Int("failed", len(result.FailedPartitions)),
			slog.Int64("totalRows", result.TotalRows))
	} else {
		ll.Info("DuckDB metric ingestion completed",
			slog.Int("partitions", len(result.DateintBins)),
			slog.Int64("totalRows", result.TotalRows))
	}

	return result, nil
}

// loadBinpbFilesIntoTable loads all binpb files into a DuckDB table.
// Files are passed as a list string to otel_metrics_read() so schemas are merged correctly.
// The extension expects format: '[file1.binpb, file2.binpb]' (string, not DuckDB array).
func loadBinpbFilesIntoTable(ctx context.Context, conn *sql.Conn, files []string, orgID string) error {
	// Build file list as string: '[file1, file2, ...]'
	// Note: files inside the list are NOT quoted - the extension parses this as a string
	fileList := "[" + strings.Join(files, ", ") + "]"

	createSQL := fmt.Sprintf("CREATE TABLE metrics_raw AS SELECT * FROM otel_metrics_read('%s', customer_id='%s')",
		escapeSingleQuote(fileList), escapeSingleQuote(orgID))

	_, err := conn.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

// getDistinctDateintKeys returns all unique dateint keys (days since epoch) from the table
func getDistinctDateintKeys(ctx context.Context, conn *sql.Conn) ([]int64, error) {
	// Use // for integer division in DuckDB (/ returns float)
	// msPerDay = 86400000 (24 hours in milliseconds)
	const query = "SELECT DISTINCT (chq_timestamp // 86400000) AS dateint_key FROM metrics_raw ORDER BY dateint_key"

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query dateints: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var keys []int64
	for rows.Next() {
		var key int64
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan dateint: %w", err)
		}
		keys = append(keys, key)
	}

	return keys, rows.Err()
}

// getTableSchema returns the column names from a table
func getTableSchema(ctx context.Context, conn *sql.Conn, tableName string) ([]string, error) {
	query := fmt.Sprintf("DESCRIBE %s", pq.QuoteIdentifier(tableName))

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("describe table: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var columns []string
	for rows.Next() {
		var name, typ, null, key, def, extra any
		if err := rows.Scan(&name, &typ, &null, &key, &def, &extra); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		if nameStr, ok := name.(string); ok {
			columns = append(columns, nameStr)
		}
	}

	return columns, rows.Err()
}

// processDateintPartition exports and computes metadata for a single dateint partition
func processDateintPartition(
	ctx context.Context,
	conn *sql.Conn,
	dateintKey int64,
	schema []string,
	tmpDir string,
) (*DateintBinResult, error) {
	// Calculate timestamp range for this partition [startMs, endMs)
	startMs := dateintKey * msPerDay
	endMs := (dateintKey + 1) * msPerDay

	// Convert dateintKey to YYYYMMDD format
	t := time.Unix(dateintKey*86400, 0).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())

	// Output file path
	outputFile := filepath.Join(tmpDir, fmt.Sprintf("metrics_%d.parquet", dateint))

	// Export partition to parquet with 10s aggregation
	if err := exportPartitionToParquet(ctx, conn, startMs, endMs, schema, outputFile); err != nil {
		return nil, fmt.Errorf("export partition: %w", err)
	}

	// Get file size
	fileInfo, err := os.Stat(outputFile)
	if err != nil {
		return nil, fmt.Errorf("stat output file: %w", err)
	}

	// Get record count from the parquet file
	recordCount, err := getParquetRecordCount(ctx, conn, outputFile)
	if err != nil {
		return nil, fmt.Errorf("get record count: %w", err)
	}

	// Extract metadata from the partition (before aggregation for accurate stats)
	metadata, err := extractPartitionMetadata(ctx, conn, startMs, endMs, schema)
	if err != nil {
		return nil, fmt.Errorf("extract metadata: %w", err)
	}
	// Compute hour from the actual first timestamp in the data
	metadata.Hour = getHourFromTimestamp(metadata.StartTs)

	return &DateintBinResult{
		Dateint:     dateint,
		OutputFile:  outputFile,
		RecordCount: recordCount,
		FileSize:    fileInfo.Size(),
		Metadata:    metadata,
	}, nil
}

// exportPartitionToParquet exports a dateint partition to a parquet file with 10s aggregation
func exportPartitionToParquet(
	ctx context.Context,
	conn *sql.Conn,
	startMs, endMs int64,
	schema []string,
	outputFile string,
) error {
	// Build aggregation query similar to writer_metrics_duckdb.go
	selectClauses, groupByClauses := buildAggregationClauses(schema, aggregationWindowMs)

	// Determine timestamp column for ordering - use chq_tsns if available, otherwise chq_timestamp
	tsCol := "chq_timestamp"
	for _, col := range schema {
		if col == "chq_tsns" {
			tsCol = "chq_tsns"
			break
		}
	}

	// Rollup extraction from ddsketch_stats_agg result
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
		FROM metrics_raw
		WHERE chq_timestamp >= %d AND chq_timestamp < %d
		GROUP BY %s`,
		strings.Join(selectClauses, ", "),
		startMs, endMs,
		strings.Join(groupByClauses, ", "))

	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT * EXCLUDE (chq_stats), %s
			FROM (%s) AS aggregated
			ORDER BY "metric_name", "chq_tid", "%s"
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		strings.Join(rollupExtraction, ", "),
		innerQuery,
		tsCol,
		escapeSingleQuote(outputFile))

	_, err := conn.ExecContext(ctx, copyQuery)
	return err
}

// buildAggregationClauses builds SELECT and GROUP BY clauses for 10s aggregation
func buildAggregationClauses(schema []string, windowMs int64) (selectClauses, groupByClauses []string) {
	groupCols := map[string]bool{"metric_name": true, "chq_tid": true}
	rollupCols := map[string]bool{
		"chq_rollup_count": true, "chq_rollup_sum": true, "chq_rollup_avg": true,
		"chq_rollup_min": true, "chq_rollup_max": true,
		"chq_rollup_p25": true, "chq_rollup_p50": true, "chq_rollup_p75": true,
		"chq_rollup_p90": true, "chq_rollup_p95": true, "chq_rollup_p99": true,
	}

	for _, col := range schema {
		qcol := pq.QuoteIdentifier(col)

		switch {
		case col == "chq_sketch":
			// Merge sketches using ddsketch_stats_agg
			selectClauses = append(selectClauses, fmt.Sprintf("ddsketch_stats_agg(%s) AS chq_stats", qcol))

		case col == "chq_timestamp":
			// Truncate timestamp to aggregation window
			selectClauses = append(selectClauses, fmt.Sprintf(
				"(%s / %d) * %d AS %s", qcol, windowMs, windowMs, qcol))
			groupByClauses = append(groupByClauses, fmt.Sprintf("(%s / %d) * %d", qcol, windowMs, windowMs))

		case col == "chq_tsns":
			// Truncate nanosecond timestamp to aggregation window (derived from chq_timestamp, use first)
			windowNs := windowMs * 1_000_000
			selectClauses = append(selectClauses, fmt.Sprintf(
				"first((%s / %d) * %d) AS %s", qcol, windowNs, windowNs, qcol))

		case groupCols[col]:
			// Group by columns pass through
			selectClauses = append(selectClauses, qcol)
			groupByClauses = append(groupByClauses, qcol)

		case rollupCols[col]:
			// Skip rollup columns - computed from merged sketch
			continue

		default:
			// Metadata columns - use first value
			selectClauses = append(selectClauses, fmt.Sprintf("first(%s) AS %s", qcol, qcol))
		}
	}

	return selectClauses, groupByClauses
}

// getParquetRecordCount returns the record count from a parquet file
func getParquetRecordCount(ctx context.Context, conn *sql.Conn, filePath string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", escapeSingleQuote(filePath))
	err := conn.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// extractPartitionMetadata extracts metadata from a dateint partition
func extractPartitionMetadata(
	ctx context.Context,
	conn *sql.Conn,
	startMs, endMs int64,
	schema []string,
) (*MetricFileMetadata, error) {
	// Get timestamp range
	var firstTs, lastTs int64
	tsQuery := fmt.Sprintf(`
		SELECT MIN(chq_timestamp), MAX(chq_timestamp)
		FROM metrics_raw
		WHERE chq_timestamp >= %d AND chq_timestamp < %d`,
		startMs, endMs)

	if err := conn.QueryRowContext(ctx, tsQuery).Scan(&firstTs, &lastTs); err != nil {
		return nil, fmt.Errorf("query timestamp range: %w", err)
	}

	// Get metric names and types
	metricNames, metricTypes, err := getMetricNamesAndTypes(ctx, conn, startMs, endMs)
	if err != nil {
		return nil, fmt.Errorf("get metric names: %w", err)
	}

	// Compute fingerprints from metric names
	fingerprints := computeFingerprints(metricNames)

	// Build label name map from schema
	labelNameMap := buildLabelNameMapFromSchema(schema)

	return &MetricFileMetadata{
		StartTs:      firstTs,
		EndTs:        lastTs + 1, // Exclusive end
		Fingerprints: fingerprints,
		MetricNames:  metricNames,
		MetricTypes:  metricTypes,
		LabelNameMap: labelNameMap,
	}, nil
}

// getMetricNamesAndTypes returns sorted metric names and their types
func getMetricNamesAndTypes(ctx context.Context, conn *sql.Conn, startMs, endMs int64) ([]string, []int16, error) {
	query := fmt.Sprintf(`
		SELECT metric_name, first(chq_metric_type) AS metric_type
		FROM metrics_raw
		WHERE chq_timestamp >= %d AND chq_timestamp < %d
		GROUP BY metric_name
		ORDER BY metric_name`,
		startMs, endMs)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("query metric names: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var names []string
	var types []int16

	for rows.Next() {
		var name, typeStr string
		if err := rows.Scan(&name, &typeStr); err != nil {
			return nil, nil, fmt.Errorf("scan metric: %w", err)
		}
		names = append(names, name)
		types = append(types, lrdb.MetricTypeFromString(typeStr))
	}

	return names, types, rows.Err()
}

// computeFingerprints generates fingerprints from metric names
func computeFingerprints(metricNames []string) []int64 {
	metricNameSet := mapset.NewSet[string]()
	for _, name := range metricNames {
		metricNameSet.Add(name)
	}

	tagValuesByName := map[string]mapset.Set[string]{
		"metric_name": metricNameSet,
	}

	fingerprintSet := fingerprint.ToFingerprints(tagValuesByName)
	fingerprints := fingerprintSet.ToSlice()
	slices.Sort(fingerprints)

	return fingerprints
}

// buildLabelNameMapFromSchema builds label name map from column names.
// This creates a JSON mapping of label columns to their dotted equivalents.
func buildLabelNameMapFromSchema(schema []string) []byte {
	mapping := make(map[string]string)
	for _, col := range schema {
		if isLabelColumn(col) {
			dottedName := underscoreToDotted(col)
			if dottedName != col {
				mapping[col] = dottedName
			} else {
				mapping[col] = ""
			}
		}
	}

	if len(mapping) == 0 {
		return nil
	}

	jsonBytes, err := json.Marshal(mapping)
	if err != nil {
		slog.Error("buildLabelNameMapFromSchema: failed to marshal mapping", slog.Any("error", err))
		return nil
	}

	return jsonBytes
}

// underscoreToDotted converts an underscored name to a dotted name where applicable.
func underscoreToDotted(underscored string) string {
	// Handle attr_ prefix: strip it first, then convert the rest
	if strings.HasPrefix(underscored, "attr_") {
		rest := underscored[len("attr_"):]
		return underscoreToDotted(rest)
	}

	// Known prefixes that should be converted to dotted format
	convertiblePrefixes := []string{"_cardinalhq_", "resource_", "log_", "metric_", "span_", "trace_"}
	for _, prefix := range convertiblePrefixes {
		if strings.HasPrefix(underscored, prefix) {
			return strings.ReplaceAll(underscored, "_", ".")
		}
	}

	// Fields with chq_ and scope_ prefixes don't have dotted equivalents
	return underscored
}

// isLabelColumn checks if a column is a label column
func isLabelColumn(columnName string) bool {
	labelPrefixes := []string{
		"_cardinalhq_",
		"chq_",
		"resource_",
		"scope_",
		"log_",
		"metric_",
		"span_",
		"trace_",
		"attr_",
	}

	for _, prefix := range labelPrefixes {
		if strings.HasPrefix(columnName, prefix) {
			return true
		}
	}
	return false
}

// getHourFromTimestamp extracts the hour component from a timestamp in milliseconds
func getHourFromTimestamp(timestampMs int64) int16 {
	return int16((timestampMs / (1000 * 60 * 60)) % 24)
}
