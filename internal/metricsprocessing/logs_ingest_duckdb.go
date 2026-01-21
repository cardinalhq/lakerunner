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
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/lib/pq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/logctx"
)

// LogIngestDuckDBResult contains the results of DuckDB-based log ingestion
type LogIngestDuckDBResult struct {
	DateintBins map[int32]*LogDateintBinResult
	TotalRows   int64
	// FailedPartitions tracks dateint keys that failed processing with their errors.
	FailedPartitions map[int64]error
}

// LogDateintBinResult contains the result for a single dateint partition
type LogDateintBinResult struct {
	Dateint     int32
	OutputFile  string // Local parquet file path
	RecordCount int64
	FileSize    int64
	Metadata    *LogFileMetadata
}

// LogFileMetadata contains metadata extracted from the log parquet file
type LogFileMetadata struct {
	StartTs       int64    // Inclusive start timestamp in ms
	EndTs         int64    // Exclusive end timestamp in ms (lastTs + 1)
	Hour          int16    // Hour of day for the first timestamp
	Fingerprints  []int64  // Unique log fingerprints for semantic grouping
	StreamIds     []string // Unique stream identifier values
	StreamIdField string   // Field used for stream identification
	LabelNameMap  []byte   // JSON map of label columns
}

// processLogIngestWithDuckDB processes log binpb files using pure DuckDB SQL.
// This loads all files into a table, partitions by dateint, and exports each
// partition to a separate parquet file sorted by [service_identifier, chq_fingerprint, chq_timestamp].
func processLogIngestWithDuckDB(
	ctx context.Context,
	binpbFiles []string,
	orgID string,
	tmpDir string,
	streamField string,
) (*LogIngestDuckDBResult, error) {
	span := trace.SpanFromContext(ctx)
	ll := logctx.FromContext(ctx)

	if len(binpbFiles) == 0 {
		return nil, fmt.Errorf("no binpb files provided")
	}

	span.SetAttributes(
		attribute.Int("duckdb.input_file_count", len(binpbFiles)),
		attribute.String("duckdb.org_id", orgID),
		attribute.String("duckdb.stream_field", streamField),
	)

	ll.Info("Processing logs with pure DuckDB pipeline",
		slog.Int("fileCount", len(binpbFiles)),
		slog.String("streamField", streamField))

	// Create a temporary file-based DuckDB database for memory efficiency
	dbPath := filepath.Join(tmpDir, "logs_ingest.duckdb")
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

	// Step 1: Load all binpb files into a table
	ctx, loadSpan := boxerTracer.Start(ctx, "duckdb.load_log_binpb_files")
	if err := loadLogBinpbFilesIntoTable(ctx, conn, binpbFiles, orgID); err != nil {
		loadSpan.RecordError(err)
		loadSpan.SetStatus(codes.Error, "failed to load log binpb files")
		loadSpan.End()
		return nil, fmt.Errorf("load log binpb files: %w", err)
	}
	loadSpan.End()

	// Step 2: Get distinct dateints
	ctx, dateintSpan := boxerTracer.Start(ctx, "duckdb.get_log_dateints")
	dateintKeys, err := getDistinctLogDateintKeys(ctx, conn)
	if err != nil {
		dateintSpan.RecordError(err)
		dateintSpan.SetStatus(codes.Error, "failed to get log dateints")
		dateintSpan.End()
		return nil, fmt.Errorf("get log dateints: %w", err)
	}
	dateintSpan.SetAttributes(attribute.Int("dateint_count", len(dateintKeys)))
	dateintSpan.End()

	if len(dateintKeys) == 0 {
		ll.Info("No log data found in binpb files")
		return &LogIngestDuckDBResult{
			DateintBins: make(map[int32]*LogDateintBinResult),
			TotalRows:   0,
		}, nil
	}

	ll.Info("Found log dateint partitions", slog.Int("count", len(dateintKeys)))

	// Step 3: Get schema
	ctx, schemaSpan := boxerTracer.Start(ctx, "duckdb.get_log_schema")
	schema, err := getTableSchema(ctx, conn, "logs_raw")
	if err != nil {
		schemaSpan.RecordError(err)
		schemaSpan.SetStatus(codes.Error, "failed to get log schema")
		schemaSpan.End()
		return nil, fmt.Errorf("get log schema: %w", err)
	}
	schemaSpan.SetAttributes(attribute.Int("column_count", len(schema)))
	schemaSpan.End()

	// Step 4: Process each dateint partition
	result := &LogIngestDuckDBResult{
		DateintBins:      make(map[int32]*LogDateintBinResult),
		FailedPartitions: make(map[int64]error),
	}

	for _, dateintKey := range dateintKeys {
		ctx, partSpan := boxerTracer.Start(ctx, "duckdb.process_log_partition",
			trace.WithAttributes(attribute.Int64("dateint_key", dateintKey)))

		binResult, err := processLogDateintPartition(ctx, conn, dateintKey, schema, tmpDir, streamField)
		if err != nil {
			partSpan.RecordError(err)
			partSpan.SetStatus(codes.Error, "failed to process log partition")
			partSpan.End()
			ll.Error("Failed to process log dateint partition",
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

		ll.Debug("Processed log dateint partition",
			slog.Int("dateint", int(binResult.Dateint)),
			slog.Int64("records", binResult.RecordCount))
	}

	span.SetAttributes(
		attribute.Int("dateint_bins", len(result.DateintBins)),
		attribute.Int64("total_rows", result.TotalRows),
		attribute.Int("failed_partitions", len(result.FailedPartitions)),
	)

	if len(result.FailedPartitions) > 0 {
		ll.Warn("DuckDB log ingestion completed with failures",
			slog.Int("succeeded", len(result.DateintBins)),
			slog.Int("failed", len(result.FailedPartitions)),
			slog.Int64("totalRows", result.TotalRows))
	} else {
		ll.Info("DuckDB log ingestion completed",
			slog.Int("partitions", len(result.DateintBins)),
			slog.Int64("totalRows", result.TotalRows))
	}

	return result, nil
}

// loadLogBinpbFilesIntoTable loads all log binpb files into a DuckDB table.
func loadLogBinpbFilesIntoTable(ctx context.Context, conn *sql.Conn, files []string, orgID string) error {
	// Build file list as string: '[file1, file2, ...]'
	fileList := "[" + strings.Join(files, ", ") + "]"

	createSQL := fmt.Sprintf("CREATE TABLE logs_raw AS SELECT * FROM otel_logs_read('%s', customer_id='%s')",
		escapeSingleQuote(fileList), escapeSingleQuote(orgID))

	_, err := conn.ExecContext(ctx, createSQL)
	if err != nil {
		return fmt.Errorf("create logs table: %w", err)
	}

	return nil
}

// getDistinctLogDateintKeys returns all unique dateint keys (days since epoch) from the logs table
func getDistinctLogDateintKeys(ctx context.Context, conn *sql.Conn) ([]int64, error) {
	// Use // for integer division in DuckDB (/ returns float)
	const query = "SELECT DISTINCT (chq_timestamp // 86400000) AS dateint_key FROM logs_raw ORDER BY dateint_key"

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query log dateints: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var keys []int64
	for rows.Next() {
		var key int64
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("scan log dateint: %w", err)
		}
		keys = append(keys, key)
	}

	return keys, rows.Err()
}

// processLogDateintPartition exports and computes metadata for a single log dateint partition
func processLogDateintPartition(
	ctx context.Context,
	conn *sql.Conn,
	dateintKey int64,
	schema []string,
	tmpDir string,
	streamField string,
) (*LogDateintBinResult, error) {
	// Calculate timestamp range for this partition [startMs, endMs)
	startMs := dateintKey * msPerDay
	endMs := (dateintKey + 1) * msPerDay

	// Convert dateintKey to YYYYMMDD format
	t := time.Unix(dateintKey*86400, 0).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())

	// Output file path
	outputFile := filepath.Join(tmpDir, fmt.Sprintf("logs_%d.parquet", dateint))

	// Determine service identifier field for sorting
	serviceIdField := getLogServiceIdentifierField(schema, streamField)

	// Export partition to parquet (sorted by service_identifier, fingerprint, timestamp)
	if err := exportLogPartitionToParquet(ctx, conn, startMs, endMs, schema, outputFile, serviceIdField); err != nil {
		return nil, fmt.Errorf("export log partition: %w", err)
	}

	// Get file size
	fileInfo, err := os.Stat(outputFile)
	if err != nil {
		return nil, fmt.Errorf("stat log output file: %w", err)
	}

	// Get record count from the parquet file
	recordCount, err := getParquetRecordCount(ctx, conn, outputFile)
	if err != nil {
		return nil, fmt.Errorf("get log record count: %w", err)
	}

	// Extract metadata from the partition
	metadata, err := extractLogPartitionMetadata(ctx, conn, startMs, endMs, schema, serviceIdField, streamField)
	if err != nil {
		return nil, fmt.Errorf("extract log metadata: %w", err)
	}
	// Compute hour from the actual first timestamp in the data
	metadata.Hour = getHourFromTimestamp(metadata.StartTs)

	return &LogDateintBinResult{
		Dateint:     dateint,
		OutputFile:  outputFile,
		RecordCount: recordCount,
		FileSize:    fileInfo.Size(),
		Metadata:    metadata,
	}, nil
}

// getLogServiceIdentifierField determines which field to use for the service identifier.
// Priority: streamField (if configured and exists) > resource_customer_domain > resource_service_name
func getLogServiceIdentifierField(schema []string, streamField string) string {
	// If a specific stream field is configured, check if it exists
	if streamField != "" && slices.Contains(schema, streamField) {
		return streamField
	}

	// Default fallback: resource_customer_domain > resource_service_name
	if slices.Contains(schema, "resource_customer_domain") {
		return "resource_customer_domain"
	}
	if slices.Contains(schema, "resource_service_name") {
		return "resource_service_name"
	}

	// No service identifier field found
	return ""
}

// exportLogPartitionToParquet exports a log dateint partition to a parquet file.
// Logs are sorted by [service_identifier, chq_fingerprint, chq_timestamp].
func exportLogPartitionToParquet(
	ctx context.Context,
	conn *sql.Conn,
	startMs, endMs int64,
	schema []string,
	outputFile string,
	serviceIdField string,
) error {
	// Build column list (quoting all column names)
	var quotedCols []string
	for _, col := range schema {
		quotedCols = append(quotedCols, pq.QuoteIdentifier(col))
	}

	// Build ORDER BY clause
	var orderByCols []string
	if serviceIdField != "" {
		orderByCols = append(orderByCols, pq.QuoteIdentifier(serviceIdField))
	}
	// chq_fingerprint is provided by the extension for log semantic grouping
	orderByCols = append(orderByCols, `"chq_fingerprint"`, `"chq_timestamp"`)

	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT %s
			FROM logs_raw
			WHERE chq_timestamp >= %d AND chq_timestamp < %d
			ORDER BY %s
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		strings.Join(quotedCols, ", "),
		startMs, endMs,
		strings.Join(orderByCols, ", "),
		escapeSingleQuote(outputFile))

	_, err := conn.ExecContext(ctx, copyQuery)
	return err
}

// extractLogPartitionMetadata extracts metadata from a log dateint partition
func extractLogPartitionMetadata(
	ctx context.Context,
	conn *sql.Conn,
	startMs, endMs int64,
	schema []string,
	serviceIdField string,
	streamField string,
) (*LogFileMetadata, error) {
	// Get timestamp range
	var firstTs, lastTs int64
	tsQuery := fmt.Sprintf(`
		SELECT MIN(chq_timestamp), MAX(chq_timestamp)
		FROM logs_raw
		WHERE chq_timestamp >= %d AND chq_timestamp < %d`,
		startMs, endMs)

	if err := conn.QueryRowContext(ctx, tsQuery).Scan(&firstTs, &lastTs); err != nil {
		return nil, fmt.Errorf("query log timestamp range: %w", err)
	}

	// Get query fingerprints computed from indexed dimensions
	fingerprints, err := getLogFingerprints(ctx, conn, startMs, endMs, schema)
	if err != nil {
		return nil, fmt.Errorf("get log fingerprints: %w", err)
	}

	// Get unique stream identifiers
	streamIds, err := getLogStreamIds(ctx, conn, startMs, endMs, serviceIdField)
	if err != nil {
		return nil, fmt.Errorf("get log stream ids: %w", err)
	}

	// Determine the actual stream ID field used
	streamIdFieldUsed := serviceIdField
	if streamIdFieldUsed == "" {
		// Use the configured streamField name even if the column doesn't exist
		// This maintains consistency with what was expected
		streamIdFieldUsed = streamField
	}

	// Build label name map from schema
	labelNameMap := buildLabelNameMapFromSchema(schema)

	return &LogFileMetadata{
		StartTs:       firstTs,
		EndTs:         lastTs + 1, // Exclusive end
		Fingerprints:  fingerprints,
		StreamIds:     streamIds,
		StreamIdField: streamIdFieldUsed,
		LabelNameMap:  labelNameMap,
	}, nil
}

// getLogFingerprints computes query fingerprints from indexed dimensions in the log partition.
// This matches the fingerprinting logic used by query-api to find segments.
func getLogFingerprints(ctx context.Context, conn *sql.Conn, startMs, endMs int64, schema []string) ([]int64, error) {
	// Build a map of field values for indexed dimensions
	tagValuesByName := make(map[string]mapset.Set[string])

	// Get indexed columns that exist in schema
	indexedCols := []string{}
	for col := range fingerprint.IndexedDimensions {
		for _, schemaCol := range schema {
			if schemaCol == col {
				indexedCols = append(indexedCols, col)
				break
			}
		}
	}

	// Also add "exists" fingerprints for all columns (but only index values for indexed dims)
	for _, col := range schema {
		tagValuesByName[col] = mapset.NewSet[string]()
	}

	// Query distinct values for each indexed column
	for _, col := range indexedCols {
		query := fmt.Sprintf(`
			SELECT DISTINCT %s
			FROM logs_raw
			WHERE chq_timestamp >= %d AND chq_timestamp < %d AND %s IS NOT NULL`,
			pq.QuoteIdentifier(col), startMs, endMs, pq.QuoteIdentifier(col))

		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			// Column might not exist or query error - skip silently
			continue
		}

		values := mapset.NewSet[string]()
		for rows.Next() {
			var val string
			if err := rows.Scan(&val); err != nil {
				_ = rows.Close()
				continue
			}
			if val != "" {
				values.Add(val)
			}
		}
		_ = rows.Close()

		if values.Cardinality() > 0 {
			tagValuesByName[col] = values
		}
	}

	// Compute fingerprints using the fingerprint package
	fpSet := fingerprint.ToFingerprints(tagValuesByName)
	fps := fpSet.ToSlice()
	slices.Sort(fps)

	return fps, nil
}

// getLogStreamIds extracts unique stream identifier values.
func getLogStreamIds(ctx context.Context, conn *sql.Conn, startMs, endMs int64, serviceIdField string) ([]string, error) {
	if serviceIdField == "" {
		return nil, nil
	}

	query := fmt.Sprintf(`
		SELECT DISTINCT %s
		FROM logs_raw
		WHERE chq_timestamp >= %d AND chq_timestamp < %d AND %s IS NOT NULL AND %s != ''`,
		pq.QuoteIdentifier(serviceIdField),
		startMs, endMs,
		pq.QuoteIdentifier(serviceIdField),
		pq.QuoteIdentifier(serviceIdField))

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query log stream ids: %w", err)
	}
	defer func() { _ = rows.Close() }()

	ids := mapset.NewSet[string]()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			continue
		}
		if id != "" {
			ids.Add(id)
		}
	}

	result := ids.ToSlice()
	slices.Sort(result)
	return result, rows.Err()
}
