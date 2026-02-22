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
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/lib/pq"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
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
	AggCounts     map[factories.LogAggKey]int64
}

// processLogIngestWithDuckDB processes log binpb files using pure DuckDB SQL.
// This loads all files into a table, partitions by dateint, and exports each
// partition to a separate parquet file sorted by [service_identifier, chq_fingerprint, chq_tsns].
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
	defer func() {
		if err := db.Close(); err != nil {
			slog.Error("failed to close duckdb", slog.Any("error", err))
		}
	}()

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

// loadLogBinpbFilesIntoTable loads log binpb files into a DuckDB table using incremental schema merging.
// Files are processed one at a time to avoid loading all data into memory at once.
// Schema differences are handled by adding new columns as they're discovered.
func loadLogBinpbFilesIntoTable(ctx context.Context, conn *sql.Conn, files []string, orgID string) error {
	ll := logctx.FromContext(ctx)
	const accumTable = "logs_raw"
	accumExists := false

	for i, file := range files {
		tempTable := fmt.Sprintf("logs_temp_%d", i)

		// Load this file into a temp table
		createTempSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM otel_logs_read('[%s]', customer_id='%s')",
			tempTable, escapeSingleQuote(file), escapeSingleQuote(orgID))

		if _, err := conn.ExecContext(ctx, createTempSQL); err != nil {
			return fmt.Errorf("create temp table for file %d (%s): %w", i, filepath.Base(file), err)
		}

		if !accumExists {
			// First file: rename temp table to accumulation table
			renameSQL := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tempTable, accumTable)
			if _, err := conn.ExecContext(ctx, renameSQL); err != nil {
				return fmt.Errorf("rename first table: %w", err)
			}
			accumExists = true
		} else {
			// Add new columns, insert data, and drop temp table
			if err := mergeSchemaAndInsert(ctx, conn, tempTable, accumTable, i); err != nil {
				return err
			}
		}

		if (i+1)%10 == 0 || i == len(files)-1 {
			ll.Debug("Log ingestion progress",
				slog.Int("processed", i+1),
				slog.Int("total", len(files)))
		}
	}

	return nil
}

// mergeSchemaAndInsert adds missing columns from tempTable to accumTable, inserts data, and drops tempTable.
// Uses defer for proper Close() error handling.
func mergeSchemaAndInsert(ctx context.Context, conn *sql.Conn, tempTable, accumTable string, fileIdx int) error {
	// Get columns from temp table that don't exist in accumulation table
	newColsQuery := fmt.Sprintf(`
		SELECT t.column_name, t.data_type
		FROM information_schema.columns t
		WHERE t.table_name = '%s'
		AND t.column_name NOT IN (
			SELECT a.column_name FROM information_schema.columns a WHERE a.table_name = '%s'
		)
	`, tempTable, accumTable)

	rows, err := conn.QueryContext(ctx, newColsQuery)
	if err != nil {
		return fmt.Errorf("query new columns for file %d: %w", fileIdx, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", slog.Any("error", err))
		}
	}()

	// Add any new columns to accumulation table
	for rows.Next() {
		var colName, dataType string
		if err := rows.Scan(&colName, &dataType); err != nil {
			return fmt.Errorf("scan column for file %d: %w", fileIdx, err)
		}

		alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			accumTable, pq.QuoteIdentifier(colName), dataType)
		if _, err := conn.ExecContext(ctx, alterSQL); err != nil {
			return fmt.Errorf("add column %s for file %d: %w", colName, fileIdx, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate new columns for file %d: %w", fileIdx, err)
	}

	// Get columns from temp table for explicit INSERT
	tempCols, err := getTempTableColumns(ctx, conn, tempTable, fileIdx)
	if err != nil {
		return err
	}

	// Insert with explicit column list - DuckDB fills NULLs for missing columns
	colList := strings.Join(tempCols, ", ")
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
		accumTable, colList, colList, tempTable)
	if _, err := conn.ExecContext(ctx, insertSQL); err != nil {
		return fmt.Errorf("insert from temp table %d: %w", fileIdx, err)
	}

	// Drop temp table to free memory
	dropSQL := fmt.Sprintf("DROP TABLE %s", tempTable)
	if _, err := conn.ExecContext(ctx, dropSQL); err != nil {
		return fmt.Errorf("drop temp table %d: %w", fileIdx, err)
	}

	return nil
}

// getTempTableColumns returns the column names from the temp table, quoted for SQL.
func getTempTableColumns(ctx context.Context, conn *sql.Conn, tempTable string, fileIdx int) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT column_name FROM information_schema.columns
		WHERE table_name = '%s' ORDER BY ordinal_position
	`, tempTable)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query temp columns for file %d: %w", fileIdx, err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close column rows", slog.Any("error", err))
		}
	}()

	var cols []string
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("scan temp column for file %d: %w", fileIdx, err)
		}
		cols = append(cols, pq.QuoteIdentifier(colName))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate temp columns for file %d: %w", fileIdx, err)
	}

	return cols, nil
}

// getDistinctLogDateintKeys returns all unique dateint keys (days since epoch) from the logs table
func getDistinctLogDateintKeys(ctx context.Context, conn *sql.Conn) ([]int64, error) {
	// Use // for integer division in DuckDB (/ returns float)
	const query = "SELECT DISTINCT (chq_timestamp // 86400000) AS dateint_key FROM logs_raw ORDER BY dateint_key"

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query log dateints: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", slog.Any("error", err))
		}
	}()

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

	// Export partition to parquet (sorted by service_identifier, fingerprint, timestamp_ns)
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
	aggCounts, err := extractLogPartitionAggCounts(ctx, conn, startMs, endMs, schema, streamField)
	if err != nil {
		return nil, fmt.Errorf("extract log aggregation counts: %w", err)
	}
	metadata.AggCounts = aggCounts
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
// Logs are sorted by [service_identifier, chq_fingerprint, chq_tsns].
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
	// chq_tsns provides nanosecond precision for ordering within groups
	orderByCols = append(orderByCols, `"chq_fingerprint"`, `"chq_tsns"`)

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

func extractLogPartitionAggCounts(
	ctx context.Context,
	conn *sql.Conn,
	startMs, endMs int64,
	schema []string,
	streamField string,
) (map[factories.LogAggKey]int64, error) {
	schemaSet := mapset.NewSet[string]()
	for _, col := range schema {
		schemaSet.Add(col)
	}

	streamFieldToUse := ""
	if streamField != "" && schemaSet.Contains(streamField) {
		streamFieldToUse = streamField
	} else if schemaSet.Contains("resource_customer_domain") {
		streamFieldToUse = "resource_customer_domain"
	} else if schemaSet.Contains("resource_service_name") {
		streamFieldToUse = "resource_service_name"
	}

	var selectParts []string
	var groupByParts []string

	if !schemaSet.Contains("chq_timestamp") {
		return nil, nil
	}
	selectParts = append(selectParts, "(chq_timestamp // 10000) * 10000 AS bucket_ts")
	groupByParts = append(groupByParts, "bucket_ts")

	if schemaSet.Contains("log_level") {
		selectParts = append(selectParts, "COALESCE(log_level, '') AS log_level_out")
		groupByParts = append(groupByParts, "log_level_out")
	} else {
		selectParts = append(selectParts, "'' AS log_level_out")
	}

	if streamFieldToUse != "" {
		selectParts = append(selectParts, fmt.Sprintf("COALESCE(%s, '') AS stream_value", pq.QuoteIdentifier(streamFieldToUse)))
		groupByParts = append(groupByParts, "stream_value")
	} else {
		selectParts = append(selectParts, "'' AS stream_value")
	}

	selectParts = append(selectParts, "COUNT(*) AS cnt")

	query := fmt.Sprintf(
		"SELECT %s FROM logs_raw WHERE chq_timestamp >= %d AND chq_timestamp < %d GROUP BY %s",
		strings.Join(selectParts, ", "),
		startMs,
		endMs,
		strings.Join(groupByParts, ", "),
	)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query partition agg counts: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", slog.Any("error", err))
		}
	}()

	aggCounts := make(map[factories.LogAggKey]int64)
	for rows.Next() {
		var bucketTS, count int64
		var logLevel, streamValue string
		if err := rows.Scan(&bucketTS, &logLevel, &streamValue, &count); err != nil {
			return nil, fmt.Errorf("scan partition agg row: %w", err)
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
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", slog.Any("error", err))
		}
	}()

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
