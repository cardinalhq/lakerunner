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

// TraceIngestDuckDBResult contains the results of DuckDB-based trace ingestion
type TraceIngestDuckDBResult struct {
	DateintBins map[int32]*TraceDateintBinResult
	TotalRows   int64
	// FailedPartitions tracks dateint keys that failed processing with their errors.
	FailedPartitions map[int64]error
}

// TraceDateintBinResult contains the result for a single dateint partition
type TraceDateintBinResult struct {
	Dateint     int32
	OutputFile  string // Local parquet file path
	RecordCount int64
	FileSize    int64
	Metadata    *TraceFileMetadata
}

// TraceFileMetadata contains metadata extracted from the trace parquet file
type TraceFileMetadata struct {
	StartTs      int64   // Inclusive start timestamp in ms
	EndTs        int64   // Exclusive end timestamp in ms (lastTs + 1)
	Hour         int16   // Hour of day for the first timestamp
	Fingerprints []int64 // Span fingerprints for indexing
	LabelNameMap []byte  // JSON map of label columns
}

// processTraceIngestWithDuckDB processes trace binpb files using pure DuckDB SQL.
// This loads all files into a table, partitions by dateint, and exports each
// partition to a separate parquet file sorted by [span_trace_id, chq_timestamp].
func processTraceIngestWithDuckDB(
	ctx context.Context,
	binpbFiles []string,
	orgID string,
	tmpDir string,
) (*TraceIngestDuckDBResult, error) {
	span := trace.SpanFromContext(ctx)
	ll := logctx.FromContext(ctx)

	if len(binpbFiles) == 0 {
		return nil, fmt.Errorf("no binpb files provided")
	}

	span.SetAttributes(
		attribute.Int("duckdb.input_file_count", len(binpbFiles)),
		attribute.String("duckdb.org_id", orgID),
	)

	ll.Info("Processing traces with pure DuckDB pipeline",
		slog.Int("fileCount", len(binpbFiles)))

	// Create a temporary file-based DuckDB database for memory efficiency
	dbPath := filepath.Join(tmpDir, "traces_ingest.duckdb")
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
	ctx, loadSpan := boxerTracer.Start(ctx, "duckdb.load_trace_binpb_files")
	if err := loadTraceBinpbFilesIntoTable(ctx, conn, binpbFiles, orgID); err != nil {
		loadSpan.RecordError(err)
		loadSpan.SetStatus(codes.Error, "failed to load trace binpb files")
		loadSpan.End()
		return nil, fmt.Errorf("load trace binpb files: %w", err)
	}
	loadSpan.End()

	// Step 2: Get distinct dateints
	ctx, dateintSpan := boxerTracer.Start(ctx, "duckdb.get_trace_dateints")
	dateintKeys, err := getDistinctTraceDateintKeys(ctx, conn)
	if err != nil {
		dateintSpan.RecordError(err)
		dateintSpan.SetStatus(codes.Error, "failed to get trace dateints")
		dateintSpan.End()
		return nil, fmt.Errorf("get trace dateints: %w", err)
	}
	dateintSpan.SetAttributes(attribute.Int("dateint_count", len(dateintKeys)))
	dateintSpan.End()

	if len(dateintKeys) == 0 {
		ll.Info("No trace data found in binpb files")
		return &TraceIngestDuckDBResult{
			DateintBins: make(map[int32]*TraceDateintBinResult),
			TotalRows:   0,
		}, nil
	}

	ll.Info("Found trace dateint partitions", slog.Int("count", len(dateintKeys)))

	// Step 3: Get schema
	ctx, schemaSpan := boxerTracer.Start(ctx, "duckdb.get_trace_schema")
	schema, err := getTableSchema(ctx, conn, "traces_raw")
	if err != nil {
		schemaSpan.RecordError(err)
		schemaSpan.SetStatus(codes.Error, "failed to get trace schema")
		schemaSpan.End()
		return nil, fmt.Errorf("get trace schema: %w", err)
	}
	schemaSpan.SetAttributes(attribute.Int("column_count", len(schema)))
	schemaSpan.End()

	// Step 4: Process each dateint partition
	result := &TraceIngestDuckDBResult{
		DateintBins:      make(map[int32]*TraceDateintBinResult),
		FailedPartitions: make(map[int64]error),
	}

	for _, dateintKey := range dateintKeys {
		ctx, partSpan := boxerTracer.Start(ctx, "duckdb.process_trace_partition",
			trace.WithAttributes(attribute.Int64("dateint_key", dateintKey)))

		binResult, err := processTraceDateintPartition(ctx, conn, dateintKey, schema, tmpDir)
		if err != nil {
			partSpan.RecordError(err)
			partSpan.SetStatus(codes.Error, "failed to process trace partition")
			partSpan.End()
			ll.Error("Failed to process trace dateint partition",
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

		ll.Debug("Processed trace dateint partition",
			slog.Int("dateint", int(binResult.Dateint)),
			slog.Int64("records", binResult.RecordCount))
	}

	span.SetAttributes(
		attribute.Int("dateint_bins", len(result.DateintBins)),
		attribute.Int64("total_rows", result.TotalRows),
		attribute.Int("failed_partitions", len(result.FailedPartitions)),
	)

	if len(result.FailedPartitions) > 0 {
		ll.Warn("DuckDB trace ingestion completed with failures",
			slog.Int("succeeded", len(result.DateintBins)),
			slog.Int("failed", len(result.FailedPartitions)),
			slog.Int64("totalRows", result.TotalRows))
	} else {
		ll.Info("DuckDB trace ingestion completed",
			slog.Int("partitions", len(result.DateintBins)),
			slog.Int64("totalRows", result.TotalRows))
	}

	return result, nil
}

// loadTraceBinpbFilesIntoTable loads trace binpb files incrementally into a DuckDB table.
// Files are processed one at a time with schema merging to bound memory usage.
func loadTraceBinpbFilesIntoTable(ctx context.Context, conn *sql.Conn, files []string, orgID string) error {
	ll := logctx.FromContext(ctx)
	const accumTable = "traces_raw"
	accumExists := false

	for i, file := range files {
		tempTable := fmt.Sprintf("traces_temp_%d", i)

		// Load this file into a temp table
		createTempSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM otel_traces_read('[%s]', customer_id='%s')",
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
			if err := mergeTraceSchemaAndInsert(ctx, conn, tempTable, accumTable, i); err != nil {
				return err
			}
		}

		if (i+1)%10 == 0 || i == len(files)-1 {
			ll.Debug("Trace ingestion progress",
				slog.Int("processed", i+1),
				slog.Int("total", len(files)))
		}
	}

	return nil
}

// mergeTraceSchemaAndInsert adds missing columns from tempTable to accumTable, inserts data, and drops tempTable.
func mergeTraceSchemaAndInsert(ctx context.Context, conn *sql.Conn, tempTable, accumTable string, fileIdx int) error {
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
	tempCols, err := getTraceTempTableColumns(ctx, conn, tempTable, fileIdx)
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

// getTraceTempTableColumns returns the column names from the temp table, quoted for SQL.
func getTraceTempTableColumns(ctx context.Context, conn *sql.Conn, tempTable string, fileIdx int) ([]string, error) {
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

// getDistinctTraceDateintKeys returns all unique dateint keys (days since epoch) from the traces table
func getDistinctTraceDateintKeys(ctx context.Context, conn *sql.Conn) ([]int64, error) {
	// Use // for integer division in DuckDB (/ returns float)
	const query = "SELECT DISTINCT (chq_timestamp // 86400000) AS dateint_key FROM traces_raw ORDER BY dateint_key"

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query trace dateints: %w", err)
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
			return nil, fmt.Errorf("scan trace dateint: %w", err)
		}
		keys = append(keys, key)
	}

	return keys, rows.Err()
}

// processTraceDateintPartition exports and computes metadata for a single trace dateint partition
func processTraceDateintPartition(
	ctx context.Context,
	conn *sql.Conn,
	dateintKey int64,
	schema []string,
	tmpDir string,
) (*TraceDateintBinResult, error) {
	// Calculate timestamp range for this partition [startMs, endMs)
	startMs := dateintKey * msPerDay
	endMs := (dateintKey + 1) * msPerDay

	// Convert dateintKey to YYYYMMDD format
	t := time.Unix(dateintKey*86400, 0).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())

	// Output file path
	outputFile := filepath.Join(tmpDir, fmt.Sprintf("traces_%d.parquet", dateint))

	// Export partition to parquet (no aggregation, just sort by trace_id, timestamp)
	if err := exportTracePartitionToParquet(ctx, conn, startMs, endMs, schema, outputFile); err != nil {
		return nil, fmt.Errorf("export trace partition: %w", err)
	}

	// Get file size
	fileInfo, err := os.Stat(outputFile)
	if err != nil {
		return nil, fmt.Errorf("stat trace output file: %w", err)
	}

	// Get record count from the parquet file
	recordCount, err := getParquetRecordCount(ctx, conn, outputFile)
	if err != nil {
		return nil, fmt.Errorf("get trace record count: %w", err)
	}

	// Extract metadata from the partition
	metadata, err := extractTracePartitionMetadata(ctx, conn, startMs, endMs, schema)
	if err != nil {
		return nil, fmt.Errorf("extract trace metadata: %w", err)
	}
	// Compute hour from the actual first timestamp in the data
	metadata.Hour = getHourFromTimestamp(metadata.StartTs)

	return &TraceDateintBinResult{
		Dateint:     dateint,
		OutputFile:  outputFile,
		RecordCount: recordCount,
		FileSize:    fileInfo.Size(),
		Metadata:    metadata,
	}, nil
}

// exportTracePartitionToParquet exports a trace dateint partition to a parquet file.
// Traces are not aggregated like metrics - just sorted by [span_trace_id, chq_timestamp].
func exportTracePartitionToParquet(
	ctx context.Context,
	conn *sql.Conn,
	startMs, endMs int64,
	schema []string,
	outputFile string,
) error {
	// Build column list (quoting all column names)
	var quotedCols []string
	for _, col := range schema {
		quotedCols = append(quotedCols, pq.QuoteIdentifier(col))
	}

	copyQuery := fmt.Sprintf(`
		COPY (
			SELECT %s
			FROM traces_raw
			WHERE chq_timestamp >= %d AND chq_timestamp < %d
			ORDER BY "span_trace_id", "chq_timestamp"
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		strings.Join(quotedCols, ", "),
		startMs, endMs,
		escapeSingleQuote(outputFile))

	_, err := conn.ExecContext(ctx, copyQuery)
	return err
}

// extractTracePartitionMetadata extracts metadata from a trace dateint partition
func extractTracePartitionMetadata(
	ctx context.Context,
	conn *sql.Conn,
	startMs, endMs int64,
	schema []string,
) (*TraceFileMetadata, error) {
	// Get timestamp range
	var firstTs, lastTs int64
	tsQuery := fmt.Sprintf(`
		SELECT MIN(chq_timestamp), MAX(chq_timestamp)
		FROM traces_raw
		WHERE chq_timestamp >= %d AND chq_timestamp < %d`,
		startMs, endMs)

	if err := conn.QueryRowContext(ctx, tsQuery).Scan(&firstTs, &lastTs); err != nil {
		return nil, fmt.Errorf("query trace timestamp range: %w", err)
	}

	// Get unique fingerprints computed from indexed dimensions
	fingerprints, err := getTraceFingerprints(ctx, conn, startMs, endMs, schema)
	if err != nil {
		return nil, fmt.Errorf("get trace fingerprints: %w", err)
	}

	// Build label name map from schema
	labelNameMap := buildLabelNameMapFromSchema(schema)

	return &TraceFileMetadata{
		StartTs:      firstTs,
		EndTs:        lastTs + 1, // Exclusive end
		Fingerprints: fingerprints,
		LabelNameMap: labelNameMap,
	}, nil
}

// getTraceFingerprints computes fingerprints from indexed dimensions in the trace partition.
// This matches the fingerprinting logic used by the old pipeline.
func getTraceFingerprints(ctx context.Context, conn *sql.Conn, startMs, endMs int64, schema []string) ([]int64, error) {
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
			FROM traces_raw
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
