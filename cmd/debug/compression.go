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

package debug

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
)

func GetCompressionTestCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "compression-test [DIR]",
		Short: "Test compression ratios and memory usage for different ingestion strategies",
		Long: `Reads binpb.gz files from the specified directory and tests different
ingestion strategies with DuckDB, reporting memory usage and performance.

Strategies tested:
1. Sequential: Process files one at a time, tracking memory release
2. Batch: Process all files at once
3. Two-phase: Convert each file to parquet, then merge
`,
		Args: cobra.ExactArgs(1),
		RunE: runCompressionTest,
	}

	cmd.Flags().String("org-id", "test-org", "Organization ID to use for parsing")
	cmd.Flags().Int("limit", 0, "Limit number of files per signal (0 = all)")
	cmd.Flags().Int64("memory-limit-mb", 3072, "DuckDB memory limit in MB")
	cmd.Flags().Int("threads", 1, "Number of DuckDB threads")
	cmd.Flags().String("signal", "", "Only test specific signal (logs, metrics, traces)")
	cmd.Flags().String("strategy", "all", "Strategy to test: sequential, batch, two-phase, or all")

	return cmd
}

type strategyResult struct {
	Strategy        string
	Signal          string
	FileCount       int
	InputSize       int64
	OutputSize      int64
	RowCount        int64
	Duration        time.Duration
	PeakMemoryBytes int64
}

func runCompressionTest(c *cobra.Command, args []string) error {
	ctx := context.Background()
	inputDir := args[0]
	orgID, _ := c.Flags().GetString("org-id")
	limit, _ := c.Flags().GetInt("limit")
	memoryLimitMB, _ := c.Flags().GetInt64("memory-limit-mb")
	threads, _ := c.Flags().GetInt("threads")
	signalFilter, _ := c.Flags().GetString("signal")
	strategy, _ := c.Flags().GetString("strategy")

	// Find all binpb.gz files grouped by signal
	filesBySignal := make(map[string][]string)
	err := filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".binpb.gz") {
			return nil
		}

		filename := filepath.Base(path)
		var signal string
		for _, s := range []string{"logs", "metrics", "traces"} {
			if strings.HasPrefix(filename, s+"_") {
				signal = s
				break
			}
		}
		if signal == "" {
			return nil
		}

		if signalFilter != "" && signal != signalFilter {
			return nil
		}

		if limit > 0 && len(filesBySignal[signal]) >= limit {
			return nil
		}

		filesBySignal[signal] = append(filesBySignal[signal], path)
		return nil
	})
	if err != nil {
		return fmt.Errorf("walking directory: %w", err)
	}

	// Create temp directory for output
	tmpDir, err := os.MkdirTemp("", "compression-test-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	slog.Info("Configuration",
		slog.Int64("memory_limit_mb", memoryLimitMB),
		slog.Int("threads", threads),
		slog.String("strategy", strategy))

	slog.Info("Found files by signal",
		slog.Int("logs", len(filesBySignal["logs"])),
		slog.Int("metrics", len(filesBySignal["metrics"])),
		slog.Int("traces", len(filesBySignal["traces"])))

	settings := &duckdbx.DuckDBSettings{
		MemoryLimitMB: memoryLimitMB,
		Threads:       threads,
	}

	allResults := make([]strategyResult, 0)

	for _, signal := range []string{"logs", "metrics", "traces"} {
		files := filesBySignal[signal]
		if len(files) == 0 {
			continue
		}

		// Calculate total input size
		var inputSize int64
		for _, f := range files {
			info, _ := os.Stat(f)
			inputSize += info.Size()
		}

		slog.Info("Testing signal", slog.String("signal", signal), slog.Int("files", len(files)), slog.String("input_size", formatBytes(inputSize)))

		if strategy == "all" || strategy == "sequential" {
			result, err := testSequential(ctx, signal, files, orgID, tmpDir, settings)
			if err != nil {
				slog.Error("Sequential test failed", slog.String("signal", signal), slog.Any("error", err))
			} else {
				result.InputSize = inputSize
				allResults = append(allResults, *result)
			}
		}

		if strategy == "all" || strategy == "batch" {
			result, err := testBatch(ctx, signal, files, orgID, tmpDir, settings)
			if err != nil {
				slog.Error("Batch test failed", slog.String("signal", signal), slog.Any("error", err))
			} else {
				result.InputSize = inputSize
				allResults = append(allResults, *result)
			}
		}

		if strategy == "all" || strategy == "two-phase" {
			result, err := testTwoPhase(ctx, signal, files, orgID, tmpDir, settings)
			if err != nil {
				slog.Error("Two-phase test failed", slog.String("signal", signal), slog.Any("error", err))
			} else {
				result.InputSize = inputSize
				allResults = append(allResults, *result)
			}
		}

		if strategy == "all" || strategy == "incremental" {
			result, err := testIncremental(ctx, signal, files, orgID, tmpDir, settings)
			if err != nil {
				slog.Error("Incremental test failed", slog.String("signal", signal), slog.Any("error", err))
			} else {
				result.InputSize = inputSize
				allResults = append(allResults, *result)
			}
		}
	}

	// Print results
	fmt.Println()
	fmt.Println("=== Ingestion Strategy Results ===")
	fmt.Println()
	fmt.Printf("%-12s %-10s %6s %12s %12s %10s %10s %12s\n",
		"Strategy", "Signal", "Files", "Input", "Output", "Rows", "Duration", "Peak Memory")
	fmt.Printf("%-12s %-10s %6s %12s %12s %10s %10s %12s\n",
		"--------", "------", "-----", "-----", "------", "----", "--------", "-----------")

	for _, r := range allResults {
		fmt.Printf("%-12s %-10s %6d %12s %12s %10d %10s %12s\n",
			r.Strategy,
			r.Signal,
			r.FileCount,
			formatBytes(r.InputSize),
			formatBytes(r.OutputSize),
			r.RowCount,
			r.Duration.Round(time.Millisecond),
			formatBytes(r.PeakMemoryBytes))
	}

	return nil
}

// testSequential processes files one at a time, creating separate parquet files
func testSequential(ctx context.Context, signal string, files []string, orgID, tmpDir string, settings *duckdbx.DuckDBSettings) (*strategyResult, error) {
	result := &strategyResult{
		Strategy:  "sequential",
		Signal:    signal,
		FileCount: len(files),
	}

	start := time.Now()
	var maxMemory int64

	for i, file := range files {
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("%s_seq_%d.duckdb", signal, i))
		db, err := duckdbx.NewDB(
			duckdbx.WithDatabasePath(dbPath),
			duckdbx.WithDuckDBSettings(*settings),
		)
		if err != nil {
			return nil, fmt.Errorf("create duckdb: %w", err)
		}

		conn, release, err := db.GetConnection(ctx)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("get connection: %w", err)
		}

		if err := duckdbx.LoadOtelBinpbExtension(ctx, conn); err != nil {
			release()
			_ = db.Close()
			return nil, fmt.Errorf("load extension: %w", err)
		}

		readFunc := getReadFunc(signal)
		tableName := signal + "_raw"

		createSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s('[%s]', customer_id='%s')",
			tableName, readFunc, escapeSingleQuote(file), escapeSingleQuote(orgID))

		if _, err := conn.ExecContext(ctx, createSQL); err != nil {
			release()
			_ = db.Close()
			return nil, fmt.Errorf("create table for file %d: %w", i, err)
		}

		var rowCount int64
		_ = conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&rowCount)
		result.RowCount += rowCount

		mem := getMemoryUsage(ctx, conn)
		if mem > maxMemory {
			maxMemory = mem
		}

		parquetPath := filepath.Join(tmpDir, fmt.Sprintf("%s_seq_%d.parquet", signal, i))
		exportSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", tableName, parquetPath)
		if _, err := conn.ExecContext(ctx, exportSQL); err != nil {
			release()
			_ = db.Close()
			return nil, fmt.Errorf("export parquet for file %d: %w", i, err)
		}

		parquetInfo, _ := os.Stat(parquetPath)
		result.OutputSize += parquetInfo.Size()

		release()
		_ = db.Close()

		// Check memory after closing
		if (i+1)%10 == 0 {
			slog.Info("Sequential progress",
				slog.String("signal", signal),
				slog.Int("processed", i+1),
				slog.Int("total", len(files)),
				slog.String("peak_memory", formatBytes(maxMemory)))
		}
	}

	result.Duration = time.Since(start)
	result.PeakMemoryBytes = maxMemory

	return result, nil
}

// testBatch processes all files at once
func testBatch(ctx context.Context, signal string, files []string, orgID, tmpDir string, settings *duckdbx.DuckDBSettings) (*strategyResult, error) {
	result := &strategyResult{
		Strategy:  "batch",
		Signal:    signal,
		FileCount: len(files),
	}

	start := time.Now()

	dbPath := filepath.Join(tmpDir, fmt.Sprintf("%s_batch.duckdb", signal))
	db, err := duckdbx.NewDB(
		duckdbx.WithDatabasePath(dbPath),
		duckdbx.WithDuckDBSettings(*settings),
	)
	if err != nil {
		return nil, fmt.Errorf("create duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}
	defer release()

	if err := duckdbx.LoadOtelBinpbExtension(ctx, conn); err != nil {
		return nil, fmt.Errorf("load extension: %w", err)
	}

	readFunc := getReadFunc(signal)
	tableName := signal + "_raw"
	fileList := "[" + strings.Join(files, ", ") + "]"

	createSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s('%s', customer_id='%s')",
		tableName, readFunc, escapeSingleQuote(fileList), escapeSingleQuote(orgID))

	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	_ = conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&result.RowCount)
	result.PeakMemoryBytes = getMemoryUsage(ctx, conn)

	parquetPath := filepath.Join(tmpDir, fmt.Sprintf("%s_batch.parquet", signal))
	exportSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", tableName, parquetPath)
	if _, err := conn.ExecContext(ctx, exportSQL); err != nil {
		return nil, fmt.Errorf("export parquet: %w", err)
	}

	parquetInfo, _ := os.Stat(parquetPath)
	result.OutputSize = parquetInfo.Size()
	result.Duration = time.Since(start)

	return result, nil
}

// testTwoPhase first converts each file to parquet, then merges them
func testTwoPhase(ctx context.Context, signal string, files []string, orgID, tmpDir string, settings *duckdbx.DuckDBSettings) (*strategyResult, error) {
	result := &strategyResult{
		Strategy:  "two-phase",
		Signal:    signal,
		FileCount: len(files),
	}

	start := time.Now()
	var maxMemory int64
	parquetFiles := make([]string, 0, len(files))

	// Phase 1: Convert each binpb.gz to parquet
	for i, file := range files {
		dbPath := filepath.Join(tmpDir, fmt.Sprintf("%s_2p_%d.duckdb", signal, i))
		db, err := duckdbx.NewDB(
			duckdbx.WithDatabasePath(dbPath),
			duckdbx.WithDuckDBSettings(*settings),
		)
		if err != nil {
			return nil, fmt.Errorf("create duckdb: %w", err)
		}

		conn, release, err := db.GetConnection(ctx)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("get connection: %w", err)
		}

		if err := duckdbx.LoadOtelBinpbExtension(ctx, conn); err != nil {
			release()
			_ = db.Close()
			return nil, fmt.Errorf("load extension: %w", err)
		}

		readFunc := getReadFunc(signal)

		parquetPath := filepath.Join(tmpDir, fmt.Sprintf("%s_2p_%d.parquet", signal, i))
		copySQL := fmt.Sprintf("COPY (SELECT * FROM %s('[%s]', customer_id='%s')) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
			readFunc, escapeSingleQuote(file), escapeSingleQuote(orgID), parquetPath)

		if _, err := conn.ExecContext(ctx, copySQL); err != nil {
			release()
			_ = db.Close()
			return nil, fmt.Errorf("convert file %d: %w", i, err)
		}

		mem := getMemoryUsage(ctx, conn)
		if mem > maxMemory {
			maxMemory = mem
		}

		parquetFiles = append(parquetFiles, parquetPath)

		release()
		_ = db.Close()
	}

	slog.Info("Phase 1 complete",
		slog.String("signal", signal),
		slog.Int("parquet_files", len(parquetFiles)),
		slog.String("peak_memory_phase1", formatBytes(maxMemory)))

	// Phase 2: Merge all parquet files
	dbPath := filepath.Join(tmpDir, fmt.Sprintf("%s_2p_merge.duckdb", signal))
	db, err := duckdbx.NewDB(
		duckdbx.WithDatabasePath(dbPath),
		duckdbx.WithDuckDBSettings(*settings),
	)
	if err != nil {
		return nil, fmt.Errorf("create merge duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get merge connection: %w", err)
	}
	defer release()

	// Read all parquet files and merge
	parquetGlob := filepath.Join(tmpDir, fmt.Sprintf("%s_2p_*.parquet", signal))
	tableName := signal + "_merged"
	createSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')", tableName, parquetGlob)

	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("merge parquet files: %w", err)
	}

	_ = conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", tableName)).Scan(&result.RowCount)

	mem := getMemoryUsage(ctx, conn)
	if mem > maxMemory {
		maxMemory = mem
	}

	// Export final merged parquet
	finalPath := filepath.Join(tmpDir, fmt.Sprintf("%s_2p_final.parquet", signal))
	exportSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", tableName, finalPath)
	if _, err := conn.ExecContext(ctx, exportSQL); err != nil {
		return nil, fmt.Errorf("export final parquet: %w", err)
	}

	finalInfo, _ := os.Stat(finalPath)
	result.OutputSize = finalInfo.Size()
	result.Duration = time.Since(start)
	result.PeakMemoryBytes = maxMemory

	return result, nil
}

// testIncremental uses a single on-disk database with incremental schema merging.
// For each file: load into temp table, merge schema into accumulation table, insert rows, drop temp.
// This avoids loading all data into memory at once while properly merging schemas.
func testIncremental(ctx context.Context, signal string, files []string, orgID, tmpDir string, settings *duckdbx.DuckDBSettings) (*strategyResult, error) {
	result := &strategyResult{
		Strategy:  "incremental",
		Signal:    signal,
		FileCount: len(files),
	}

	start := time.Now()
	var maxMemory int64

	// Create a single on-disk database for all processing
	dbPath := filepath.Join(tmpDir, fmt.Sprintf("%s_incremental.duckdb", signal))
	db, err := duckdbx.NewDB(
		duckdbx.WithDatabasePath(dbPath),
		duckdbx.WithDuckDBSettings(*settings),
	)
	if err != nil {
		return nil, fmt.Errorf("create duckdb: %w", err)
	}
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}
	defer release()

	if err := duckdbx.LoadOtelBinpbExtension(ctx, conn); err != nil {
		return nil, fmt.Errorf("load extension: %w", err)
	}

	readFunc := getReadFunc(signal)
	accumTable := signal + "_accumulated"
	accumExists := false

	for i, file := range files {
		tempTable := fmt.Sprintf("temp_%d", i)

		// Load this file into a temp table
		createTempSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s('[%s]', customer_id='%s')",
			tempTable, readFunc, escapeSingleQuote(file), escapeSingleQuote(orgID))

		if _, err := conn.ExecContext(ctx, createTempSQL); err != nil {
			return nil, fmt.Errorf("create temp table for file %d: %w", i, err)
		}

		if !accumExists {
			// First file: rename temp table to accumulation table
			renameSQL := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", tempTable, accumTable)
			if _, err := conn.ExecContext(ctx, renameSQL); err != nil {
				return nil, fmt.Errorf("rename first table: %w", err)
			}
			accumExists = true
		} else {
			// Merge schema and insert data from temp table
			if err := mergeSchemaAndInsertDebug(ctx, conn, tempTable, accumTable, i); err != nil {
				return nil, err
			}
		}

		// Track memory
		mem := getMemoryUsage(ctx, conn)
		if mem > maxMemory {
			maxMemory = mem
		}

		if (i+1)%10 == 0 {
			slog.Info("Incremental progress",
				slog.String("signal", signal),
				slog.Int("processed", i+1),
				slog.Int("total", len(files)),
				slog.String("peak_memory", formatBytes(maxMemory)))
		}
	}

	// Get final row count
	_ = conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", accumTable)).Scan(&result.RowCount)

	// Final memory check
	mem := getMemoryUsage(ctx, conn)
	if mem > maxMemory {
		maxMemory = mem
	}

	// Export to parquet
	parquetPath := filepath.Join(tmpDir, fmt.Sprintf("%s_incremental.parquet", signal))
	exportSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", accumTable, parquetPath)
	if _, err := conn.ExecContext(ctx, exportSQL); err != nil {
		return nil, fmt.Errorf("export parquet: %w", err)
	}

	parquetInfo, _ := os.Stat(parquetPath)
	result.OutputSize = parquetInfo.Size()
	result.Duration = time.Since(start)
	result.PeakMemoryBytes = maxMemory

	slog.Info("Incremental complete",
		slog.String("signal", signal),
		slog.Int64("rows", result.RowCount),
		slog.String("output_size", formatBytes(result.OutputSize)),
		slog.String("duration", result.Duration.String()))

	return result, nil
}

// mergeSchemaAndInsertDebug adds missing columns from tempTable to accumTable, inserts data, and drops tempTable.
func mergeSchemaAndInsertDebug(ctx context.Context, conn *sql.Conn, tempTable, accumTable string, fileIdx int) error {
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
		return fmt.Errorf("query new columns: %w", err)
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
			return fmt.Errorf("scan column: %w", err)
		}

		alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"%s\" %s", accumTable, colName, dataType)
		if _, err := conn.ExecContext(ctx, alterSQL); err != nil {
			return fmt.Errorf("add column %s: %w", colName, err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate new columns: %w", err)
	}

	// Get columns from temp table for explicit INSERT
	tempCols, err := getDebugTempTableColumns(ctx, conn, tempTable)
	if err != nil {
		return err
	}

	// Insert with explicit column list - DuckDB fills NULLs for missing columns
	colList := strings.Join(tempCols, ", ")
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", accumTable, colList, colList, tempTable)
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

// getDebugTempTableColumns returns the column names from the temp table, quoted for SQL.
func getDebugTempTableColumns(ctx context.Context, conn *sql.Conn, tempTable string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT column_name FROM information_schema.columns
		WHERE table_name = '%s' ORDER BY ordinal_position
	`, tempTable)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query temp columns: %w", err)
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
			return nil, fmt.Errorf("scan temp column: %w", err)
		}
		cols = append(cols, fmt.Sprintf("\"%s\"", colName))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate temp columns: %w", err)
	}

	return cols, nil
}

func getReadFunc(signal string) string {
	switch signal {
	case "logs":
		return "otel_logs_read"
	case "metrics":
		return "otel_metrics_read"
	case "traces":
		return "otel_traces_read"
	default:
		return "otel_logs_read"
	}
}

func getMemoryUsage(ctx context.Context, conn *sql.Conn) int64 {
	var memBytes int64
	// Try to get memory usage from duckdb_memory()
	query := `SELECT COALESCE(SUM(memory_usage_bytes), 0) FROM duckdb_memory()`
	if err := conn.QueryRowContext(ctx, query).Scan(&memBytes); err != nil {
		return 0
	}
	return memBytes
}

func escapeSingleQuote(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
