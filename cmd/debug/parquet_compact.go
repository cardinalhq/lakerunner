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

func GetParquetCompactCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "parquet-compact [DIR]",
		Short: "Test parquet compaction strategies for memory usage",
		Long: `Reads parquet files from the specified directory and tests different
compaction strategies with DuckDB, comparing batch (union_by_name) vs incremental.

The incremental approach reads parquet schema directly without temp tables.

Use --from-binpb to first convert binpb.gz files to individual parquet files.`,
		Args: cobra.ExactArgs(1),
		RunE: runParquetCompactTest,
	}

	cmd.Flags().Int("limit", 0, "Limit number of files (0 = all)")
	cmd.Flags().Int64("memory-limit-mb", 3072, "DuckDB memory limit in MB")
	cmd.Flags().Int("threads", 1, "Number of DuckDB threads")
	cmd.Flags().Bool("from-binpb", false, "Convert binpb.gz files to parquet first (for logs only)")
	cmd.Flags().String("org-id", "test-org", "Organization ID for binpb parsing")

	return cmd
}

func runParquetCompactTest(c *cobra.Command, args []string) error {
	ctx := context.Background()
	inputDir := args[0]
	limit, _ := c.Flags().GetInt("limit")
	memoryLimitMB, _ := c.Flags().GetInt64("memory-limit-mb")
	threads, _ := c.Flags().GetInt("threads")
	fromBinpb, _ := c.Flags().GetBool("from-binpb")
	orgID, _ := c.Flags().GetString("org-id")

	var files []string
	var parquetDir string

	if fromBinpb {
		// Find binpb.gz files and convert to parquet
		var binpbFiles []string
		err := filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".binpb.gz") {
				return nil
			}
			// Only logs for now
			if !strings.Contains(filepath.Base(path), "logs_") {
				return nil
			}
			if limit > 0 && len(binpbFiles) >= limit {
				return nil
			}
			binpbFiles = append(binpbFiles, path)
			return nil
		})
		if err != nil {
			return fmt.Errorf("walk directory: %w", err)
		}

		if len(binpbFiles) == 0 {
			return fmt.Errorf("no logs binpb.gz files found in %s", inputDir)
		}

		// Create temp dir for parquet files
		var err2 error
		parquetDir, err2 = os.MkdirTemp("", "parquet-input-*")
		if err2 != nil {
			return fmt.Errorf("create temp dir: %w", err2)
		}
		defer func() { _ = os.RemoveAll(parquetDir) }()

		slog.Info("Converting binpb.gz to parquet", slog.Int("files", len(binpbFiles)))

		// Convert each binpb.gz to parquet
		settings := &duckdbx.DuckDBSettings{
			MemoryLimitMB: memoryLimitMB,
			Threads:       threads,
		}
		convertedFiles, err := convertBinpbToParquet(ctx, binpbFiles, orgID, parquetDir, settings)
		if err != nil {
			return fmt.Errorf("convert binpb to parquet: %w", err)
		}
		files = convertedFiles
		slog.Info("Conversion complete", slog.Int("parquet_files", len(files)))
	} else {
		// Find existing parquet files
		err := filepath.Walk(inputDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".parquet") {
				return nil
			}
			if limit > 0 && len(files) >= limit {
				return nil
			}
			files = append(files, path)
			return nil
		})
		if err != nil {
			return fmt.Errorf("walk directory: %w", err)
		}

		if len(files) == 0 {
			return fmt.Errorf("no parquet files found in %s", inputDir)
		}
	}

	// Calculate total input size
	var totalSize int64
	for _, f := range files {
		info, _ := os.Stat(f)
		totalSize += info.Size()
	}

	slog.Info("Found parquet files",
		slog.Int("count", len(files)),
		slog.String("total_size", formatBytes(totalSize)))

	settings := &duckdbx.DuckDBSettings{
		MemoryLimitMB: memoryLimitMB,
		Threads:       threads,
	}

	tmpDir, err := os.MkdirTemp("", "parquet-compact-*")
	if err != nil {
		return fmt.Errorf("create temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmpDir) }()

	// Test batch strategy (union_by_name)
	slog.Info("Testing batch strategy (union_by_name=true)")
	batchResult, err := testBatchParquetCompact(ctx, files, tmpDir, settings)
	if err != nil {
		slog.Error("Batch strategy failed", slog.Any("error", err))
	} else {
		printCompactResult("batch", batchResult)
	}

	// Test incremental strategy
	slog.Info("Testing incremental strategy (schema-aware)")
	incrResult, err := testIncrementalParquetCompact(ctx, files, tmpDir, settings)
	if err != nil {
		slog.Error("Incremental strategy failed", slog.Any("error", err))
	} else {
		printCompactResult("incremental", incrResult)
	}

	return nil
}

type compactResult struct {
	FileCount       int
	RowCount        int64
	OutputSize      int64
	Duration        time.Duration
	PeakMemoryBytes int64
}

func printCompactResult(strategy string, r *compactResult) {
	slog.Info("Strategy result",
		slog.String("strategy", strategy),
		slog.Int("files", r.FileCount),
		slog.Int64("rows", r.RowCount),
		slog.String("output_size", formatBytes(r.OutputSize)),
		slog.String("duration", r.Duration.String()),
		slog.String("peak_memory", formatBytes(r.PeakMemoryBytes)))
}

// testBatchParquetCompact loads all parquet files at once using union_by_name
func testBatchParquetCompact(ctx context.Context, files []string, tmpDir string, settings *duckdbx.DuckDBSettings) (*compactResult, error) {
	start := time.Now()
	result := &compactResult{FileCount: len(files)}

	dbPath := filepath.Join(tmpDir, "batch_compact.duckdb")
	db, err := duckdbx.NewDB(duckdbx.WithDatabasePath(dbPath), duckdbx.WithDuckDBSettings(*settings))
	if err != nil {
		return nil, fmt.Errorf("create duckdb: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.Error("failed to close duckdb", slog.Any("error", err))
		}
	}()

	conn, release, err := db.GetConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}
	defer release()

	// Build file list
	quotedFiles := make([]string, len(files))
	for i, f := range files {
		quotedFiles[i] = fmt.Sprintf("'%s'", escapeSingleQuote(f))
	}
	fileList := strings.Join(quotedFiles, ", ")

	// Load all files at once with union_by_name
	createSQL := fmt.Sprintf("CREATE TABLE data AS SELECT * FROM read_parquet([%s], union_by_name=true)", fileList)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	// Get row count and memory
	_ = conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM data").Scan(&result.RowCount)
	result.PeakMemoryBytes = getMemoryUsage(ctx, conn)

	// Export to parquet
	outputPath := filepath.Join(tmpDir, "batch_output.parquet")
	exportSQL := fmt.Sprintf("COPY data TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", outputPath)
	if _, err := conn.ExecContext(ctx, exportSQL); err != nil {
		return nil, fmt.Errorf("export parquet: %w", err)
	}

	info, _ := os.Stat(outputPath)
	result.OutputSize = info.Size()
	result.Duration = time.Since(start)

	return result, nil
}

// testIncrementalParquetCompact loads parquet files one at a time, merging schemas
func testIncrementalParquetCompact(ctx context.Context, files []string, tmpDir string, settings *duckdbx.DuckDBSettings) (*compactResult, error) {
	start := time.Now()
	result := &compactResult{FileCount: len(files)}
	var maxMemory int64

	dbPath := filepath.Join(tmpDir, "incr_compact.duckdb")
	db, err := duckdbx.NewDB(duckdbx.WithDatabasePath(dbPath), duckdbx.WithDuckDBSettings(*settings))
	if err != nil {
		return nil, fmt.Errorf("create duckdb: %w", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			slog.Error("failed to close duckdb", slog.Any("error", err))
		}
	}()

	conn, release, err := db.GetConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get connection: %w", err)
	}
	defer release()

	const accumTable = "data"
	accumExists := false

	for i, file := range files {
		escapedFile := escapeSingleQuote(file)

		if !accumExists {
			// First file: create table directly from parquet
			createSQL := fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM read_parquet('%s')", accumTable, escapedFile)
			if _, err := conn.ExecContext(ctx, createSQL); err != nil {
				return nil, fmt.Errorf("create initial table from file %d: %w", i, err)
			}
			accumExists = true
		} else {
			// Get schema of new file directly (no temp table needed)
			newCols, err := getParquetNewColumns(ctx, conn, file, accumTable)
			if err != nil {
				return nil, fmt.Errorf("get new columns for file %d: %w", i, err)
			}

			// Add new columns to accumulation table
			for _, col := range newCols {
				alterSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN \"%s\" %s", accumTable, col.Name, col.Type)
				if _, err := conn.ExecContext(ctx, alterSQL); err != nil {
					return nil, fmt.Errorf("add column %s for file %d: %w", col.Name, i, err)
				}
			}

			// Get columns from the parquet file for explicit INSERT
			fileCols, err := getParquetColumns(ctx, conn, file)
			if err != nil {
				return nil, fmt.Errorf("get parquet columns for file %d: %w", i, err)
			}

			// Build quoted column list
			quotedCols := make([]string, len(fileCols))
			for j, c := range fileCols {
				quotedCols[j] = fmt.Sprintf("\"%s\"", c)
			}
			colList := strings.Join(quotedCols, ", ")

			// Insert with explicit column list
			insertSQL := fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM read_parquet('%s')",
				accumTable, colList, colList, escapedFile)
			if _, err := conn.ExecContext(ctx, insertSQL); err != nil {
				return nil, fmt.Errorf("insert from file %d: %w", i, err)
			}
		}

		// Track memory
		mem := getMemoryUsage(ctx, conn)
		if mem > maxMemory {
			maxMemory = mem
		}

		if (i+1)%10 == 0 {
			slog.Info("Incremental progress",
				slog.Int("processed", i+1),
				slog.Int("total", len(files)),
				slog.String("peak_memory", formatBytes(maxMemory)))
		}
	}

	// Get row count
	_ = conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", accumTable)).Scan(&result.RowCount)

	// Final memory check
	mem := getMemoryUsage(ctx, conn)
	if mem > maxMemory {
		maxMemory = mem
	}
	result.PeakMemoryBytes = maxMemory

	// Export to parquet
	outputPath := filepath.Join(tmpDir, "incr_output.parquet")
	exportSQL := fmt.Sprintf("COPY %s TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)", accumTable, outputPath)
	if _, err := conn.ExecContext(ctx, exportSQL); err != nil {
		return nil, fmt.Errorf("export parquet: %w", err)
	}

	info, _ := os.Stat(outputPath)
	result.OutputSize = info.Size()
	result.Duration = time.Since(start)

	return result, nil
}

type columnInfo struct {
	Name string
	Type string
}

// getParquetNewColumns returns columns in the parquet file that don't exist in the accumulation table
func getParquetNewColumns(ctx context.Context, conn *sql.Conn, parquetFile, accumTable string) ([]columnInfo, error) {
	// Get columns from parquet file that don't exist in accum table
	// Using DESCRIBE to get actual data columns (not parquet metadata)
	query := fmt.Sprintf(`
		SELECT p.column_name, p.column_type
		FROM (DESCRIBE SELECT * FROM read_parquet('%s')) p
		WHERE p.column_name NOT IN (
			SELECT column_name FROM information_schema.columns WHERE table_name = '%s'
		)
	`, escapeSingleQuote(parquetFile), accumTable)

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query new columns: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", slog.Any("error", err))
		}
	}()

	var cols []columnInfo
	for rows.Next() {
		var col columnInfo
		if err := rows.Scan(&col.Name, &col.Type); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns: %w", err)
	}

	return cols, nil
}

// getParquetColumns returns all column names from a parquet file
func getParquetColumns(ctx context.Context, conn *sql.Conn, parquetFile string) ([]string, error) {
	query := fmt.Sprintf("SELECT column_name FROM (DESCRIBE SELECT * FROM read_parquet('%s'))", escapeSingleQuote(parquetFile))

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("query columns: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("failed to close rows", slog.Any("error", err))
		}
	}()

	var cols []string
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, fmt.Errorf("scan column: %w", err)
		}
		cols = append(cols, col)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns: %w", err)
	}

	return cols, nil
}

// convertBinpbToParquet converts each binpb.gz file to an individual parquet file
func convertBinpbToParquet(ctx context.Context, binpbFiles []string, orgID, outputDir string, settings *duckdbx.DuckDBSettings) ([]string, error) {
	var parquetFiles []string

	for i, binpbFile := range binpbFiles {
		// Create a separate DuckDB instance for each conversion to manage memory
		dbPath := filepath.Join(outputDir, fmt.Sprintf("convert_%d.duckdb", i))
		db, err := duckdbx.NewDB(duckdbx.WithDatabasePath(dbPath), duckdbx.WithDuckDBSettings(*settings))
		if err != nil {
			return nil, fmt.Errorf("create duckdb for file %d: %w", i, err)
		}

		conn, release, err := db.GetConnection(ctx)
		if err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("get connection for file %d: %w", i, err)
		}

		if err := duckdbx.LoadOtelBinpbExtension(ctx, conn); err != nil {
			release()
			_ = db.Close()
			return nil, fmt.Errorf("load extension for file %d: %w", i, err)
		}

		// Output parquet file
		base := filepath.Base(binpbFile)
		base = strings.TrimSuffix(base, ".binpb.gz")
		parquetPath := filepath.Join(outputDir, base+".parquet")

		// Convert binpb to parquet
		copySQL := fmt.Sprintf(
			"COPY (SELECT * FROM otel_logs_read('[%s]', customer_id='%s')) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
			escapeSingleQuote(binpbFile), escapeSingleQuote(orgID), escapeSingleQuote(parquetPath))

		if _, err := conn.ExecContext(ctx, copySQL); err != nil {
			release()
			_ = db.Close()
			return nil, fmt.Errorf("convert file %d: %w", i, err)
		}

		release()
		if err := db.Close(); err != nil {
			slog.Error("failed to close duckdb", slog.Any("error", err))
		}

		// Clean up the temp database file
		_ = os.Remove(dbPath)
		_ = os.Remove(dbPath + ".wal")

		parquetFiles = append(parquetFiles, parquetPath)

		if (i+1)%10 == 0 {
			slog.Info("Conversion progress",
				slog.Int("converted", i+1),
				slog.Int("total", len(binpbFiles)))
		}
	}

	return parquetFiles, nil
}
