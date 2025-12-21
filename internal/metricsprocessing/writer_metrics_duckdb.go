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

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/lib/pq"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/helpers"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter/factories"
	"github.com/cardinalhq/lakerunner/lrdb"
)

// groupByColumns are the columns used to group metrics for aggregation.
// Metrics with the same (metric_name, chq_tid, truncated_timestamp) are merged.
var groupByColumns = []string{"metric_name", "chq_tid", "chq_timestamp"}

// sketchColumn is the column containing the DDSketch data to merge.
const sketchColumn = "chq_sketch"

// rollupColumns are computed from the merged sketch.
var rollupColumns = []string{
	"chq_rollup_count",
	"chq_rollup_sum",
	"chq_rollup_avg",
	"chq_rollup_min",
	"chq_rollup_max",
	"chq_rollup_p25",
	"chq_rollup_p50",
	"chq_rollup_p75",
	"chq_rollup_p90",
	"chq_rollup_p95",
	"chq_rollup_p99",
}

// processMetricsWithDuckDB performs metric aggregation using DuckDB SQL:
// 1. Read parquet files from local disk
// 2. Truncate timestamps to aggregation period
// 3. Group by (metric_name, chq_tid, truncated_timestamp)
// 4. Merge DDSketches using ddsketch_agg
// 5. Extract statistics from merged sketch
// 6. Write result to parquet
func processMetricsWithDuckDB(ctx context.Context, duckDB *duckdbx.DB, params metricProcessingParams) ([]parquetwriter.Result, error) {
	ll := logctx.FromContext(ctx)

	// Download parquet files to local temp directory
	localFiles, err := downloadSegmentFiles(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("download segment files: %w", err)
	}
	defer cleanupLocalFiles(localFiles)

	if len(localFiles) == 0 {
		return nil, fmt.Errorf("no parquet files to process")
	}

	ll.Info("Processing metrics with DuckDB",
		slog.Int("fileCount", len(localFiles)),
		slog.Int("frequencyMs", int(params.FrequencyMs)))

	// Get a connection from the pool
	conn, release, err := duckDB.GetConnection(ctx)
	if err != nil {
		return nil, fmt.Errorf("get duckdb connection: %w", err)
	}
	defer release()

	// Load DDSketch extension on this connection
	if err := duckdbx.LoadDDSketchExtension(ctx, conn); err != nil {
		return nil, fmt.Errorf("load ddsketch extension: %w", err)
	}

	// Discover schema from input files
	schema, err := discoverSchemaConn(ctx, conn, localFiles[0])
	if err != nil {
		return nil, fmt.Errorf("discover schema: %w", err)
	}

	// Generate and execute aggregation SQL
	outputFile := filepath.Join(params.TmpDir, "rollup_output.parquet")
	if err := executeAggregationConn(ctx, conn, localFiles, outputFile, schema, params.FrequencyMs); err != nil {
		return nil, fmt.Errorf("execute aggregation: %w", err)
	}

	// Get output file stats
	fileInfo, err := os.Stat(outputFile)
	if err != nil {
		return nil, fmt.Errorf("stat output file: %w", err)
	}

	// Get record count from output file (the only thing we need to query)
	recordCount, err := getRecordCount(ctx, conn, outputFile)
	if err != nil {
		return nil, fmt.Errorf("get record count: %w", err)
	}

	// Compute metadata from input segments (no need to query output file)
	firstTS, lastTS, fingerprints, metricNames, metricTypes := computeStatsFromSegments(params.ActiveSegments)

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

	ll.Info("DuckDB aggregation completed",
		slog.Int64("recordCount", recordCount),
		slog.Int64("fileSize", fileInfo.Size()))

	return []parquetwriter.Result{result}, nil
}

// downloadSegmentFiles downloads parquet files from cloud storage to local temp directory.
func downloadSegmentFiles(ctx context.Context, params metricProcessingParams) ([]string, error) {
	ll := logctx.FromContext(ctx)
	var localFiles []string

	for _, segment := range params.ActiveSegments {
		objectPath := helpers.MakeDBObjectID(
			params.OrganizationID,
			params.StorageProfile.CollectorName,
			segment.Dateint,
			int16((segment.TsRange.Lower.Int64/(1000*60*60))%24),
			segment.SegmentID,
			"metrics",
		)

		localPath, _, _, err := params.StorageClient.DownloadObject(ctx, params.TmpDir, params.StorageProfile.Bucket, objectPath)
		if err != nil {
			ll.Warn("Failed to download segment file",
				slog.Int64("segmentID", segment.SegmentID),
				slog.Any("error", err))
			continue
		}

		localFiles = append(localFiles, localPath)
	}

	return localFiles, nil
}

// cleanupLocalFiles removes temporary local files.
func cleanupLocalFiles(files []string) {
	for _, f := range files {
		_ = os.Remove(f)
	}
}

// discoverSchemaConn reads the schema from a parquet file and returns column names.
func discoverSchemaConn(ctx context.Context, conn *sql.Conn, filePath string) ([]string, error) {
	query := fmt.Sprintf("DESCRIBE SELECT * FROM read_parquet('%s')", filePath)
	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("describe parquet: %w", err)
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

// executeAggregationConn runs the DuckDB aggregation query.
func executeAggregationConn(ctx context.Context, conn *sql.Conn, inputFiles []string, outputFile string, schema []string, frequencyMs int32) error {
	// Build file list for read_parquet
	quotedFiles := make([]string, len(inputFiles))
	for i, f := range inputFiles {
		quotedFiles[i] = fmt.Sprintf("'%s'", f)
	}
	fileList := strings.Join(quotedFiles, ", ")

	// Classify columns
	groupCols := make(map[string]bool)
	for _, c := range groupByColumns {
		groupCols[c] = true
	}

	rollupCols := make(map[string]bool)
	for _, c := range rollupColumns {
		rollupCols[c] = true
	}

	// Build SELECT clauses
	var selectClauses []string
	var groupByClauses []string

	for _, col := range schema {
		qcol := pq.QuoteIdentifier(col)
		if col == sketchColumn {
			// Merge sketches
			selectClauses = append(selectClauses, fmt.Sprintf("ddsketch_agg(%s) AS %s", qcol, qcol))
		} else if col == "chq_timestamp" {
			// Truncate timestamp to aggregation period
			selectClauses = append(selectClauses, fmt.Sprintf(
				"(%s / %d) * %d AS %s",
				qcol, frequencyMs, frequencyMs, qcol))
			groupByClauses = append(groupByClauses, fmt.Sprintf("(%s / %d) * %d", qcol, frequencyMs, frequencyMs))
		} else if col == "chq_tsns" {
			// Truncate nanosecond timestamp to aggregation period
			freqNs := int64(frequencyMs) * 1_000_000
			selectClauses = append(selectClauses, fmt.Sprintf(
				"(%s / %d) * %d AS %s",
				qcol, freqNs, freqNs, qcol))
			groupByClauses = append(groupByClauses, fmt.Sprintf("(%s / %d) * %d", qcol, freqNs, freqNs))
		} else if groupCols[col] {
			// Group by column - pass through
			selectClauses = append(selectClauses, qcol)
			groupByClauses = append(groupByClauses, qcol)
		} else if rollupCols[col] {
			// Skip rollup columns - we'll compute them from the merged sketch
			continue
		} else {
			// Metadata column - use first value
			selectClauses = append(selectClauses, fmt.Sprintf("first(%s) AS %s", qcol, qcol))
		}
	}

	// Build outer query with rollup column extraction
	innerQuery := fmt.Sprintf(`
		SELECT %s
		FROM read_parquet([%s])
		GROUP BY %s`,
		strings.Join(selectClauses, ", "),
		fileList,
		strings.Join(groupByClauses, ", "))

	// Add rollup columns computed from merged sketch
	rollupExtraction := []string{
		"ddsketch_count(chq_sketch) AS chq_rollup_count",
		"ddsketch_sum(chq_sketch) AS chq_rollup_sum",
		"ddsketch_avg(chq_sketch) AS chq_rollup_avg",
		"ddsketch_min(chq_sketch) AS chq_rollup_min",
		"ddsketch_max(chq_sketch) AS chq_rollup_max",
		"ddsketch_quantile(chq_sketch, 0.25) AS chq_rollup_p25",
		"ddsketch_quantile(chq_sketch, 0.50) AS chq_rollup_p50",
		"ddsketch_quantile(chq_sketch, 0.75) AS chq_rollup_p75",
		"ddsketch_quantile(chq_sketch, 0.90) AS chq_rollup_p90",
		"ddsketch_quantile(chq_sketch, 0.95) AS chq_rollup_p95",
		"ddsketch_quantile(chq_sketch, 0.99) AS chq_rollup_p99",
	}

	fullQuery := fmt.Sprintf(`
		COPY (
			SELECT *, %s
			FROM (%s) AS merged
			ORDER BY "metric_name", "chq_tid", "chq_timestamp"
		) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)`,
		strings.Join(rollupExtraction, ", "),
		innerQuery,
		outputFile)

	_, err := conn.ExecContext(ctx, fullQuery)
	return err
}

// getRecordCount queries the record count from a parquet file.
func getRecordCount(ctx context.Context, conn *sql.Conn, filePath string) (int64, error) {
	var count int64
	err := conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", filePath)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("count records: %w", err)
	}
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
