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

package promql

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

const (
	// Source parquet file with all columns
	sourceParquetFile = "/Users/mgraff/test-parquet/parquet/00/tbl_49867390892142563.parquet"
	stepMs            = int64(10000) // 10 second buckets
)

// BenchmarkLogAggregation compares the performance of the complex CTE-based
// SQL path vs the simple flat SQL path for log aggregation queries.
//
// Run with: go test -bench=BenchmarkLogAggregation -benchmem ./promql/
func BenchmarkLogAggregation(b *testing.B) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Get time range from the parquet file
	var minTS, maxTS int64
	row := db.QueryRow(fmt.Sprintf(
		`SELECT MIN(chq_timestamp), MAX(chq_timestamp) FROM read_parquet('%s')`,
		sourceParquetFile,
	))
	if err := row.Scan(&minTS, &maxTS); err != nil {
		b.Fatalf("get time range: %v", err)
	}

	stepMs := int64(10000) // 10 second buckets

	// Complex SQL: CTE pipeline with SELECT * (old path)
	complexSQL := fmt.Sprintf(`
WITH
  s0 AS (SELECT * FROM read_parquet('%s')),
  s0_norm AS (SELECT s0.* REPLACE(CAST("chq_fingerprint" AS VARCHAR) AS "chq_fingerprint") FROM s0),
  s1 AS (SELECT s0_norm.* FROM s0_norm WHERE 1=1 AND true AND CAST("chq_timestamp" AS BIGINT) >= %d AND CAST("chq_timestamp" AS BIGINT) < %d),
  s2 AS (SELECT s1.* FROM s1 WHERE "resource_service_name" IS NOT NULL)
SELECT
  (CAST("chq_timestamp" AS BIGINT) - (CAST("chq_timestamp" AS BIGINT) %% %d)) AS bucket_ts,
  "log_level",
  COUNT(*) AS count
FROM s2
WHERE "chq_timestamp" >= %d AND "chq_timestamp" < %d
GROUP BY bucket_ts, "log_level"
ORDER BY bucket_ts ASC
`, sourceParquetFile, minTS, maxTS, stepMs, minTS, maxTS)

	// Simple SQL: flat query with only needed columns (new path)
	simpleSQL := fmt.Sprintf(`
SELECT
  (CAST("chq_timestamp" AS BIGINT) - (CAST("chq_timestamp" AS BIGINT) %% %d)) AS bucket_ts,
  "log_level",
  COUNT(*) AS count
FROM read_parquet('%s')
WHERE "resource_service_name" IS NOT NULL
  AND "chq_timestamp" >= %d AND "chq_timestamp" < %d
GROUP BY bucket_ts, "log_level"
ORDER BY bucket_ts ASC
`, stepMs, sourceParquetFile, minTS, maxTS)

	b.Run("Complex_CTE_Path", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(complexSQL)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			_ = rows.Close()
			if count == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	b.Run("Simple_Flat_Path", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(simpleSQL)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			_ = rows.Close()
			if count == 0 {
				b.Fatal("no rows returned")
			}
		}
	})
}

// BenchmarkLogAggregationMinimalColumns compares performance when using a parquet
// file with only the columns needed for the query vs the full file.
//
// Run with: go test -bench=BenchmarkLogAggregationMinimalColumns -benchmem ./promql/
func BenchmarkLogAggregationMinimalColumns(b *testing.B) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Create a temp file for the minimal parquet
	tempDir := b.TempDir()
	minimalParquetFile := filepath.Join(tempDir, "minimal.parquet")

	// Create minimal parquet with only needed columns
	createSQL := fmt.Sprintf(`
COPY (
  SELECT chq_timestamp, log_level, resource_service_name
  FROM read_parquet('%s')
) TO '%s' (FORMAT PARQUET)
`, sourceParquetFile, minimalParquetFile)

	_, err = db.Exec(createSQL)
	if err != nil {
		b.Fatalf("create minimal parquet: %v", err)
	}

	// Report file sizes
	fullInfo, err := os.Stat(sourceParquetFile)
	if err != nil {
		b.Fatalf("stat full file: %v", err)
	}
	minimalInfo, err := os.Stat(minimalParquetFile)
	if err != nil {
		b.Fatalf("stat minimal file: %v", err)
	}
	b.Logf("Full file size: %d bytes (%.2f MB)", fullInfo.Size(), float64(fullInfo.Size())/1024/1024)
	b.Logf("Minimal file size: %d bytes (%.2f MB)", minimalInfo.Size(), float64(minimalInfo.Size())/1024/1024)
	b.Logf("Size reduction: %.1f%%", 100*(1-float64(minimalInfo.Size())/float64(fullInfo.Size())))

	// Get time range
	var minTS, maxTS int64
	row := db.QueryRow(fmt.Sprintf(
		`SELECT MIN(chq_timestamp), MAX(chq_timestamp) FROM read_parquet('%s')`,
		sourceParquetFile,
	))
	if err := row.Scan(&minTS, &maxTS); err != nil {
		b.Fatalf("get time range: %v", err)
	}

	stepMs := int64(10000)

	// Simple query on full file
	fullSQL := fmt.Sprintf(`
SELECT
  (CAST("chq_timestamp" AS BIGINT) - (CAST("chq_timestamp" AS BIGINT) %% %d)) AS bucket_ts,
  "log_level",
  COUNT(*) AS count
FROM read_parquet('%s')
WHERE "resource_service_name" IS NOT NULL
  AND "chq_timestamp" >= %d AND "chq_timestamp" < %d
GROUP BY bucket_ts, "log_level"
ORDER BY bucket_ts ASC
`, stepMs, sourceParquetFile, minTS, maxTS)

	// Simple query on minimal file
	minimalSQL := fmt.Sprintf(`
SELECT
  (CAST("chq_timestamp" AS BIGINT) - (CAST("chq_timestamp" AS BIGINT) %% %d)) AS bucket_ts,
  "log_level",
  COUNT(*) AS count
FROM read_parquet('%s')
WHERE "resource_service_name" IS NOT NULL
  AND "chq_timestamp" >= %d AND "chq_timestamp" < %d
GROUP BY bucket_ts, "log_level"
ORDER BY bucket_ts ASC
`, stepMs, minimalParquetFile, minTS, maxTS)

	b.Run("Full_File_69_Columns", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(fullSQL)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			_ = rows.Close()
			if count == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	b.Run("Minimal_File_3_Columns", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(minimalSQL)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			_ = rows.Close()
			if count == 0 {
				b.Fatal("no rows returned")
			}
		}
	})
}

// BenchmarkLogAggregationPreAggregated tests reading from a pre-aggregated parquet
// file that already contains (bucket_ts, log_level, count). This simulates having
// pre-computed rollups/statistics files.
//
// Run with: go test -bench=BenchmarkLogAggregationPreAggregated -benchmem ./promql/
func BenchmarkLogAggregationPreAggregated(b *testing.B) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Get time range
	var minTS, maxTS int64
	row := db.QueryRow(fmt.Sprintf(
		`SELECT MIN(chq_timestamp), MAX(chq_timestamp) FROM read_parquet('%s')`,
		sourceParquetFile,
	))
	if err := row.Scan(&minTS, &maxTS); err != nil {
		b.Fatalf("get time range: %v", err)
	}

	// Create temp files
	tempDir := b.TempDir()
	preAggFile := filepath.Join(tempDir, "preagg.parquet")

	// Create pre-aggregated parquet: already grouped by bucket and log_level
	createPreAggSQL := fmt.Sprintf(`
COPY (
  SELECT
    (CAST("chq_timestamp" AS BIGINT) - (CAST("chq_timestamp" AS BIGINT) %% %d)) AS bucket_ts,
    "log_level",
    COUNT(*) AS count
  FROM read_parquet('%s')
  WHERE "resource_service_name" IS NOT NULL
    AND "chq_timestamp" >= %d AND "chq_timestamp" < %d
  GROUP BY bucket_ts, "log_level"
) TO '%s' (FORMAT PARQUET)
`, stepMs, sourceParquetFile, minTS, maxTS, preAggFile)

	_, err = db.Exec(createPreAggSQL)
	if err != nil {
		b.Fatalf("create pre-aggregated parquet: %v", err)
	}

	// Report file sizes and row counts
	fullInfo, _ := os.Stat(sourceParquetFile)
	preAggInfo, _ := os.Stat(preAggFile)

	var fullRows, preAggRows int64
	row = db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, sourceParquetFile))
	_ = row.Scan(&fullRows)
	row = db.QueryRow(fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, preAggFile))
	_ = row.Scan(&preAggRows)

	b.Logf("Full file: %d bytes (%.2f MB), %d rows", fullInfo.Size(), float64(fullInfo.Size())/1024/1024, fullRows)
	b.Logf("Pre-agg file: %d bytes (%.2f KB), %d rows", preAggInfo.Size(), float64(preAggInfo.Size())/1024, preAggRows)
	b.Logf("Row reduction: %.1f%% (%d -> %d rows)", 100*(1-float64(preAggRows)/float64(fullRows)), fullRows, preAggRows)

	// Query on full file (baseline)
	fullSQL := fmt.Sprintf(`
SELECT
  (CAST("chq_timestamp" AS BIGINT) - (CAST("chq_timestamp" AS BIGINT) %% %d)) AS bucket_ts,
  "log_level",
  COUNT(*) AS count
FROM read_parquet('%s')
WHERE "resource_service_name" IS NOT NULL
  AND "chq_timestamp" >= %d AND "chq_timestamp" < %d
GROUP BY bucket_ts, "log_level"
ORDER BY bucket_ts ASC
`, stepMs, sourceParquetFile, minTS, maxTS)

	// Query on pre-aggregated file - just read and re-sum (or just read if already final)
	// Since it's already grouped by bucket_ts and log_level, we can just read it
	preAggSQL := fmt.Sprintf(`
SELECT bucket_ts, log_level, count
FROM read_parquet('%s')
ORDER BY bucket_ts ASC
`, preAggFile)

	// Or if we want to be able to merge multiple pre-agg files, we'd sum:
	preAggSumSQL := fmt.Sprintf(`
SELECT bucket_ts, log_level, SUM(count) AS count
FROM read_parquet('%s')
GROUP BY bucket_ts, log_level
ORDER BY bucket_ts ASC
`, preAggFile)

	b.Run("Full_File_Aggregate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(fullSQL)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			_ = rows.Close()
			if count == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	b.Run("PreAgg_Just_Read", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(preAggSQL)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			_ = rows.Close()
			if count == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	b.Run("PreAgg_With_Sum", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(preAggSumSQL)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			_ = rows.Close()
			if count == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	// Now test with 100 copies to simulate merging many segment files
	preAggDir := filepath.Join(tempDir, "preagg_files")
	if err := os.MkdirAll(preAggDir, 0755); err != nil {
		b.Fatalf("mkdir: %v", err)
	}

	// Copy the pre-agg file 100 times
	for i := range 100 {
		dest := filepath.Join(preAggDir, fmt.Sprintf("preagg_%03d.parquet", i))
		copySQL := fmt.Sprintf(`COPY (SELECT * FROM read_parquet('%s')) TO '%s' (FORMAT PARQUET)`, preAggFile, dest)
		if _, err := db.Exec(copySQL); err != nil {
			b.Fatalf("copy file %d: %v", i, err)
		}
	}

	// Query to merge all 100 pre-agg files
	preAgg100SQL := fmt.Sprintf(`
SELECT bucket_ts, log_level, SUM(count) AS count
FROM read_parquet('%s/*.parquet')
GROUP BY bucket_ts, log_level
ORDER BY bucket_ts ASC
`, preAggDir)

	b.Logf("Created 100 pre-agg files, total ~%.1f KB", float64(100*preAggInfo.Size())/1024)

	b.Run("PreAgg_100_Files_Merge", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(preAgg100SQL)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			_ = rows.Close()
			if count == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	// Create 100 copies of the minimal columns file for fair comparison
	minimalDir := filepath.Join(tempDir, "minimal_files")
	if err := os.MkdirAll(minimalDir, 0755); err != nil {
		b.Fatalf("mkdir: %v", err)
	}

	// Create minimal file first
	minimalFile := filepath.Join(tempDir, "minimal.parquet")
	createMinimalSQL := fmt.Sprintf(`
COPY (
  SELECT chq_timestamp, log_level, resource_service_name
  FROM read_parquet('%s')
) TO '%s' (FORMAT PARQUET)
`, sourceParquetFile, minimalFile)
	if _, err := db.Exec(createMinimalSQL); err != nil {
		b.Fatalf("create minimal: %v", err)
	}

	// Copy minimal file 100 times
	for i := range 100 {
		dest := filepath.Join(minimalDir, fmt.Sprintf("minimal_%03d.parquet", i))
		copySQL := fmt.Sprintf(`COPY (SELECT * FROM read_parquet('%s')) TO '%s' (FORMAT PARQUET)`, minimalFile, dest)
		if _, err := db.Exec(copySQL); err != nil {
			b.Fatalf("copy minimal %d: %v", i, err)
		}
	}

	minimalInfo, _ := os.Stat(minimalFile)
	b.Logf("Created 100 minimal files, total ~%.1f MB", float64(100*minimalInfo.Size())/1024/1024)

	// Query to aggregate all 100 minimal files
	minimal100SQL := fmt.Sprintf(`
SELECT
  (CAST("chq_timestamp" AS BIGINT) - (CAST("chq_timestamp" AS BIGINT) %% %d)) AS bucket_ts,
  "log_level",
  COUNT(*) AS count
FROM read_parquet('%s/*.parquet')
WHERE "resource_service_name" IS NOT NULL
GROUP BY bucket_ts, "log_level"
ORDER BY bucket_ts ASC
`, stepMs, minimalDir)

	b.Run("Minimal_100_Files_Aggregate", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			rows, err := db.Query(minimal100SQL)
			if err != nil {
				b.Fatalf("query failed: %v", err)
			}
			count := 0
			for rows.Next() {
				count++
			}
			_ = rows.Close()
			if count == 0 {
				b.Fatal("no rows returned")
			}
		}
	})

	// Test rollups from 10s pre-agg to coarser granularities
	rollupSteps := []struct {
		name   string
		stepMs int64
	}{
		{"10s_native", 10000},
		{"1m_rollup", 60000},
		{"5m_rollup", 300000},
		{"20m_rollup", 1200000},
		{"1h_rollup", 3600000},
	}

	for _, rs := range rollupSteps {
		rs := rs // capture for closure
		b.Run(fmt.Sprintf("PreAgg_100_to_%s", rs.name), func(b *testing.B) {
			rollupSQL := fmt.Sprintf(`
SELECT
  (bucket_ts - (bucket_ts %% %d)) AS rolled_bucket_ts,
  log_level,
  SUM(count) AS count
FROM read_parquet('%s/*.parquet')
GROUP BY rolled_bucket_ts, log_level
ORDER BY rolled_bucket_ts ASC
`, rs.stepMs, preAggDir)

			b.ReportAllocs()
			var resultRows int
			for i := 0; i < b.N; i++ {
				rows, err := db.Query(rollupSQL)
				if err != nil {
					b.Fatalf("query failed: %v", err)
				}
				resultRows = 0
				for rows.Next() {
					resultRows++
				}
				_ = rows.Close()
				if resultRows == 0 {
					b.Fatal("no rows returned")
				}
			}
			b.ReportMetric(float64(resultRows), "result_rows")
		})
	}
}
