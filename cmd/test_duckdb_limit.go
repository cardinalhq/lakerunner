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

//go:build ignore
// +build ignore

package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/process"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
)

func main() {
	// Get process handle for memory monitoring
	pid := int32(os.Getpid())
	proc, err := process.NewProcess(pid)
	if err != nil {
		log.Fatal("Failed to get process handle:", err)
	}

	// Helper to print memory stats
	printMemory := func(label string) {
		runtime.GC() // Force GC for consistent measurements
		time.Sleep(100 * time.Millisecond)

		memInfo, err := proc.MemoryInfo()
		if err != nil {
			log.Printf("Failed to get memory info: %v", err)
			return
		}

		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		fmt.Printf("%-50s RSS: %8.2f MB, Go Heap: %6.2f MB\n",
			label,
			float64(memInfo.RSS)/1024/1024,
			float64(m.HeapAlloc)/1024/1024,
		)
	}

	// Set a strict 1GB limit
	os.Setenv("DUCKDB_MEMORY_LIMIT", "1024")
	os.Setenv("DUCKDB_S3_POOL_SIZE", "2")

	printMemory("Initial (before DuckDB)")

	// Create S3DB
	s3db, err := duckdbx.NewS3DB()
	if err != nil {
		log.Fatal("Failed to create S3DB:", err)
	}
	defer s3db.Close()

	printMemory("After creating S3DB")

	ctx := context.Background()
	conn, release, err := s3db.GetConnection(ctx)
	if err != nil {
		log.Fatal("Failed to get connection:", err)
	}
	defer release()

	printMemory("After getting connection")

	// Query current memory settings
	fmt.Println("\n=== DuckDB Configuration ===")

	configs := []string{
		"memory_limit",
		"max_memory",
		"threads",
		"worker_threads",
		"external_threads",
		"temp_directory",
	}

	for _, config := range configs {
		var value string
		err := conn.QueryRowContext(ctx, fmt.Sprintf("SELECT current_setting('%s')", config)).Scan(&value)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				continue
			}
			log.Printf("Failed to get %s: %v", config, err)
			continue
		}
		fmt.Printf("  %-20s: %s\n", config, value)
	}

	// Check memory stats from PRAGMA
	fmt.Println("\n=== PRAGMA database_size ===")
	rows, err := conn.QueryContext(ctx, "PRAGMA database_size")
	if err != nil {
		log.Fatal("Failed to run PRAGMA database_size:", err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	fmt.Println("Columns:", cols)

	for rows.Next() {
		// Use interface{} slice to handle any column types
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}

		if err := rows.Scan(valPtrs...); err != nil {
			log.Fatal("Failed to scan:", err)
		}

		for i, col := range cols {
			fmt.Printf("  %-20s: %v\n", col, vals[i])
		}
		fmt.Println()
	}

	printMemory("After checking configuration")

	// Now let's try to allocate memory and see what happens
	fmt.Println("\n=== Testing memory allocation ===")

	// Create a large table to test memory limits
	_, err = conn.ExecContext(ctx, `
		CREATE TABLE test_memory AS
		SELECT
			i AS id,
			repeat('x', 1000) AS data
		FROM generate_series(1, 1000000) AS t(i)
	`)
	if err != nil {
		log.Printf("Failed to create large table: %v", err)
	}

	printMemory("After creating 1M row table")

	// Try a memory-intensive operation
	fmt.Println("\nTrying memory-intensive sort...")
	rows, err = conn.QueryContext(ctx, `
		SELECT COUNT(*)
		FROM (
			SELECT * FROM test_memory
			ORDER BY data, id
		) t
	`)
	if err != nil {
		log.Printf("Failed to run sort: %v", err)
	} else {
		rows.Close()
	}

	printMemory("After memory-intensive query")

	// Try aggregation
	fmt.Println("\nTrying aggregation...")
	rows, err = conn.QueryContext(ctx, `
		SELECT data, COUNT(*), STRING_AGG(CAST(id AS VARCHAR), ',')
		FROM test_memory
		GROUP BY data
	`)
	if err != nil {
		log.Printf("Failed to run aggregation: %v", err)
	} else {
		rows.Close()
	}

	printMemory("After aggregation query")

	// Check final memory stats
	fmt.Println("\n=== Final PRAGMA database_size ===")
	rows, err = conn.QueryContext(ctx, "PRAGMA database_size")
	if err != nil {
		log.Fatal("Failed to run PRAGMA database_size:", err)
	}
	defer rows.Close()

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}

		if err := rows.Scan(valPtrs...); err != nil {
			log.Fatal("Failed to scan:", err)
		}

		// Look for memory_usage and memory_limit specifically
		for i, col := range cols {
			if col == "memory_usage" || col == "memory_limit" {
				fmt.Printf("  %-20s: %v\n", col, vals[i])
			}
		}
	}

	printMemory("Final")
}
