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
	"database/sql"
	"fmt"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/shirou/gopsutil/v4/process"
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

		fmt.Printf("%-40s RSS: %8.2f MB, VMS: %10.2f MB, Go Heap: %6.2f MB\n",
			label,
			float64(memInfo.RSS)/1024/1024,
			float64(memInfo.VMS)/1024/1024,
			float64(m.HeapAlloc)/1024/1024,
		)
	}

	// Test different pool sizes
	testPoolSizes := []int{1, 2, 4, 8}

	printMemory("Initial (before any DuckDB)")

	for _, poolSize := range testPoolSizes {
		fmt.Printf("\n=== Testing with pool size %d ===\n", poolSize)

		// Create S3DB with specified pool size
		os.Setenv("DUCKDB_S3_POOL_SIZE", fmt.Sprintf("%d", poolSize))
		os.Setenv("DUCKDB_MEMORY_LIMIT", "256") // 256MB limit per DuckDB instance

		s3db, err := duckdbx.NewS3DB()
		if err != nil {
			log.Fatal("Failed to create S3DB:", err)
		}

		printMemory(fmt.Sprintf("After creating S3DB (pool=%d)", poolSize))

		// Get connections up to pool size and run queries
		ctx := context.Background()
		conns := make([]*sql.Conn, 0, poolSize)
		releaseFuncs := make([]func(), 0, poolSize)

		for i := 0; i < poolSize; i++ {
			conn, release, err := s3db.GetConnection(ctx)
			if err != nil {
				log.Fatal("Failed to get connection:", err)
			}
			conns = append(conns, conn)
			releaseFuncs = append(releaseFuncs, release)

			// Run a simple query to actually use the connection
			var result int
			err = conn.QueryRowContext(ctx, "SELECT 1+1").Scan(&result)
			if err != nil {
				log.Fatal("Failed to run query:", err)
			}

			printMemory(fmt.Sprintf("After getting connection %d/%d", i+1, poolSize))
		}

		// Create a test table and insert some data to see memory impact
		for i, conn := range conns {
			_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS test%d (id INTEGER, data VARCHAR)", i))
			if err != nil {
				log.Fatal("Failed to create table:", err)
			}

			// Insert some rows
			for j := 0; j < 1000; j++ {
				_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO test%d VALUES (%d, 'test data %d')", i, j, j))
				if err != nil {
					log.Fatal("Failed to insert:", err)
				}
			}

			printMemory(fmt.Sprintf("After creating table and data on conn %d", i+1))
		}

		// Run queries that actually use memory
		for i, conn := range conns {
			rows, err := conn.QueryContext(ctx, fmt.Sprintf("SELECT * FROM test%d ORDER BY id", i))
			if err != nil {
				log.Fatal("Failed to query:", err)
			}

			count := 0
			for rows.Next() {
				var id int
				var data string
				if err := rows.Scan(&id, &data); err != nil {
					log.Fatal("Failed to scan:", err)
				}
				count++
			}
			rows.Close()

			printMemory(fmt.Sprintf("After querying %d rows from conn %d", count, i+1))
		}

		// Release all connections
		for _, release := range releaseFuncs {
			release()
		}
		printMemory(fmt.Sprintf("After releasing all %d connections", poolSize))

		// Close S3DB
		s3db.Close()
		printMemory(fmt.Sprintf("After closing S3DB (pool=%d)", poolSize))

		// Force GC and wait a bit
		runtime.GC()
		time.Sleep(500 * time.Millisecond)
		printMemory("After GC and cleanup")
	}

	// Final memory check
	runtime.GC()
	time.Sleep(500 * time.Millisecond)
	printMemory("Final (after all tests)")
}
