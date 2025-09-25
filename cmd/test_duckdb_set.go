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

	_ "github.com/marcboeker/go-duckdb/v2"
)

func main() {
	ctx := context.Background()

	// Open DuckDB directly
	db, err := sql.Open("duckdb", "")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	conn, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	// Check default memory limit
	var memLimit string
	err = conn.QueryRowContext(ctx, "SELECT current_setting('memory_limit')").Scan(&memLimit)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Default memory_limit: %s\n", memLimit)

	// Try to set memory limit
	fmt.Println("\nSetting memory_limit to 1024MB...")
	_, err = conn.ExecContext(ctx, "SET memory_limit='1024MB';")
	if err != nil {
		log.Fatal("Failed to set memory limit:", err)
	}

	// Check if it was set
	err = conn.QueryRowContext(ctx, "SELECT current_setting('memory_limit')").Scan(&memLimit)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("After SET memory_limit: %s\n", memLimit)

	// Try PRAGMA
	fmt.Println("\nChecking via PRAGMA database_size...")
	rows, err := conn.QueryContext(ctx, "PRAGMA database_size")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	cols, _ := rows.Columns()
	for rows.Next() {
		vals := make([]interface{}, len(cols))
		valPtrs := make([]interface{}, len(cols))
		for i := range vals {
			valPtrs[i] = &vals[i]
		}
		rows.Scan(valPtrs...)

		for i, col := range cols {
			if col == "memory_limit" {
				fmt.Printf("PRAGMA memory_limit: %v\n", vals[i])
			}
		}
	}

	// Test if limit is actually enforced
	fmt.Println("\n=== Testing if limit is enforced ===")

	// Create a second connection to same DB
	conn2, err := db.Conn(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn2.Close()

	// Check memory limit on second connection
	err = conn2.QueryRowContext(ctx, "SELECT current_setting('memory_limit')").Scan(&memLimit)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Second connection memory_limit: %s\n", memLimit)

	// Try setting on second connection
	_, err = conn2.ExecContext(ctx, "SET memory_limit='512MB';")
	if err != nil {
		fmt.Printf("Failed to set on second connection: %v\n", err)
	} else {
		err = conn2.QueryRowContext(ctx, "SELECT current_setting('memory_limit')").Scan(&memLimit)
		if err == nil {
			fmt.Printf("Second connection after SET: %s\n", memLimit)
		}
	}

	// Check first connection again
	err = conn.QueryRowContext(ctx, "SELECT current_setting('memory_limit')").Scan(&memLimit)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("First connection still: %s\n", memLimit)
}
