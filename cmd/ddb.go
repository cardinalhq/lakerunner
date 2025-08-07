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

package cmd

// import (
// 	"context"

// 	"github.com/spf13/cobra"

// 	"github.com/cardinalhq/lakerunner/internal/duckdbx"
// )

// func init() {
// 	ddbCmd := &cobra.Command{
// 		Use:   "ddb",
// 		Short: "Run a DuckDB SQL test",
// 		RunE: func(cmd *cobra.Command, args []string) error {
// 			return runDDB(cmd.Context(), args)
// 		},
// 	}

// 	rootCmd.AddCommand(ddbCmd)
// }

// // runDDB runs a DuckDB SQL test.
// func runDDB(ctx context.Context, _ []string) error {
// 	ddb, err := duckdbx.Open("",
// 		duckdbx.WithMemoryLimitMB(2048),
// 		duckdbx.WithExtension("httpfs", ""),
// 		duckdbx.WithMetrics(10),
// 	)
// 	if err != nil {
// 		return err
// 	}
// 	defer ddb.Close()

// 	for range 100 {
// 		c, err := ddb.Conn(ctx)
// 		if err != nil {
// 			return err
// 		}
// 		defer c.Close()

// 		rows, err := c.QueryContext(ctx, "SELECT 42;")
// 		if err != nil {
// 			return err
// 		}

// 		for rows.Next() {
// 			var answer int
// 			if err := rows.Scan(&answer); err != nil {
// 				return err
// 			}
// 			println("The answer is", answer)
// 		}
// 	}

// 	return nil
// }
