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

import (
	"fmt"

	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "parquet-schema",
		Short: "Print out the schema of a Parquet file",
		RunE: func(c *cobra.Command, _ []string) error {
			filename, err := c.Flags().GetString("file")
			if err != nil {
				return fmt.Errorf("failed to get file flag: %w", err)
			}

			return runParquetSchema(filename)
		},
	}

	rootCmd.AddCommand(cmd)

	cmd.Flags().String("file", "", "Parquet file to read")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(fmt.Errorf("failed to mark file flag as required: %w", err))
	}
}

func runParquetSchema(filename string) error {

	fh, err := filecrunch.LoadSchemaForFile(filename)
	if err != nil {
		return fmt.Errorf("failed to load schema for file %s: %w", filename, err)
	}
	defer func() {
		_ = fh.Close()
	}()

	fmt.Println(fh.Schema.String())

	return nil
}
