// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileconvcmd

import (
	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/pkg/fileconv/rawparquet"
	"github.com/cardinalhq/lakerunner/pkg/fileconv/translate"
)

var Cmd = &cobra.Command{
	Use:   "fileconv",
	Short: "File conversion utilities",
	Long:  `Utilities for converting files from various formats to a common format for processing.`,
	RunE:  run,
}

func init() {
	Cmd.Flags().StringP("input", "i", "", "Input file path")
	_ = Cmd.MarkFlagRequired("input")

	Cmd.Flags().StringP("output", "o", "", "Output file path")
	_ = Cmd.MarkFlagRequired("output")

	Cmd.Flags().StringP("format", "f", "parquet", "Input format (e.g., parquet, csv)")
	_ = Cmd.MarkFlagRequired("format")
}

func run(cmd *cobra.Command, args []string) error {
	input, _ := cmd.Flags().GetString("input")
	output, _ := cmd.Flags().GetString("output")
	format, _ := cmd.Flags().GetString("format")

	cmd.Printf("Converting file from %s to %s in %s format...\n", input, output, format)

	r, err := rawparquet.NewRawParquetReader(input, translate.NewMapper(), nil)
	if err != nil {
		return err
	}
	defer r.Close()

	for {
		row, done, err := r.GetRow()
		if err != nil {
			return err
		}
		if done {
			cmd.Println("No rows to process.")
			return nil
		}
		cmd.Printf("Read row: %+v\n", row)
	}
}
