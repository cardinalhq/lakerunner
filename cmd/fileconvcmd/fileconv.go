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

package fileconvcmd

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/ingestlogs"
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
}

func run(cmd *cobra.Command, args []string) error {
	input, _ := cmd.Flags().GetString("input")

	tmpdir, err := os.MkdirTemp("", "fileconv-*")
	if err != nil {
		return err
	}

	files, err := ingestlogs.ConvertRawParquet(input, tmpdir, "default-bucket", "default-object-id")
	if err != nil {
		return err
	}
	if len(files) == 0 {
		cmd.Println("Input contained no rows")
		return nil
	}

	cmd.Printf("Conversion complete. Output files: %v\n", files)
	return nil
}
