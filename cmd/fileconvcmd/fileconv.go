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
	Cmd.MarkFlagRequired("input")

	Cmd.Flags().StringP("output", "o", "", "Output file path")
	Cmd.MarkFlagRequired("output")

	Cmd.Flags().StringP("format", "f", "parquet", "Input format (e.g., parquet, csv)")
	Cmd.MarkFlagRequired("format")
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
