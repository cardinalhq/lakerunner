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

package debug

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

func getParquetArrowCatSubCmd() *cobra.Command {
	var limit int
	var translator string

	cmd := &cobra.Command{
		Use:   "arrow-cat",
		Short: "Read a parquet file using Apache Arrow and output as JSON",
		RunE: func(cmd *cobra.Command, args []string) error {
			filename, _ := cmd.Flags().GetString("file")
			if filename == "" {
				return errors.New("file is required")
			}

			// Validate translator option
			if translator != "" && translator != "ingest-logs" {
				return fmt.Errorf("invalid translator: %s (supported: ingest-logs)", translator)
			}

			return runParquetArrowCat(filename, limit, translator)
		},
	}

	cmd.Flags().String("file", "", "The parquet file to read")
	cmd.Flags().IntVar(&limit, "limit", 0, "Limit the number of rows to output (0 for no limit)")
	cmd.Flags().StringVar(&translator, "translator", "", "Apply a translator to the rows (options: ingest-logs)")

	return cmd
}

func runParquetArrowCat(filename string, limit int, translatorType string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer func() { _ = file.Close() }()

	ctx := context.Background()
	// os.File already implements parquet.ReaderAtSeeker (io.ReaderAt + io.Seeker)
	ar, err := filereader.NewArrowRawReader(ctx, file, 1000)
	if err != nil {
		return fmt.Errorf("failed to create arrow reader: %w", err)
	}
	defer func() { _ = ar.Close() }()

	// Create translator if requested
	var logTranslator *metricsprocessing.ParquetLogTranslator
	if translatorType == "ingest-logs" {
		// Extract bucket and object ID from filename for translator
		// This is just for debug purposes, so we use simple defaults
		objectID := filepath.Base(filename)
		bucket := "debug-bucket"
		orgID := "debug-org"

		logTranslator = &metricsprocessing.ParquetLogTranslator{
			OrgID:    orgID,
			Bucket:   bucket,
			ObjectID: objectID,
			// ExemplarProcessor is nil for debug mode
		}

		fmt.Fprintf(os.Stderr, "Using ingest-logs translator (org: %s, bucket: %s, object: %s)\n", orgID, bucket, objectID)
	}

	// Print schema information
	schema, err := ar.GetSchema()
	if err != nil {
		return fmt.Errorf("failed to get schema: %w", err)
	}

	fmt.Fprintf(os.Stderr, "Schema has %d fields:\n", len(schema.Fields()))
	for i, field := range schema.Fields() {
		fmt.Fprintf(os.Stderr, "  [%d] %s: %s\n", i, field.Name, field.Type)
	}
	fmt.Fprintln(os.Stderr, "---")

	rowsOutput := 0
	for limit <= 0 || rowsOutput < limit {
		batch, err := ar.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("error reading batch: %w", err)
		}

		// Convert batch rows to JSON
		for i := 0; i < batch.Len(); i++ {
			if limit > 0 && rowsOutput >= limit {
				break
			}

			row := batch.Get(i)

			// Apply translator if configured
			if logTranslator != nil {
				if err := logTranslator.TranslateRow(&row); err != nil {
					return fmt.Errorf("error translating row: %w", err)
				}
			}

			// Convert row to regular map for JSON marshaling
			jsonRow := make(map[string]any)
			for k, v := range row {
				jsonRow[wkk.RowKeyValue(k)] = v
			}

			jsonBytes, err := json.Marshal(jsonRow)
			if err != nil {
				return fmt.Errorf("error marshaling row to JSON: %w", err)
			}
			fmt.Println(string(jsonBytes))
			rowsOutput++
		}
	}

	fmt.Fprintf(os.Stderr, "\nTotal rows read: %d\n", ar.TotalRowsReturned())
	return nil
}
