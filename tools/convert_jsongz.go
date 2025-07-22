package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/pkg/fileconv/jsongz"
	"github.com/cardinalhq/lakerunner/pkg/fileconv/translate"
)

func main() {
	var inputFile, outputDir string
	flag.StringVar(&inputFile, "input", "", "Input JSON.gz file path")
	flag.StringVar(&outputDir, "output", "./output", "Output directory for parquet files")
	flag.Parse()

	if inputFile == "" {
		log.Fatal("Please provide input file with -input flag")
	}

	// Check if input file exists
	if _, err := os.Stat(inputFile); os.IsNotExist(err) {
		log.Fatalf("Input file does not exist: %s", inputFile)
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		log.Fatalf("Failed to create output directory: %v", err)
	}

	// Extract bucket and objectID from filename for metadata
	bucket := "test-bucket"
	objectID := filepath.Base(inputFile)

	fmt.Printf("Converting %s to parquet...\n", inputFile)
	fmt.Printf("Output directory: %s\n", outputDir)

	// Convert the file
	fnames, err := convertJSONGzFile(inputFile, outputDir, bucket, objectID)
	if err != nil {
		log.Fatalf("Failed to convert file: %v", err)
	}

	fmt.Printf("Successfully converted to %d parquet file(s):\n", len(fnames))
	for _, fname := range fnames {
		fmt.Printf("  %s\n", fname)
	}
}

// convertJSONGzFile converts a JSON.gz file to the standardized format
// This follows the same pipeline as cmd/ingest_logs_cmd.go
func convertJSONGzFile(tmpfilename, tmpdir, bucket, objectID string) ([]string, error) {
	r, err := jsongz.NewJSONGzReader(tmpfilename, translate.NewMapper(), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	nmb := buffet.NewNodeMapBuilder()

	baseitems := map[string]string{
		"resource.bucket.name": bucket,
		"resource.file.name":   "./" + objectID,
		"resource.file.type":   getFileType(objectID),
	}

	// First pass: read all rows to build complete schema
	allRows := make([]map[string]any, 0)
	for {
		row, done, err := r.GetRow()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}

		// Add base items to the row
		for k, v := range baseitems {
			row[k] = v
		}

		// Add row to schema builder
		if err := nmb.Add(row); err != nil {
			return nil, fmt.Errorf("failed to add row to schema: %w", err)
		}

		allRows = append(allRows, row)
	}

	if len(allRows) == 0 {
		return nil, fmt.Errorf("no rows processed")
	}

	// Create writer with complete schema
	w, err := buffet.NewWriter("fileconv", tmpdir, nmb.Build(), 0, 0)
	if err != nil {
		return nil, err
	}

	defer func() {
		_, err := w.Close()
		if err != buffet.ErrAlreadyClosed && err != nil {
			log.Printf("Failed to close writer: %v", err)
		}
	}()

	// Second pass: write all rows
	for _, row := range allRows {
		if err := w.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write row: %w", err)
		}
	}

	result, err := w.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no records written to file")
	}

	var fnames []string
	for _, res := range result {
		fnames = append(fnames, res.FileName)
	}
	return fnames, nil
}

func getFileType(p string) string {
	ext := filepath.Ext(p)
	switch ext {
	case ".json.gz":
		return "json.gz"
	case ".parquet":
		return "parquet"
	default:
		return ext[1:] // Remove the dot
	}
}
