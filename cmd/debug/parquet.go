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
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"sort"

	"github.com/parquet-go/parquet-go"
	"github.com/spf13/cobra"
)

func GetParquetCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "parquet",
		Short: "Parquet file debugging utilities",
		Long:  `Various utilities for debugging and inspecting parquet files.`,
	}

	cmd.AddCommand(getParquetCatSubCmd())
	cmd.AddCommand(getParquetSchemaSubCmd())
	cmd.AddCommand(getParquetSchemaRawSubCmd())
	cmd.AddCommand(getParquetArrowCatSubCmd())

	return cmd
}

func getParquetCatSubCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cat",
		Short: "Output parquet file contents as JSON lines",
		Long:  `Reads a parquet file and outputs each row as a JSON line for debugging purposes.`,
		RunE: func(c *cobra.Command, _ []string) error {
			filename, err := c.Flags().GetString("file")
			if err != nil {
				return fmt.Errorf("failed to get file flag: %w", err)
			}

			limit, err := c.Flags().GetInt("limit")
			if err != nil {
				return fmt.Errorf("failed to get limit flag: %w", err)
			}

			keepByteSlices, err := c.Flags().GetBool("keep-byte-slices")
			if err != nil {
				return fmt.Errorf("failed to get keep-byte-slices flag: %w", err)
			}

			return runParquetCat(filename, limit, keepByteSlices)
		},
	}

	cmd.Flags().String("file", "", "Parquet file to read")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(fmt.Errorf("failed to mark file flag as required: %w", err))
	}

	cmd.Flags().Int("limit", 0, "Maximum number of rows to output (0 for unlimited)")
	cmd.Flags().Bool("keep-byte-slices", false, "Keep literal byte slice values instead of converting to '[size]byte'")

	return cmd
}

func getParquetSchemaSubCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema",
		Short: "Analyze parquet file data to determine actual column types",
		Long:  `Reads parquet file data until all columns have non-nil values or EOF, then outputs a JSON schema with actual types found.`,
		RunE: func(c *cobra.Command, _ []string) error {
			filename, err := c.Flags().GetString("file")
			if err != nil {
				return fmt.Errorf("failed to get file flag: %w", err)
			}

			return runParquetSchemaFromData(filename)
		},
	}

	cmd.Flags().String("file", "", "Parquet file to analyze")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(fmt.Errorf("failed to mark file flag as required: %w", err))
	}

	return cmd
}

func getParquetSchemaRawSubCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schema-raw",
		Short: "Print out the raw metadata schema of a Parquet file",
		Long:  `Prints the raw parquet metadata schema structure as defined in the file headers.`,
		RunE: func(c *cobra.Command, _ []string) error {
			filename, err := c.Flags().GetString("file")
			if err != nil {
				return fmt.Errorf("failed to get file flag: %w", err)
			}

			return runParquetSchemaFromMetadata(filename)
		},
	}

	cmd.Flags().String("file", "", "Parquet file to read")
	if err := cmd.MarkFlagRequired("file"); err != nil {
		panic(fmt.Errorf("failed to mark file flag as required: %w", err))
	}

	return cmd
}

func runParquetCat(filename string, limit int, keepByteSlices bool) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file %s: %w", filename, err)
	}

	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	reader := parquet.NewGenericReader[map[string]any](pf, pf.Schema())
	defer func() { _ = reader.Close() }()

	rowsOutput := 0
	batchSize := 1000
	if limit > 0 && limit < batchSize {
		batchSize = limit
	}

	for limit <= 0 || rowsOutput < limit {

		currentBatchSize := batchSize
		if limit > 0 && rowsOutput+batchSize > limit {
			currentBatchSize = limit - rowsOutput
		}

		rows := make([]map[string]any, currentBatchSize)
		for i := range rows {
			rows[i] = make(map[string]any)
		}

		n, err := reader.Read(rows)
		if err != nil && err != io.EOF {
			return fmt.Errorf("error reading parquet rows: %w", err)
		}

		if n == 0 {
			break
		}

		for i := 0; i < n; i++ {
			row := rows[i]
			// Convert sketch field from string to []byte if needed
			row = convertSketchField(row)
			if !keepByteSlices {
				row = convertByteSlices(row)
			}
			jsonBytes, err := json.Marshal(row)
			if err != nil {
				return fmt.Errorf("error marshaling row to JSON: %w", err)
			}
			fmt.Println(string(jsonBytes))
			rowsOutput++

			if limit > 0 && rowsOutput >= limit {
				return nil
			}
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

func convertSketchField(row map[string]any) map[string]any {
	if sketch, ok := row["chq_sketch"].(string); ok && sketch != "" {
		row["chq_sketch"] = []byte(sketch)
	}
	return row
}

func convertByteSlices(row map[string]any) map[string]any {
	converted := make(map[string]any)
	for key, value := range row {
		if byteSlice, ok := value.([]byte); ok {
			converted[key] = fmt.Sprintf("[%d]byte", len(byteSlice))
		} else {
			converted[key] = value
		}
	}
	return converted
}

func runParquetSchemaFromMetadata(filename string) error {
	fh, err := LoadSchemaForFile(filename)
	if err != nil {
		return fmt.Errorf("failed to load schema for file %s: %w", filename, err)
	}
	defer func() {
		_ = fh.Close()
	}()

	fmt.Println(fh.Schema.String())
	return nil
}

type ColumnSchema struct {
	Name    string  `json:"name"`
	Type    string  `json:"type"`
	RawType *string `json:"rawType,omitempty"`
}

type ParquetSchema struct {
	Columns []ColumnSchema `json:"columns"`
}

func runParquetSchemaFromData(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", filename, err)
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat file %s: %w", filename, err)
	}

	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return fmt.Errorf("failed to open parquet file: %w", err)
	}

	reader := parquet.NewGenericReader[map[string]any](pf, pf.Schema())
	defer func() { _ = reader.Close() }()

	seenColumns := make(map[string]string)
	rawTypes := make(map[string]string) // Track original types before conversion
	allColumns := make(map[string]bool)
	batchSize := 1000

	for {
		rows := make([]map[string]any, batchSize)
		for i := range rows {
			rows[i] = make(map[string]any)
		}

		n, err := reader.Read(rows)
		if err != nil && err != io.EOF {
			return fmt.Errorf("error reading parquet rows: %w", err)
		}

		if n == 0 {
			break
		}

		for i := 0; i < n; i++ {
			row := rows[i]

			for columnName, value := range row {
				allColumns[columnName] = true

				if _, exists := seenColumns[columnName]; !exists && value != nil {
					// Track original type before any conversion
					originalType := getGoTypeName(value)

					// Special case: chq_sketch field conversion from string to []byte
					if columnName == "chq_sketch" {
						if _, ok := value.(string); ok {
							rawTypes[columnName] = originalType
							seenColumns[columnName] = "[]byte"
						} else {
							seenColumns[columnName] = originalType
						}
					} else {
						seenColumns[columnName] = originalType
					}
				}
			}
		}

		if err == io.EOF {
			break
		}
	}

	var columns []ColumnSchema
	var columnNames []string
	for columnName := range allColumns {
		columnNames = append(columnNames, columnName)
	}
	sort.Strings(columnNames)

	for _, columnName := range columnNames {
		typeName := seenColumns[columnName]
		if typeName == "" {
			typeName = "null"
		}

		columnSchema := ColumnSchema{
			Name: columnName,
			Type: typeName,
		}

		// Add rawType if we converted the field
		if rawType, hasRawType := rawTypes[columnName]; hasRawType {
			columnSchema.RawType = &rawType
		}

		columns = append(columns, columnSchema)
	}

	schema := ParquetSchema{
		Columns: columns,
	}

	jsonBytes, err := json.MarshalIndent(schema, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling schema to JSON: %w", err)
	}

	fmt.Println(string(jsonBytes))
	return nil
}

func getGoTypeName(value any) string {
	if value == nil {
		return "nil"
	}

	t := reflect.TypeOf(value)
	switch t.Kind() {
	case reflect.String:
		return "string"
	case reflect.Int:
		return "int"
	case reflect.Int8:
		return "int8"
	case reflect.Int16:
		return "int16"
	case reflect.Int32:
		return "int32"
	case reflect.Int64:
		return "int64"
	case reflect.Uint:
		return "uint"
	case reflect.Uint8:
		return "uint8"
	case reflect.Uint16:
		return "uint16"
	case reflect.Uint32:
		return "uint32"
	case reflect.Uint64:
		return "uint64"
	case reflect.Float32:
		return "float32"
	case reflect.Float64:
		return "float64"
	case reflect.Bool:
		return "bool"
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return "[]byte"
		}
		return fmt.Sprintf("[]%s", t.Elem().Kind().String())
	default:
		return t.String()
	}
}
