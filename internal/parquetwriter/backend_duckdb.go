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

package parquetwriter

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/duckdb/duckdb-go/v2"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// DuckDBBackend implements ParquetBackend using DuckDB's Appender for efficient bulk loading.
// It creates a file-based table in TmpDir, loads data via Appender, and exports to Parquet using COPY TO.
// Using a file-based database avoids memory pressure for large datasets.
type DuckDBBackend struct {
	config BackendConfig

	// DuckDB connection state (using duckdbx for consistent connection management)
	duckdbDB *duckdbx.DB
	conn     *sql.Conn
	release  func() // Release function for the connection
	dbPath   string // Path to the database file (for cleanup)

	// Appender for efficient bulk loading
	appender *duckdb.Appender

	// Schema information
	columnNames  []string              // Sorted column names for consistent ordering
	columnTypes  []filereader.DataType // Types matching columnNames
	columnKeyMap map[wkk.RowKey]int    // Map from row key to column index

	// Data tracking
	rowCount int64

	// Configuration
	conversionPrefixes []string
	tableName          string

	// State tracking
	closed  bool
	aborted bool
}

// NewDuckDBBackend creates a new DuckDB-based backend.
func NewDuckDBBackend(config BackendConfig) (*DuckDBBackend, error) {
	if config.Schema == nil {
		return nil, fmt.Errorf("schema is required and cannot be nil")
	}

	// Get string conversion prefixes
	conversionPrefixes := config.StringConversionPrefixes
	if len(conversionPrefixes) == 0 {
		conversionPrefixes = DefaultStringConversionPrefixes
	}

	// Build sorted column list from schema (filtering out all-null columns)
	var columnNames []string
	columnKeyMap := make(map[wkk.RowKey]int)
	columnTypeMap := make(map[string]filereader.DataType)

	for _, col := range config.Schema.Columns() {
		if !col.HasNonNull {
			continue // Skip all-null columns
		}
		colName := wkk.RowKeyValue(col.Name)
		columnNames = append(columnNames, colName)
		columnTypeMap[colName] = col.DataType
	}

	if len(columnNames) == 0 {
		return nil, fmt.Errorf("schema has no non-null columns")
	}

	// Sort columns for consistent ordering
	sort.Strings(columnNames)

	// Build column types array and key map
	columnTypes := make([]filereader.DataType, len(columnNames))
	for i, name := range columnNames {
		columnTypes[i] = columnTypeMap[name]
		columnKeyMap[wkk.NewRowKey(name)] = i
	}

	// Create a file-based DuckDB database in the temp directory.
	// This avoids memory pressure for large datasets - DuckDB will spill to disk as needed.
	// We create a temp file to get a unique path, then remove it so DuckDB can create the database.
	dbFile, err := os.CreateTemp(config.TmpDir, "duckdb-*.db")
	if err != nil {
		return nil, fmt.Errorf("create temp db file: %w", err)
	}
	dbPath := dbFile.Name()
	_ = dbFile.Close()
	_ = os.Remove(dbPath) // Remove so DuckDB can create a fresh database

	duckdbDB, err := duckdbx.NewDB(duckdbx.WithDatabasePath(dbPath))
	if err != nil {
		_ = os.Remove(dbPath)
		return nil, fmt.Errorf("create duckdb: %w", err)
	}

	// Get a connection from the pool
	ctx := context.Background()
	conn, release, err := duckdbDB.GetConnection(ctx)
	if err != nil {
		_ = duckdbDB.Close()
		_ = os.Remove(dbPath)
		return nil, fmt.Errorf("get connection: %w", err)
	}

	tableName := "parquet_data"

	// Create table with schema
	createSQL := buildCreateTableSQL(tableName, columnNames, columnTypes, conversionPrefixes)
	if _, err := conn.ExecContext(ctx, createSQL); err != nil {
		release()
		_ = duckdbDB.Close()
		_ = os.Remove(dbPath)
		return nil, fmt.Errorf("create table: %w (SQL: %s)", err, createSQL)
	}

	// Create appender
	var appender *duckdb.Appender
	if err := conn.Raw(func(driverConn any) error {
		rawConn, ok := driverConn.(driver.Conn)
		if !ok {
			return fmt.Errorf("failed to get driver connection")
		}
		var appErr error
		appender, appErr = duckdb.NewAppenderFromConn(rawConn, "main", tableName)
		return appErr
	}); err != nil {
		release()
		_ = duckdbDB.Close()
		_ = os.Remove(dbPath)
		return nil, fmt.Errorf("create appender: %w", err)
	}

	return &DuckDBBackend{
		config:             config,
		duckdbDB:           duckdbDB,
		conn:               conn,
		release:            release,
		dbPath:             dbPath,
		appender:           appender,
		columnNames:        columnNames,
		columnTypes:        columnTypes,
		columnKeyMap:       columnKeyMap,
		conversionPrefixes: conversionPrefixes,
		tableName:          tableName,
	}, nil
}

// buildCreateTableSQL builds a CREATE TABLE statement from the schema.
func buildCreateTableSQL(tableName string, columnNames []string, columnTypes []filereader.DataType, conversionPrefixes []string) string {
	var columns []string
	for i, name := range columnNames {
		duckdbType := dataTypeToDuckDB(columnTypes[i], name, conversionPrefixes)
		// Quote column names to handle special characters
		columns = append(columns, fmt.Sprintf("\"%s\" %s", name, duckdbType))
	}
	return fmt.Sprintf("CREATE TABLE %s (%s)", tableName, strings.Join(columns, ", "))
}

// dataTypeToDuckDB converts a filereader.DataType to a DuckDB type name.
func dataTypeToDuckDB(dt filereader.DataType, fieldName string, conversionPrefixes []string) string {
	// Check if this field should be converted to string
	for _, prefix := range conversionPrefixes {
		if strings.HasPrefix(fieldName, prefix) {
			return "VARCHAR"
		}
	}

	switch dt {
	case filereader.DataTypeBool:
		return "BOOLEAN"
	case filereader.DataTypeInt64:
		return "BIGINT"
	case filereader.DataTypeFloat64:
		return "DOUBLE"
	case filereader.DataTypeString:
		return "VARCHAR"
	case filereader.DataTypeBytes:
		return "BLOB"
	case filereader.DataTypeAny:
		return "VARCHAR" // Complex types as JSON/string
	default:
		return "VARCHAR"
	}
}

// Name returns the backend name.
func (b *DuckDBBackend) Name() string {
	return "duckdb"
}

// WriteBatch appends a batch of rows to the DuckDB table via Appender.
func (b *DuckDBBackend) WriteBatch(ctx context.Context, batch *pipeline.Batch) error {
	if b.closed || b.aborted {
		return fmt.Errorf("backend is closed or aborted")
	}

	numRows := batch.Len()
	values := make([]driver.Value, len(b.columnNames))

	for i := range numRows {
		row := batch.Get(i)
		if row == nil {
			continue
		}

		// Reset values to nil
		for j := range values {
			values[j] = nil
		}

		// Fill in values from row
		for key, value := range row {
			idx, ok := b.columnKeyMap[key]
			if !ok {
				// Column not in schema - this is an error
				return fmt.Errorf("row contains unexpected column '%s' not in schema (row %d)",
					wkk.RowKeyValue(key), b.rowCount+int64(i))
			}

			// Convert value if needed
			colName := b.columnNames[idx]
			convertedValue := b.convertValue(colName, value, b.columnTypes[idx])
			values[idx] = convertedValue
		}

		// Append row
		if err := b.appender.AppendRow(values...); err != nil {
			return fmt.Errorf("append row %d: %w", b.rowCount+int64(i), err)
		}
	}

	b.rowCount += int64(numRows)
	return nil
}

// convertValue converts a Go value to the appropriate type for DuckDB.
func (b *DuckDBBackend) convertValue(fieldName string, value any, targetType filereader.DataType) any {
	if value == nil {
		return nil
	}

	// Check if this field should be converted to string
	shouldConvertToString := false
	for _, prefix := range b.conversionPrefixes {
		if strings.HasPrefix(fieldName, prefix) {
			shouldConvertToString = true
			break
		}
	}

	if shouldConvertToString {
		return convertToStringForDuckDB(value)
	}

	// Convert based on target type
	switch targetType {
	case filereader.DataTypeBool:
		if v, ok := value.(bool); ok {
			return v
		}
		return nil
	case filereader.DataTypeInt64:
		switch v := value.(type) {
		case int64:
			return v
		case int:
			return int64(v)
		case int32:
			return int64(v)
		default:
			return nil
		}
	case filereader.DataTypeFloat64:
		switch v := value.(type) {
		case float64:
			return v
		case float32:
			return float64(v)
		case int64:
			return float64(v)
		case int:
			return float64(v)
		default:
			return nil
		}
	case filereader.DataTypeString:
		if v, ok := value.(string); ok {
			return v
		}
		return fmt.Sprintf("%v", value)
	case filereader.DataTypeBytes:
		if v, ok := value.([]byte); ok {
			return v
		}
		return nil
	default:
		return value
	}
}

// convertToStringForDuckDB converts any value to string for DuckDB.
func convertToStringForDuckDB(value any) string {
	if value == nil {
		return ""
	}

	switch v := value.(type) {
	case string:
		return v
	case int:
		return fmt.Sprintf("%d", v)
	case int32:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case uint:
		return fmt.Sprintf("%d", v)
	case uint32:
		return fmt.Sprintf("%d", v)
	case uint64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%f", v)
	case float64:
		return fmt.Sprintf("%f", v)
	case bool:
		return fmt.Sprintf("%t", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// Close finalizes the backend, writes the Parquet file to the writer.
func (b *DuckDBBackend) Close(ctx context.Context, writer io.Writer) (*BackendMetadata, error) {
	if b.closed {
		return nil, fmt.Errorf("backend already closed")
	}
	b.closed = true

	// Flush and close appender
	if b.appender != nil {
		if err := b.appender.Close(); err != nil {
			b.cleanup()
			return nil, fmt.Errorf("close appender: %w", err)
		}
		b.appender = nil
	}

	// Export to Parquet file
	tmpFile, err := os.CreateTemp(b.config.TmpDir, "duckdb-*.parquet")
	if err != nil {
		b.cleanup()
		return nil, fmt.Errorf("create temp file: %w", err)
	}
	tmpFilePath := tmpFile.Name()
	_ = tmpFile.Close()

	copySQL := fmt.Sprintf("COPY (SELECT * FROM %s) TO '%s' (FORMAT PARQUET, COMPRESSION ZSTD)",
		b.tableName, tmpFilePath)

	if _, err := b.conn.ExecContext(ctx, copySQL); err != nil {
		_ = os.Remove(tmpFilePath)
		b.cleanup()
		return nil, fmt.Errorf("export to parquet: %w", err)
	}

	// Copy temp file to output writer
	tmpFileReader, err := os.Open(tmpFilePath)
	if err != nil {
		_ = os.Remove(tmpFilePath)
		b.cleanup()
		return nil, fmt.Errorf("open temp file: %w", err)
	}
	defer func() {
		_ = tmpFileReader.Close()
		_ = os.Remove(tmpFilePath)
	}()

	if _, err := io.Copy(writer, tmpFileReader); err != nil {
		b.cleanup()
		return nil, fmt.Errorf("copy to output: %w", err)
	}

	columnCount := len(b.columnNames)

	// Cleanup DuckDB resources
	b.cleanup()

	return &BackendMetadata{
		RowCount:    b.rowCount,
		ColumnCount: columnCount,
		Extra: map[string]any{
			"backend": "duckdb",
		},
	}, nil
}

// Abort cleans up resources without writing output.
func (b *DuckDBBackend) Abort() {
	if b.aborted {
		return
	}
	b.aborted = true

	if b.appender != nil {
		_ = b.appender.Close()
		b.appender = nil
	}
	b.cleanup()
}

// cleanup releases DuckDB resources and removes the temporary database file.
func (b *DuckDBBackend) cleanup() {
	if b.release != nil {
		b.release()
		b.release = nil
	}
	b.conn = nil
	if b.duckdbDB != nil {
		_ = b.duckdbDB.Close()
		b.duckdbDB = nil
	}
	// Remove the database file and any WAL files
	if b.dbPath != "" {
		_ = os.Remove(b.dbPath)
		_ = os.Remove(b.dbPath + ".wal")
		b.dbPath = ""
	}
}
