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

package queryworker

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
)

type DDBSink struct {
	db        *duckdbx.DB
	parquetDb *sql.DB
	table     string

	// schema cache for quick diffs
	schemaMu sync.RWMutex
	schema   schemaCache

	// serialize all writes (ALTER/INSERT/DELETE)
	writeMu sync.Mutex

	// running row count for the cache table
	totalRows atomic.Int64
}

type schemaCache struct {
	cols  []colDef       // ordered
	index map[string]int // name -> index into cols
}

type colDef struct {
	Name string
	Type string // DuckDB logical type
}

// NewDDBSink opens/creates the DuckDB database at dbPath and ensures `table` exists.
// Clean slate: if dbPath looks like a file (not ":memory:"), we delete it first.
// It also ensures a `segment_id VARCHAR` column is present (idempotent ALTER)
// and loads the schema cache.
func NewDDBSink(dataset string, ctx context.Context) (*DDBSink, error) {
	dbPath := fmt.Sprintf("./db/%s_cached.ddb", dataset)

	// Best-effort remove previous DB file.
	_ = os.Remove(dbPath)

	// Clean slate: remove any existing file.
	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", filepath.Dir(dbPath), err)
	}

	db, err := duckdbx.Open(dbPath,
		duckdbx.WithMemoryLimitMB(2048),
	)
	if err != nil {
		return nil, fmt.Errorf("open duckdb: %w", err)
	}

	parquetDb, err := openForLocalParquet()
	if err != nil {
		return nil, fmt.Errorf("open duckdb for local parquet: %w", err)
	}
	s := &DDBSink{
		db:        db,
		parquetDb: parquetDb,
		table:     "cached",
		schema: schemaCache{
			index: make(map[string]int),
		},
	}

	// Create table (idempotent). Keep minimal schema for compatibility.
	_, conn, err := s.db.ExecContext(ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (ts BIGINT);`, ident("cached")),
	)
	if err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("create table: %w", err)
	}
	defer conn.Close()

	// Ensure segment_id exists (handles pre-existing tables).
	if err := s.ensureSegmentIDColumn(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ensure segment_id: %w", err)
	}

	// Load schema into memory.
	if err := s.reloadSchema(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load schema: %w", err)
	}

	// Clean slate means empty table → totalRows = 0
	s.totalRows.Store(0)

	return s, nil
}

// Close closes the underlying DuckDB connection.
func (s *DDBSink) Close() error {
	return s.db.Close()
}

// RowCount returns the current cached idea of row count (no DB call).
func (s *DDBSink) RowCount() int64 { return s.totalRows.Load() }

// IngestParquetBatch ingests multiple parquet files.
// - Widens schema once using the union schema (single DDL txn).
// - Inserts each file with its own short txn.
// - Populates segment_id per file.
// - Forces anchor timestamp into `ts BIGINT` (errors if no recognizable ts).
func (s *DDBSink) IngestParquetBatch(ctx context.Context, parquetPaths []string, segmentIDs []int64) error {
	if len(parquetPaths) == 0 {
		return nil
	}
	if len(parquetPaths) != len(segmentIDs) {
		return fmt.Errorf("paths and segmentIDs length mismatch")
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// 1) Probe union schema across the whole batch → plan ALTERs once.
	unionAll, err := s.probeParquetSchemaList(ctx, parquetPaths)
	if err != nil {
		return fmt.Errorf("probe batch schema: %w", err)
	}
	// Require the anchor column to exist in the files.
	if _, ok := unionAll["_cardinalhq.timestamp"]; !ok {
		return fmt.Errorf("batch missing required column _cardinalhq.timestamp")
	}

	// 2) Determine missing columns and add them in one txn.
	missing := s.diffMissing(unionAll)
	if len(missing) > 0 {
		if err := s.applyAltersLocked(ctx, missing); err != nil {
			return err
		}
	}

	// Snapshot current table schema for projections (order matters).
	s.schemaMu.RLock()
	tableCols := make([]colDef, len(s.schema.cols))
	copy(tableCols, s.schema.cols)
	s.schemaMu.RUnlock()

	// Helper: build left column list "c1, c2, ..."
	leftCols := func(cols []colDef) string {
		out := make([]string, len(cols))
		for i, c := range cols {
			out[i] = ident(c.Name)
		}
		return strings.Join(out, ", ")
	}

	// 3) Chunked, imperative inserts: many files per txn.
	const chunkSize = 64 // tune 32–256 based on CPU/IO
	for start := 0; start < len(parquetPaths); start += chunkSize {
		end := start + chunkSize
		if end > len(parquetPaths) {
			end = len(parquetPaths)
		}
		pathsChunk := parquetPaths[start:end]
		idsChunk := segmentIDs[start:end]

		// 3a) Probe union schema for this chunk (cheap LIMIT 0) & assert timestamp present.
		unionChunk, err := s.probeParquetSchemaList(ctx, pathsChunk)
		if err != nil {
			return fmt.Errorf("probe chunk schema [%d:%d]: %w", start, end, err)
		}
		if _, ok := unionChunk["_cardinalhq.timestamp"]; !ok {
			return fmt.Errorf("chunk [%d:%d] missing required column _cardinalhq.timestamp", start, end)
		}

		// 3b) VALUES mapping from absolute file path → segment_id
		vals := make([]string, len(pathsChunk))
		for i := range pathsChunk {
			vals[i] = fmt.Sprintf("('%s','%d')", escape(pathsChunk[i]), idsChunk[i])
		}
		mapping := " (VALUES " + strings.Join(vals, ", ") + ") AS m(path, segment_id) "

		// 3c) Build SELECT list aligned to table schema.
		// - segment_id from mapping
		// - every other column: pass-through if present in unionChunk, else CAST(NULL AS type)
		sel := make([]string, 0, len(tableCols))
		for _, c := range tableCols {
			switch c.Name {
			case "segment_id":
				sel = append(sel, "m.segment_id AS segment_id")
			default:
				if _, ok := unionChunk[c.Name]; ok {
					sel = append(sel, ident(c.Name))
				} else {
					sel = append(sel, fmt.Sprintf("CAST(NULL AS %s) AS %s", c.Type, ident(c.Name)))
				}
			}
		}

		// 3d) Single INSERT for the chunk; DuckDB can parallel-scan these files.
		insSQL := fmt.Sprintf(`
INSERT INTO %s (%s)
SELECT %s
FROM (
  SELECT filename, * 
  FROM read_parquet(%s, union_by_name=true, filename=true)
) f
JOIN %s ON f.filename = m.path;
`, ident(s.table), leftCols(tableCols), strings.Join(sel, ", "), sqlStringArray(pathsChunk), mapping)

		// 3e) Short transaction for the chunk.
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
		if err != nil {
			return fmt.Errorf("begin tx (chunk [%d:%d]): %w", start, end, err)
		}
		res, execErr := tx.ExecContext(ctx, insSQL)
		if execErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert chunk [%d:%d]: %w", start, end, execErr)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit (chunk [%d:%d]): %w", start, end, err)
		}

		if affected, err := res.RowsAffected(); err == nil && affected > 0 {
			s.totalRows.Add(affected)
		}
	}

	return nil
}

// DeleteSegments removes all rows for the given segment IDs.
// Returns number of affected rows. Serialized by writeMu.
// Maintains totalRows by subtracting RowsAffected().
func (s *DDBSink) DeleteSegments(ctx context.Context, segmentIDs []int64) (int64, error) {
	if len(segmentIDs) == 0 {
		return 0, nil
	}

	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	// Use VALUES-table to bind arbitrary number of ids safely.
	valHolders := make([]string, len(segmentIDs))
	args := make([]any, len(segmentIDs))
	for i, id := range segmentIDs {
		valHolders[i] = "(?)"
		args[i] = id
	}
	sqlText := fmt.Sprintf(`
DELETE FROM %s
WHERE segment_id IN (
  SELECT v FROM (VALUES %s) AS t(v)
)`, ident(s.table), strings.Join(valHolders, ", "))

	res, conn, err := s.db.ExecContext(ctx, sqlText, args...)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	affected, _ := res.RowsAffected()
	if affected > 0 {
		s.totalRows.Add(-affected)
	}
	return affected, nil
}

// ------------------------- internal: schema handling --------------------------

func (s *DDBSink) ensureSegmentIDColumn(ctx context.Context) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	stmt := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS segment_id BIGINT;`, ident(s.table))
	_, conn, err := s.db.ExecContext(ctx, stmt)
	if err != nil {
		return fmt.Errorf("alter add segment_id: %w", err)
	}
	defer conn.Close()
	return nil
}

func (s *DDBSink) reloadSchema(ctx context.Context) error {
	const q = `
SELECT column_name, data_type
FROM duckdb_columns
WHERE table_name = ?
ORDER BY column_index;
`
	rows, conn, err := s.db.QueryContext(ctx, q, s.table)
	if err != nil {
		return err
	}
	defer rows.Close()
	defer conn.Close()

	var cols []colDef
	idx := make(map[string]int)
	i := 0
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			return err
		}
		cols = append(cols, colDef{Name: name, Type: normalizeDuckType(typ)})
		idx[name] = i
		i++
	}
	if err := rows.Err(); err != nil {
		return err
	}

	s.schemaMu.Lock()
	s.schema.cols = cols
	s.schema.index = idx
	s.schemaMu.Unlock()
	return nil
}

func (s *DDBSink) diffMissing(incoming map[string]string) map[string]string {
	s.schemaMu.RLock()
	defer s.schemaMu.RUnlock()

	out := make(map[string]string)
	for name, typ := range incoming {
		if _, ok := s.schema.index[name]; !ok {
			out[name] = normalizeDuckType(typ)
		}
	}
	// Never try to add duplicates for anchor/system columns:
	delete(out, "ts")
	delete(out, "segment_id")
	return out
}

// applyAltersLocked assumes writeMu is already held.
func (s *DDBSink) applyAltersLocked(ctx context.Context, missing map[string]string) error {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return err
	}
	for name, typ := range missing {
		stmt := fmt.Sprintf(`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;`,
			ident(s.table), ident(name), typ)
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("alter add %s %s: %w", name, typ, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	// Refresh cache after DDL.
	return s.reloadSchema(ctx)
}

// Non-locked variant for callers that don’t hold writeMu (kept for completeness).
func (s *DDBSink) applyAlters(ctx context.Context, missing map[string]string) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.applyAltersLocked(ctx, missing)
}

func openForLocalParquet() (*sql.DB, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		slog.Error("Error opening duckdb for local parquet", "error", err.Error())
	}
	return db, err
}

// Batch insert over a list of files to discover union schema (for ALTER planning).
func (s *DDBSink) probeParquetSchemaList(ctx context.Context, paths []string) (map[string]string, error) {
	q := fmt.Sprintf(`SELECT * FROM read_parquet(%s, union_by_name=true) LIMIT 0`, sqlStringArray(paths))
	rows, err := s.parquetDb.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	colTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	out := make(map[string]string, len(colTypes))
	for _, ct := range colTypes {
		name := ct.Name()
		typ := ct.DatabaseTypeName()
		if typ == "" {
			typ = "VARCHAR"
		}
		out[name] = normalizeDuckType(typ)
	}
	return out, nil
}

func sqlStringArray(paths []string) string {
	quoted := make([]string, len(paths))
	for i, p := range paths {
		quoted[i] = "'" + escape(p) + "'"
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

func ident(name string) string {
	if name == "" {
		return `""`
	}
	// Quote if not [A-Za-z_][A-Za-z0-9_]*
	if name[0] >= '0' && name[0] <= '9' {
		return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
	}
	for _, r := range name {
		if !(r == '_' || (r >= '0' && r <= '9') || (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z')) {
			return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
		}
	}
	return name
}

func escape(path string) string {
	return strings.ReplaceAll(path, `'`, `''`)
}

func normalizeDuckType(t string) string {
	u := strings.ToUpper(strings.TrimSpace(t))
	switch u {
	case "DOUBLE", "DOUBLE PRECISION", "FLOAT8":
		return "DOUBLE"
	case "FLOAT", "FLOAT4", "REAL":
		return "REAL"
	case "BIGINT", "INT8", "LONG":
		return "BIGINT"
	case "INTEGER", "INT", "INT4":
		return "INTEGER"
	case "SMALLINT", "INT2":
		return "SMALLINT"
	case "TINYINT", "INT1":
		return "TINYINT"
	case "BOOLEAN", "BOOL":
		return "BOOLEAN"
	case "TIMESTAMP", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S":
		return "TIMESTAMP"
	case "BLOB", "BYTEA", "VARBINARY":
		return "BLOB"
	case "DECIMAL":
		return "DECIMAL"
	}
	if strings.Contains(u, "VARCHAR") || strings.Contains(u, "STRING") || strings.Contains(u, "UTF8") {
		return "VARCHAR"
	}
	return u
}
