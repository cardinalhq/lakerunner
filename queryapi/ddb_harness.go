// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package queryapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/cardinalhq/lakerunner/logql"
)

// IngestExemplarLogsJSONToDuckDB ingests a map[string]any exemplar into DuckDB.
// It returns the number of rows inserted.
func IngestExemplarLogsJSONToDuckDB(
	ctx context.Context,
	db *sql.DB,
	tableName string,
	exemplarData map[string]any,
) (int, error) {
	if err := ensureBaseTable(ctx, db, tableName); err != nil {
		slog.Error("Failed to create base table", "error", err)
		return 0, err
	}

	existingCols, err := getExistingColumns(ctx, db, tableName)
	if err != nil {
		slog.Error("Failed to get existing columns", "error", err)
		return 0, err
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("Failed to begin transaction", "error", err)
		return 0, fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }() // no-op if already committed

	// Add a dummy fingerprint for test queries that expect it
	if _, exists := exemplarData["chq_fingerprint"]; !exists {
		exemplarData["chq_fingerprint"] = "test-fingerprint"
	}
	// Add timestamp if absent in exemplar
	if _, exists := exemplarData["chq_timestamp"]; !exists {
		exemplarData["chq_timestamp"] = time.Now().UnixMilli()
	}

	if err := ensureColumnsForRow(ctx, tx, tableName, exemplarData, existingCols); err != nil {
		slog.Error("Failed to ensure columns for row", "error", err)
		return 0, err
	}

	if _, err := insertRow(ctx, tx, tableName, exemplarData); err != nil {
		slog.Error("Failed to insert row", "error", err)
		return 0, err
	}

	if err := tx.Commit(); err != nil {
		slog.Error("Failed to commit transaction", "error", err)
		return 0, fmt.Errorf("commit tx: %w", err)
	}
	return 1, nil
}

func ensureBaseTable(ctx context.Context, db *sql.DB, table string) error {
	q := fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s ("%s" BIGINT);`,
		quoteIdent(table),
		"chq_timestamp",
	)
	if _, err := db.ExecContext(ctx, q); err != nil {
		return fmt.Errorf("create base table: %w", err)
	}
	return nil
}

// Uses the function form pragma_table_info(name) which returns columns:
// cid, name, type, notnull, dflt_value, pk
func getExistingColumns(ctx context.Context, db execQuerier, table string) (map[string]struct{}, error) {
	q := fmt.Sprintf(`SELECT name, type FROM pragma_table_info(%s);`, quoteStringLit(table))
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("pragma_table_info: %w", err)
	}
	defer func() { _ = rows.Close() }()

	cols := make(map[string]struct{})
	for rows.Next() {
		var name, ctype string
		if err := rows.Scan(&name, &ctype); err != nil {
			return nil, fmt.Errorf("scan pragma_table_info: %w", err)
		}
		cols[name] = struct{}{}
	}
	return cols, rows.Err()
}

// ensureColumnsForRow adds columns for keys present in row (except chq_timestamp),
// choosing DuckDB types based on Go types. Uses IF NOT EXISTS for safety.
func ensureColumnsForRow(ctx context.Context, db execQuerier, table string, row map[string]any, existing map[string]struct{}) error {
	// Deterministic order so ALTER statements are stable
	keys := make([]string, 0, len(row))
	for k := range row {
		if k == "chq_timestamp" {
			continue
		}
		if _, ok := existing[k]; ok {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		colType := duckTypeOf(row[k])
		alter := fmt.Sprintf(
			`ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s %s;`,
			quoteIdent(table),
			quoteIdent(k),
			colType,
		)
		if _, err := db.ExecContext(ctx, alter); err != nil {
			return fmt.Errorf("add column %q: %w", k, err)
		}
		existing[k] = struct{}{}
	}
	return nil
}

func insertRow(ctx context.Context, db execQuerier, table string, row map[string]any) (int64, error) {
	// Require timestamp
	ts, ok := row["chq_timestamp"]
	if !ok {
		return 0, fmt.Errorf("row missing %q", "chq_timestamp")
	}
	tsVal, err := toInt64(ts)
	if err != nil {
		return 0, fmt.Errorf("timestamp not int64-coercible: %v (%T)", ts, ts)
	}

	// Prepare stable column order: timestamp first, then sorted remaining keys
	keys := make([]string, 0, len(row))
	for k := range row {
		if k == "chq_timestamp" {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	cols := make([]string, 0, 1+len(keys))
	vals := make([]any, 0, 1+len(keys))
	placeholders := make([]string, 0, 1+len(keys))

	cols = append(cols, quoteIdent("chq_timestamp"))
	vals = append(vals, tsVal)
	placeholders = append(placeholders, "?")

	for _, k := range keys {
		cols = append(cols, quoteIdent(k))
		v, arg := normalizeValue(row[k])
		vals = append(vals, v)
		placeholders = append(placeholders, arg) // usually "?"
	}

	stmt := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s);`,
		quoteIdent(table),
		join(cols, ", "),
		join(placeholders, ", "),
	)

	res, err := db.ExecContext(ctx, stmt, vals...)
	if err != nil {
		return 0, fmt.Errorf("insert row: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("rows affected: %w", err)
	}
	return affected, nil
}

// normalizeValue converts map/slice to JSON text so we can bind it.
// Returns (value, placeholder) where placeholder is normally "?".
func normalizeValue(v any) (any, string) {
	if v == nil {
		return nil, "?"
	}
	switch x := v.(type) {
	case map[string]any, []any:
		b, _ := json.Marshal(x)
		return string(b), "?"
	case json.Number:
		// Prefer float64 for generality
		if f, err := x.Float64(); err == nil {
			return f, "?"
		}
		return string(x), "?"
	case time.Time:
		return x, "?"
	default:
		// Handle pointers transparently
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() {
			return normalizeValue(rv.Elem().Interface())
		}
		return v, "?"
	}
}

func duckTypeOf(v any) string {
	if v == nil {
		// If first seen as nil, default to TEXT to keep it permissive.
		return "TEXT"
	}
	switch v := v.(type) {
	case bool:
		return "BOOLEAN"
	case int8, int16, int32, int64, int:
		return "BIGINT"
	case uint8, uint16, uint32, uint64, uint:
		// To avoid surprises, store as BIGINT too (DuckDB supports UBIGINT,
		// but BIGINT is safer for mixed bindings).
		return "BIGINT"
	case float32, float64:
		return "DOUBLE"
	case json.Number:
		// Unknown width; use DOUBLE for safety.
		return "DOUBLE"
	case string:
		return "TEXT"
	case []byte:
		return "BLOB"
	case time.Time:
		return "TIMESTAMP"
	case map[string]any, []any:
		// Could be JSON type, but TEXT is simplest + always bindable.
		return "TEXT"
	default:
		// Unwrap pointers
		rv := reflect.ValueOf(v)
		if rv.Kind() == reflect.Ptr && !rv.IsNil() {
			return duckTypeOf(rv.Elem().Interface())
		}
		// Fallback
		return "TEXT"
	}
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case uint64:
		// best-effort
		return int64(x), nil
	case float64:
		return int64(x), nil
	case float32:
		return int64(x), nil
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, nil
		}
		if f, ferr := x.Float64(); ferr == nil {
			return int64(f), nil
		}
		return 0, fmt.Errorf("json.Number not int/float: %v", x)
	default:
		return 0, fmt.Errorf("unsupported ts type %T", v)
	}
}

// --- SQL small utils --------------------------------------------------------

type execQuerier interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func quoteIdent(ident string) string {
	// double-up any " inside identifier, wrap with "
	return `"` + stringReplaceAll(ident, `"`, `""`) + `"`
}

func quoteStringLit(s string) string {
	// single-quote literal for pragma_table_info('name')
	return `'` + stringReplaceAll(s, `'`, `''`) + `'`
}

func stringReplaceAll(s, old, new string) string {
	return string(bytes.ReplaceAll([]byte(s), []byte(old), []byte(new)))
}

func join(xs []string, sep string) string {
	switch len(xs) {
	case 0:
		return ""
	case 1:
		return xs[0]
	default:
		var b bytes.Buffer
		for i, s := range xs {
			if i > 0 {
				b.WriteString(sep)
			}
			b.WriteString(s)
		}
		return b.String()
	}
}

// ValidateResult is what the util returns for assertions in tests and for the API.
type ValidateResult struct {
	WorkerSQL    string      // the SQL that was executed
	StartMillis  int64       // resolved start placeholder
	EndMillis    int64       // resolved end placeholder
	InsertedRows int         // rows ingested from exemplar
	Rows         []rowstruct // rows returned from worker SQL
	IsAggregate  bool        // whether query was aggregate path (PromQL rewrite)
}

func ValidateLogQLAgainstExemplar(ctx context.Context, query string, exemplarData map[string]any, opts ...ValidateOption) (*ValidateResult, error) {
	cfg := validateConfig{
		table:        "logs",
		aggStep:      10 * time.Second,
		logLimit:     1000,
		logOrder:     "desc",
		extraOrderBy: nil,
		resolveRange: true,
		startMillis:  0,
		endMillis:    0,
		manageDB:     true,
	}
	for _, o := range opts {
		o(&cfg)
	}

	// 1) Parse to simplified AST (works for both pipeline and aggregate)
	ast, err := logql.FromLogQL(query)
	if err != nil {
		return nil, fmt.Errorf("parse logql: %w", err)
	}

	if err := ValidateEqualityMatcherRequirement(ast); err != nil {
		return nil, err
	}

	// 2) Open DuckDB (in-memory default)
	if cfg.db == nil {
		cfg.db, err = sql.Open("duckdb", "")
		if err != nil {
			return nil, fmt.Errorf("open duckdb: %w", err)
		}
		cfg.manageDB = true
	}
	if cfg.manageDB {
		defer func() { _ = cfg.db.Close() }()
	}

	// 3) Ingest exemplar rows
	inserted, err := IngestExemplarLogsJSONToDuckDB(ctx, cfg.db, cfg.table, exemplarData)
	if err != nil {
		return nil, fmt.Errorf("ingest exemplar: %w", err)
	}
	if inserted == 0 {
		return nil, fmt.Errorf("no rows ingested from exemplar")
	}

	// 4) Resolve [start,end] range
	var startMillis, endMillis int64
	if cfg.resolveRange {
		startMillis, endMillis, err = minMaxTimestamp(ctx, cfg.db, cfg.table)
		if err != nil {
			return nil, fmt.Errorf("resolve [start,end]: %w", err)
		}
		if endMillis == startMillis {
			endMillis = startMillis + 1
		}
	} else {
		startMillis, endMillis = cfg.startMillis, cfg.endMillis
	}

	// 5) Extract the first pipeline (selector + range) from the AST,
	//    regardless of aggregate/vector context.
	sel, rng, ok := ast.FirstPipeline()
	if !ok || sel == nil {
		return nil, fmt.Errorf("no log pipeline (selector) found in expression")
	}

	// 6) Build a synthetic leaf from that pipeline for validation.
	leaf := logql.LogLeaf{
		Matchers:     append([]logql.LabelMatch(nil), sel.Matchers...),
		LineFilters:  append([]logql.LineFilter(nil), sel.LineFilters...),
		LabelFilters: append([]logql.LabelFilter(nil), sel.LabelFilters...),
		Parsers:      append([]logql.ParserStage(nil), sel.Parsers...),

		Range:  "",
		Offset: "",
		Unwrap: false,
	}
	if rng != nil {
		leaf.Range = rng.Range
		leaf.Offset = rng.Offset
		leaf.Unwrap = rng.Unwrap
	}
	// Assign a stable ID (optional; helps determinism/debug)
	leaf.ID = leaf.Label()

	// 7) Stage-wise validation: matchers → +independent (incl. line filters) → +each parser (and parser’s label filters).
	stages, err := stageWiseValidation(
		cfg.db, cfg.table,
		leaf,
		startMillis, endMillis,
		cfg.logLimit,
		cfg.logOrder,
		cfg.extraOrderBy,
	)
	if err != nil {
		return nil, fmt.Errorf("stage-wise validation: %w", err)
	}
	if len(stages) == 0 {
		return nil, fmt.Errorf("stage-wise validator returned no stages")
	}

	final := stages[len(stages)-1]
	if !final.OK {
		// Surface a helpful error including missing fields, if any.
		if len(final.MissingFields) > 0 {
			return nil, fmt.Errorf("validation failed at final stage: missing fields: %v", final.MissingFields)
		}
		if final.Error != nil {
			return nil, fmt.Errorf("validation failed at final stage: %v", final.Error)
		}
		return nil, fmt.Errorf("validation failed at final stage")
	}
	if final.RowCount == 0 {
		return nil, fmt.Errorf("final stage returned 0 rows")
	}

	// 8) Fetch final rows for the response (mirrors old behavior)
	rows, err := queryAllRows(cfg.db, final.SQL)
	if err != nil {
		return nil, fmt.Errorf("execute final SQL: %w\nSQL was:\n%s", err, final.SQL)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no rows returned from final SQL; SQL was:\n%s", final.SQL)
	}

	// 9) Return a ValidateResult compatible with the existing HTTP handler.
	return &ValidateResult{
		WorkerSQL:    final.SQL, // final stage SQL (fully resolved; no placeholders)
		StartMillis:  startMillis,
		EndMillis:    endMillis,
		InsertedRows: inserted,
		Rows:         rows,
		IsAggregate:  ast.IsAggregateExpr(), // keep this info; we validated the pipeline inside it
	}, nil
}

// ValidateEqualityMatcherRequirement checks that the LogQL query has at least one equality matcher
func ValidateEqualityMatcherRequirement(ast logql.LogAST) error {
	// Find all log selectors in the AST (including nested ones)
	selectors := findAllLogSelectors(ast)

	if len(selectors) == 0 {
		return fmt.Errorf("no log selectors found in query")
	}

	// Check each selector for at least one equality matcher
	for i, selector := range selectors {
		foundAtleastOneEq := false
		for _, m := range selector.Matchers {
			foundAtleastOneEq = foundAtleastOneEq || m.Op == logql.MatchEq
		}
		if !foundAtleastOneEq {
			if len(selectors) == 1 {
				return fmt.Errorf("at least one equality matcher is required in selector")
			} else {
				return fmt.Errorf("at least one equality matcher is required in selector %d", i+1)
			}
		}
	}

	return nil
}

// findAllLogSelectors recursively finds all log selectors in the AST
func findAllLogSelectors(ast logql.LogAST) []logql.LogSelector {
	var selectors []logql.LogSelector

	switch ast.Kind {
	case logql.KindLogSelector:
		if ast.LogSel != nil {
			selectors = append(selectors, *ast.LogSel)
		}

	case logql.KindLogRange:
		if ast.LogRange != nil {
			selectors = append(selectors, ast.LogRange.Selector)
		}

	case logql.KindRangeAgg:
		if ast.RangeAgg != nil {
			// RangeAgg wraps a LogRange, so we need to check its selector
			selectors = append(selectors, ast.RangeAgg.Left.Selector)
		}

	case logql.KindVectorAgg:
		if ast.VectorAgg != nil {
			// VectorAgg wraps another expression, recurse into it
			selectors = append(selectors, findAllLogSelectors(ast.VectorAgg.Left)...)
		}

	case logql.KindBinOp:
		if ast.BinOp != nil {
			// BinOp has LHS and RHS, recurse into both
			selectors = append(selectors, findAllLogSelectors(ast.BinOp.LHS)...)
			selectors = append(selectors, findAllLogSelectors(ast.BinOp.RHS)...)
		}
	}

	return selectors
}

// ValidateStreamAttributeRequirement checks that the LogQL query has an equality matcher
// for the specified stream attribute in all log selectors
func ValidateStreamAttributeRequirement(ast logql.LogAST, streamAttribute string) error {
	if streamAttribute == "" {
		return nil
	}

	// Find all log selectors in the AST (including nested ones)
	selectors := findAllLogSelectors(ast)

	if len(selectors) == 0 {
		return fmt.Errorf("no log selectors found in query")
	}

	// Check each selector for the required stream attribute equality matcher
	for i, selector := range selectors {
		foundAttribute := false
		for _, m := range selector.Matchers {
			if m.Label == streamAttribute && m.Op == logql.MatchEq {
				foundAttribute = true
				break
			}
		}
		if !foundAttribute {
			if len(selectors) == 1 {
				return fmt.Errorf("stream attribute '%s' must be present as an equality matcher in selector", streamAttribute)
			}
			return fmt.Errorf("stream attribute '%s' must be present as an equality matcher in selector %d", streamAttribute, i+1)
		}
	}

	return nil
}

// findAllLogRanges recursively finds all log ranges in the AST
func findAllLogRanges(ast logql.LogAST) []logql.LogRange {
	var ranges []logql.LogRange

	switch ast.Kind {
	case logql.KindLogRange:
		if ast.LogRange != nil {
			ranges = append(ranges, *ast.LogRange)
		}

	case logql.KindRangeAgg:
		if ast.RangeAgg != nil {
			// RangeAgg wraps a LogRange
			ranges = append(ranges, ast.RangeAgg.Left)
		}

	case logql.KindVectorAgg:
		if ast.VectorAgg != nil {
			// VectorAgg wraps another expression, recurse into it
			ranges = append(ranges, findAllLogRanges(ast.VectorAgg.Left)...)
		}

	case logql.KindBinOp:
		if ast.BinOp != nil {
			// BinOp has LHS and RHS, recurse into both
			ranges = append(ranges, findAllLogRanges(ast.BinOp.LHS)...)
			ranges = append(ranges, findAllLogRanges(ast.BinOp.RHS)...)
		}
	}

	return ranges
}

// ValidateRangeSelector checks that all range selectors in the LogQL query
// match the expected duration calculated from the query time range
func ValidateRangeSelector(ast logql.LogAST, expectedDur time.Duration) error {
	ranges := findAllLogRanges(ast)

	// If there are no ranges in the query, no validation needed
	if len(ranges) == 0 {
		return nil
	}

	for i, logRange := range ranges {
		// Parse the range string (e.g., "5m") into time.Duration
		rangeDur, err := time.ParseDuration(logRange.Range)
		if err != nil {
			return fmt.Errorf("invalid range syntax '%s': %w", logRange.Range, err)
		}

		if rangeDur != expectedDur {
			// Format error message
			if len(ranges) == 1 {
				return fmt.Errorf("range selector [%s] must match query duration %s",
					logRange.Range, expectedDur)
			}
			return fmt.Errorf("range selector %d [%s] must match query duration %s",
				i+1, logRange.Range, expectedDur)
		}
	}

	return nil
}

func minMaxTimestamp(ctx context.Context, db *sql.DB, table string) (int64, int64, error) {
	q := fmt.Sprintf(`SELECT MIN("%s"), MAX("%s") FROM %s`,
		"chq_timestamp", "chq_timestamp", quoteIdent(table))
	var minimum sql.NullInt64
	var maximum sql.NullInt64
	if err := db.QueryRowContext(ctx, q).Scan(&minimum, &maximum); err != nil {
		return 0, 0, err
	}
	if !minimum.Valid || !maximum.Valid {
		return 0, 0, fmt.Errorf("no timestamps in table %s", table)
	}
	return minimum.Int64, maximum.Int64, nil
}

func replacePlaceholders(sqlText, table string, start, end int64) string {
	// Test exemplar data has both chq_timestamp (ms) and chq_tsns (ns) columns
	sqlText = strings.ReplaceAll(sqlText, "{table}", table)
	sqlText = strings.ReplaceAll(sqlText, "{start}", fmt.Sprintf("%d", start))
	sqlText = strings.ReplaceAll(sqlText, "{end}", fmt.Sprintf("%d", end))
	return sqlText
}

// ---- options ---------------------------------------------------------------

type validateConfig struct {
	db           *sql.DB
	manageDB     bool
	table        string
	aggStep      time.Duration
	logLimit     int
	logOrder     string
	extraOrderBy []string
	resolveRange bool
	startMillis  int64
	endMillis    int64
}

type ValidateOption func(*validateConfig)

// WithDB allows injecting a DB (e.g., for tests). If provided, caller manages its lifecycle.
func WithDB(db *sql.DB) ValidateOption {
	return func(c *validateConfig) {
		c.db = db
		c.manageDB = false
	}
}

// WithTable sets the target table name; default "logs".
func WithTable(name string) ValidateOption {
	return func(c *validateConfig) { c.table = name }
}

// WithAggStep sets the step used when building aggregate worker SQL; default 10s.
func WithAggStep(step time.Duration) ValidateOption {
	return func(c *validateConfig) { c.aggStep = step }
}

type rowstruct map[string]any

func queryAllRows(db *sql.DB, q string) ([]rowstruct, error) {
	rows, err := db.Query(q)
	if err != nil {
		return nil, fmt.Errorf("query failed: %v\nsql:\n%s", err, q)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("columns failed: %v", err)
	}

	var out []rowstruct
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("scan failed: %v", err)
		}
		m := make(rowstruct, len(cols))
		for i, c := range cols {
			m[c] = raw[i]
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows err: %v", err)
	}
	return out, nil
}
