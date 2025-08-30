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

package logql

import (
	"database/sql"
	"log/slog"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

func openDuckDB(t *testing.T) *sql.DB {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		slog.Error("Error opening duckdb for local parquet", "error", err.Error())
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExec(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec failed: %v\nsql:\n%s", err, q)
	}
}

type rowmap map[string]any

func queryAll(t *testing.T, db *sql.DB, q string) []rowmap {
	t.Helper()
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query failed: %v\nsql:\n%s", err, q)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns failed: %v", err)
	}

	var out []rowmap
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		m := make(rowmap, len(cols))
		for i, c := range cols {
			m[c] = raw[i]
		}
		out = append(out, m)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	return out
}

func replaceTable(sql string) string {
	return strings.ReplaceAll(sql, "{table}", "logs")
}

func getString(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case []byte:
		return string(x)
	default:
		return ""
	}
}

func TestToWorkerSQL_Regexp_ExtractOnly(t *testing.T) {
	db := openDuckDB(t)
	// base table
	mustExec(t, db, `CREATE TABLE logs("_cardinalhq.message" TEXT);`)
	// rows: INFO/alice, ERROR/bob, ERROR/carol
	mustExec(t, db, `INSERT INTO logs VALUES 
('ts=1 level=INFO user=alice msg="hello"'), 
('ts=2 level=ERROR user=bob msg="boom"'),
('ts=3 level=ERROR user=carol msg="warn"');`)

	leaf := LogLeaf{
		Parsers: []ParserStage{{
			Type: "regexp",
			Params: map[string]string{
				"pattern": `level=(?P<log_level>\w+).*user=(?P<username>\w+)`,
			},
		}},
	}

	rows := queryAll(t, db, replaceTable(leaf.ToWorkerSQL(0)))
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	// Verify extracted columns exist and look sane for each row
	// Note: DuckDB returns column names as provided; we projected log_level and username
	foundExtracts := 0
	for _, r := range rows {
		ll := getString(r["log_level"])
		un := getString(r["username"])
		if ll != "" && un != "" {
			foundExtracts++
		}
	}
	if foundExtracts != 3 {
		t.Fatalf("expected 3 rows to have extracted fields, got %d", foundExtracts)
	}
}

func TestToWorkerSQL_Regexp_WithFilters(t *testing.T) {
	db := openDuckDB(t)
	mustExec(t, db, `CREATE TABLE logs("_cardinalhq.message" TEXT);`)
	mustExec(t, db, `INSERT INTO logs VALUES 
('ts=1 level=INFO user=alice msg="hello"'), 
('ts=2 level=ERROR user=bob msg="boom"'),
('ts=3 level=ERROR user=carol msg="warn"');`)

	leaf := LogLeaf{
		Parsers: []ParserStage{{
			Type: "regexp",
			Params: map[string]string{
				"pattern": `level=(?P<log_level>\w+).*user=(?P<username>\w+)`,
			},
		}},
		LabelFilters: []LabelFilter{
			{Label: "log_level", Op: MatchEq, Value: "ERROR"},
			{Label: "username", Op: MatchRe, Value: "(alice|bob)"},
		},
	}

	rows := queryAll(t, db, replaceTable(leaf.ToWorkerSQL(0)))
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after filters, got %d", len(rows))
	}
	ll := getString(rows[0]["log_level"])
	un := getString(rows[0]["username"])
	if ll != "ERROR" || un != "bob" {
		t.Fatalf("wrong row selected: log_level=%q user=%q", ll, un)
	}
}

func TestToWorkerSQL_JSON_WithFilters(t *testing.T) {
	db := openDuckDB(t)
	mustExec(t, db, `CREATE TABLE logs("_cardinalhq.message" TEXT);`)
	mustExec(t, db, `INSERT INTO logs VALUES 
('{"level":"INFO","user":"alice","msg":"hello"}'),
('{"level":"ERROR","user":"bob","msg":"boom"}'),
('{"level":"ERROR","user":"carol","msg":"warn"}');`)

	leaf := LogLeaf{
		Parsers: []ParserStage{{
			Type:   "json",
			Params: map[string]string{},
		}},
		LabelFilters: []LabelFilter{
			{Label: "level", Op: MatchEq, Value: "ERROR"},
			{Label: "user", Op: MatchRe, Value: "(alice|bob)"},
		},
	}
	rows := queryAll(t, db, replaceTable(leaf.ToWorkerSQL(time.Second)))
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after filters, got %d", len(rows))
	}
	lv := getString(rows[0]["level"])
	us := getString(rows[0]["user"])
	if lv != "ERROR" || us != "bob" {
		t.Fatalf("wrong row selected: level=%q user=%q", lv, us)
	}
}

func TestToWorkerSQL_Logfmt_WithFilters(t *testing.T) {
	db := openDuckDB(t)
	mustExec(t, db, `CREATE TABLE logs("_cardinalhq.message" TEXT);`)
	// 1) status=200 user=bob      -> should match
	// 2) status=200 user=carol    -> filtered out by user regex
	// 3) status=500 user=alice    -> filtered out by status eq
	mustExec(t, db, `INSERT INTO logs VALUES
('ts=1 status=200 duration=15ms user=bob msg="ok"'),
('ts=2 status=200 duration=05ms user=carol msg="ok"'),
('ts=3 status=500 duration=20ms user=alice msg="err"');`)

	leaf := LogLeaf{
		Parsers: []ParserStage{{
			Type:   "logfmt",
			Params: map[string]string{},
		}},
		LabelFilters: []LabelFilter{
			{Label: "status", Op: MatchEq, Value: "200"},
			{Label: "user", Op: MatchRe, Value: "(alice|bob)"},
		},
	}

	rows := queryAll(t, db, replaceTable(leaf.ToWorkerSQL(0)))

	if len(rows) != 1 {
		t.Fatalf("expected 1 row after logfmt filters, got %d", len(rows))
	}
	st := getString(rows[0]["status"])
	us := getString(rows[0]["user"])
	if st != "200" || us != "bob" {
		t.Fatalf("wrong row selected after logfmt parsing: status=%q user=%q", st, us)
	}
}

func TestToWorkerSQL_MatchersOnly_FilterApplied(t *testing.T) {
	db := openDuckDB(t)
	mustExec(t, db, `CREATE TABLE logs(job TEXT, "_cardinalhq.message" TEXT);`)
	mustExec(t, db, `INSERT INTO logs(job, "_cardinalhq.message") VALUES
('my-app',  'hello a'),
('other',   'hello b');`)

	leaf := LogLeaf{
		Matchers: []LabelMatch{
			{Label: "job", Op: MatchEq, Value: "my-app"},
		},
	}

	sql := replaceTable(leaf.ToWorkerSQL(0))

	// Sanity: matcher should be present in SQL WHERE
	if !strings.Contains(sql, "job = 'my-app'") {
		t.Fatalf("generated SQL missing matcher WHERE: \n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after matcher filter, got %d\nsql:\n%s", len(rows), sql)
	}
	if got := getString(rows[0]["job"]); got != "my-app" {
		t.Fatalf("wrong row selected, job=%q", got)
	}
}

func TestToWorkerSQL_MatchersThenJSON_FilterApplied_FromLogQL(t *testing.T) {
	db := openDuckDB(t)
	mustExec(t, db, `CREATE TABLE logs(job TEXT, "_cardinalhq.message" TEXT);`)
	mustExec(t, db, `INSERT INTO logs(job, "_cardinalhq.message") VALUES
('my-app',  '{"level":"ERROR","user":"bob","msg":"boom"}'),
('my-app',  '{"level":"INFO","user":"alice","msg":"ok"}'),
('other',   '{"level":"ERROR","user":"carol","msg":"warn"}');`)

	// Matchers + parser + filter:
	//   {job="my-app"} | json | level="ERROR"
	q := `{job="my-app"} | json | level="ERROR"`

	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL error: %v", err)
	}
	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog error: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	replacedSql := replaceTable(leaf.ToWorkerSQL(0))

	// Sanity: both matcher and JSON extraction should show up
	if !strings.Contains(replacedSql, "job = 'my-app'") {
		t.Fatalf("generated SQL missing matcher WHERE:\n%s", replacedSql)
	}
	if !strings.Contains(replacedSql, "json_extract_string") {
		t.Fatalf("generated SQL missing json extraction:\n%s", replacedSql)
	}

	rows := queryAll(t, db, replacedSql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after matcher+json+filter, got %d\nsql:\n%s", len(rows), replacedSql)
	}

	if gotJob := getString(rows[0]["job"]); gotJob != "my-app" {
		t.Fatalf("wrong job, got %q", gotJob)
	}
	if gotLevel := getString(rows[0]["level"]); gotLevel != "ERROR" {
		t.Fatalf("wrong level, got %q", gotLevel)
	}
}

func TestToWorkerSQL_LabelFormat_Conditional_FromLogQL(t *testing.T) {
	db := openDuckDB(t)
	// Include a "job" column so selector matchers can be pushed down as WHERE.
	mustExec(t, db, `CREATE TABLE logs(job TEXT, "_cardinalhq.message" TEXT);`)

	// Three rows for job=my-app; 2 start with "Error", 1 is "OK".
	mustExec(t, db, `INSERT INTO logs VALUES
('my-app', '{"response":"ErrorBadGateway","msg":"x"}'),
('my-app', '{"response":"OK","msg":"y"}'),
('my-app', '{"response":"ErrorOops","msg":"z"}');`)

	// Full LogQL:
	//   - selector matcher: job="my-app"
	//   - json parser
	//   - filter referencing JSON field to ensure it is projected: response=~"(OK|Error.*)"
	//   - label_format creating "api" via conditional template
	//   - filter on the newly created label: api="ERROR"
	q := `{job="my-app"} | json | response=~"(OK|Error.*)" | label_format api=` +
		"`{{ if hasPrefix \"Error\" .response }}ERROR{{else}}{{.response}}{{end}}`" +
		` | api="ERROR"`

	// Parse & compile to a single leaf
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL error: %v", err)
	}
	plan, err := CompileLog(ast)
	if err != nil {
		t.Fatalf("CompileLog error: %v", err)
	}
	if len(plan.Leaves) != 1 {
		t.Fatalf("expected 1 leaf, got %d", len(plan.Leaves))
	}
	leaf := plan.Leaves[0]

	replacedSql := replaceTable(leaf.ToWorkerSQL(0))

	// Sanity checks on the generated SQL
	if !strings.Contains(replacedSql, `job = 'my-app'`) {
		t.Fatalf("generated SQL missing selector matcher WHERE clause:\n%s", replacedSql)
	}
	if !strings.Contains(replacedSql, `json_extract_string("_cardinalhq.message", '$.response') AS response`) {
		t.Fatalf("generated SQL missing JSON projection for 'response':\n%s", replacedSql)
	}
	// Should include CASE WHEN from template + starts_with(response, 'Error')
	if !strings.Contains(replacedSql, "CASE WHEN") || !strings.Contains(replacedSql, "starts_with(response, 'Error')") {
		t.Fatalf("generated SQL missing CASE WHEN/starts_with for label_format:\n%s", replacedSql)
	}
	// Should apply final filter api="ERROR"
	if !strings.Contains(replacedSql, `api = 'ERROR'`) {
		t.Fatalf("generated SQL missing final filter on derived label api:\n%s", replacedSql)
	}

	rows := queryAll(t, db, replacedSql)
	// Only rows whose response starts with "Error" should survive AND be mapped to api="ERROR".
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (Error* only), got %d\nsql:\n%s", len(rows), replacedSql)
	}

	type pair struct{ resp, api string }
	var got []pair
	for _, r := range rows {
		got = append(got, pair{
			resp: getString(r["response"]),
			api:  getString(r["api"]),
		})
	}

	for _, p := range got {
		if !strings.HasPrefix(p.resp, "Error") {
			t.Fatalf("unexpected non-Error response passed filter: response=%q api=%q", p.resp, p.api)
		}
		if p.api != "ERROR" {
			t.Fatalf("derived label mismatch: response=%q -> api=%q (want ERROR)", p.resp, p.api)
		}
	}
}
