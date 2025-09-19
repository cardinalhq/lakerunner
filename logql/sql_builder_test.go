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
	"fmt"
	"log/slog"
	"strings"
	"testing"

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
	defer func() { _ = rows.Close() }()

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

// Replace {table} with a local base subquery that injects stub columns for
// timestamp/id/level/fingerprint so exemplar queries always have them available.
func replaceTable(sql string) string {
	base := `(SELECT *,
  0::BIGINT   AS "_cardinalhq.timestamp",
  ''::VARCHAR AS "_cardinalhq.id",
  ''::VARCHAR AS "_cardinalhq.level",
  -4446492996171837732::BIGINT   AS "_cardinalhq.fingerprint"
FROM logs) AS _t`

	// Substitute table and time placeholders.
	sql = strings.ReplaceAll(sql, "{table}", base)

	return sql
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

// --- New: sanity that fingerprint is projected and queryable ---------------
func TestToWorkerSQL_Fingerprint_Present(t *testing.T) {
	db := openDuckDB(t)
	mustExec(t, db, `CREATE TABLE logs("_cardinalhq.message" TEXT);`)
	mustExec(t, db, `INSERT INTO logs VALUES ('hello'), ('world');`)

	leaf := LogLeaf{} // no parsers/filters; just pass-through with defaults
	sql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

	// Should include the sentinel so CacheManager can splice segment filter.
	if !strings.Contains(sql, "AND true") {
		t.Fatalf("expected sentinel AND true in generated SQL:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Verify the default columns exist, including fingerprint.
	for _, r := range rows {
		_ = getString(r["_cardinalhq.id"])
		_ = getString(r["_cardinalhq.level"])
		_ = getString(r["_cardinalhq.fingerprint"]) // must exist even if empty
	}
}

func TestToWorkerSQL_Fingerprint_AsString(t *testing.T) {
	db := openDuckDB(t)
	mustExec(t, db, `CREATE TABLE logs("_cardinalhq.message" TEXT);`)
	mustExec(t, db, `INSERT INTO logs VALUES ('hello'), ('world');`)

	leaf := LogLeaf{}
	sql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

	rows := queryAll(t, db, sql)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Verify that fingerprint is returned as a string (not int64)
	for i, r := range rows {
		fingerprint := r["_cardinalhq.fingerprint"]
		if fingerprint == nil {
			t.Fatalf("row %d: fingerprint is nil", i)
		}

		// Check that it's a string type
		switch v := fingerprint.(type) {
		case string:
			if v != "-4446492996171837732" {
				t.Fatalf("row %d: expected fingerprint to be '-4446492996171837732' (string), got %q", i, v)
			}
		case int64:
			t.Fatalf("row %d: fingerprint is still int64 (%d), should be string", i, v)
		default:
			t.Fatalf("row %d: fingerprint has unexpected type %T: %v", i, fingerprint, fingerprint)
		}
	}
}

// ---------------- Existing tests adjusted to use new replaceTable ----------
func replaceStartEnd(sql string, start, end int64) string {
	s := strings.ReplaceAll(sql, "{start}", fmt.Sprintf("%d", start))
	s = strings.ReplaceAll(s, "{end}", fmt.Sprintf("%d", end))
	return s
}

func mustDropTable(db *sql.DB, name string) {
	_, _ = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", name))
}

// Sets up a minimal logs table used by tag values tests.
func createLogsTable(t *testing.T, db *sql.DB) {
	t.Helper()
	mustDropTable(db, "logs")
	stmt := `CREATE TABLE logs(
		"_cardinalhq.timestamp" BIGINT,
		"_cardinalhq.id"       TEXT,
		"_cardinalhq.level"    TEXT,
		"_cardinalhq.message"  TEXT,
		"level"                TEXT,
		"service"              TEXT,
		"pod"                  TEXT,
		"user"                 TEXT
	);`
	mustExec(t, db, stmt)
}

func TestToWorkerSQL_Regexp_ExtractOnly(t *testing.T) {
	db := openDuckDB(t)
	// base table: only message is required; id/level/timestamp/fingerprint are stubbed in replaceTable(...)
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

	sql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)
	if !strings.Contains(sql, "AND true") {
		t.Fatalf("expected sentinel AND true in generated SQL:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
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

func TestToWorkerSQL_Regexp_NumericCompare_EmulateGTZero(t *testing.T) {
	db := openDuckDB(t)

	mustExec(t, db, `CREATE TABLE logs("_cardinalhq.message" TEXT);`)

	mustExec(t, db, `INSERT INTO logs VALUES
('Rolled new log segment in 1.25 ms'),
('Rolled new log segment in 0 s'),
('Some line without a duration'),
('Rolled new log segment in 8.5 ms'),
('Rolled new log segment in 0.000 s');`)

	leaf := LogLeaf{
		Parsers: []ParserStage{
			{
				Type: "regexp",
				Params: map[string]string{
					// Capture a numeric duration (integer or decimal) followed by a unit.
					"pattern": `(?P<dur>[0-9]+(?:\.[0-9]+)?)\s*(?:ns|us|µs|ms|s|m|h)`,
				},
				Filters: []LabelFilter{
					{Label: "dur", Op: MatchGt, Value: `0`},
				},
			},
		},
	}

	sql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 10_000)
	println(sql)
	if !strings.Contains(sql, "AND true") {
		t.Fatalf("expected sentinel AND true in generated SQL:\n%s", sql)
	}

	rows := queryAll(t, db, sql)

	// Expect exactly the two positive-duration rows to survive.
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows with dur > 0, got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}

	// Verify each surviving row has a non-zero 'dur' extracted.
	for _, r := range rows {
		durStr := getString(r["dur"])
		if durStr == "" {
			t.Fatalf("expected extracted 'dur' in row: %v", r)
		}
		// Quick guard against zeros by string check (keeps deps minimal).
		if durStr == "0" || durStr == "0.0" || durStr == "0.00" || durStr == "0.000" {
			t.Fatalf("unexpected zero duration passed filter: %v", r)
		}
	}
}

func TestToWorkerSQL_Regexp_ExtractWithGeneratedRegex(t *testing.T) {
	db := openDuckDB(t)
	mustExec(t, db, `CREATE TABLE logs("_cardinalhq.message" TEXT);`)
	mustExec(t, db, `INSERT INTO logs VALUES 
('Received playback request for movieId=XKTATT8'), 
('Received playback request for movieId=BIDGDS8'),
('Received playback request for movieId=IGNAJR8');`)

	leaf := LogLeaf{
		Parsers: []ParserStage{{
			Type: "regexp",
			Params: map[string]string{
				"pattern": `([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+)=(?P<movieId>[A-Za-z0-9]+)`,
			},
		}},
	}

	sql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)
	if !strings.Contains(sql, "AND true") {
		t.Fatalf("expected sentinel AND true in generated SQL:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	var movieIds []string
	for _, r := range rows {
		movieId := getString(r["movieId"])
		if movieId != "" {
			movieIds = append(movieIds, movieId)
		}
	}
	if len(movieIds) != 3 {
		t.Fatalf("expected 3 rows to have extracted movieIds, got %d", len(movieIds))
	}
	if movieIds[0] != "XKTATT8" || movieIds[1] != "BIDGDS8" || movieIds[2] != "IGNAJR8" {
		t.Fatalf("expected movieIds to be XKTATT8, BIDGDS8, IGNAJR8, got %v", movieIds)
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

	rows := queryAll(t, db, replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000))
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
	replacedSql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)
	rows := queryAll(t, db, replacedSql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after filters, got %d", len(rows))
	}
	lv := getString(rows[0]["level"])
	us := getString(rows[0]["user"])
	if lv != "ERROR" || us != "bob" {
		t.Fatalf("wrong row selected: level=%q user=%q", lv, us)
	}
}

func TestToWorkerSQL_JSON_WithNOFilters(t *testing.T) {
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
	}
	replacedSql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)
	rows := queryAll(t, db, replacedSql)

	k := rows[0]["__json"]
	if k == nil {
		t.Fatalf("expected __json to be present")
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

	rows := queryAll(t, db, replaceStartEnd(replaceTable(leaf.ToWorkerSQL(0, "desc", nil)), 0, 5000))

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

	sql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

	if !strings.Contains(sql, "job = 'my-app'") {
		t.Fatalf("generated SQL missing matcher WHERE: \n%s", sql)
	}
	if !strings.Contains(sql, "AND true") {
		t.Fatalf("expected sentinel AND true in generated SQL:\n%s", sql)
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

	replacedSql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

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
	mustExec(t, db, `CREATE TABLE logs(job TEXT, "_cardinalhq.message" TEXT);`)

	mustExec(t, db, `INSERT INTO logs VALUES
('my-app', '{"response":"ErrorBadGateway","msg":"x"}'),
('my-app', '{"response":"OK","msg":"y"}'),
('my-app', '{"response":"ErrorOops","msg":"z"}');`)

	q := `{job="my-app"} | json | response=~"(OK|Error.*)" | label_format api=` +
		"`{{ if hasPrefix \"Error\" .response }}ERROR{{else}}{{.response}}{{end}}`" +
		` | api="ERROR"`

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

	replacedSql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

	if !strings.Contains(replacedSql, `job = 'my-app'`) {
		t.Fatalf("generated SQL missing selector matcher WHERE clause:\n%s", replacedSql)
	}
	if !strings.Contains(replacedSql, `json_extract_string("_cardinalhq.message", '$.response') AS response`) {
		t.Fatalf("generated SQL missing JSON projection for 'response':\n%s", replacedSql)
	}
	if !strings.Contains(replacedSql, "CASE WHEN") || !strings.Contains(replacedSql, "starts_with(response, 'Error')") {
		t.Fatalf("generated SQL missing CASE WHEN/starts_with for label_format:\n%s", replacedSql)
	}
	if !strings.Contains(replacedSql, `api = 'ERROR'`) {
		t.Fatalf("generated SQL missing final filter on derived label api:\n%s", replacedSql)
	}

	rows := queryAll(t, db, replacedSql)
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

func TestToWorkerSQLForTagValues_Basic(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data with different tag values
	mustExec(t, db, `INSERT INTO logs VALUES
	 (0, 'id1', 'info', 'test message 1', 'info', 'api', 'pod1', NULL),
	 (1000, 'id2', 'error', 'test message 2', 'error', 'api', 'pod2', NULL),
	 (2000, 'id3', 'info', 'test message 3', 'info', 'web', 'pod1', NULL),
	 (3000, 'id4', 'debug', 'test message 4', 'debug', 'api', 'pod3', NULL),
	 (4000, 'id5', 'info', 'test message 5', 'info', 'web', 'pod2', NULL)`)

	// Test basic tag values query
	be := &LogLeaf{}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues("pod")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return distinct pod values
	if len(rows) != 3 {
		t.Fatalf("expected 3 distinct pod values, got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	expected := map[string]bool{"pod1": true, "pod2": true, "pod3": true}
	for k, v := range expected {
		if !got[k] {
			t.Fatalf("missing expected pod value: %s", k)
		}
		if !v {
			t.Fatalf("unexpected pod value: %s", k)
		}
	}
}

func TestToWorkerSQLForTagValues_WithMatchers(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data with different tag values
	mustExec(t, db, `INSERT INTO logs VALUES
	 (0, 'id1', 'info', 'test message 1', 'info', 'api', 'pod1', NULL),
	 (1000, 'id2', 'error', 'test message 2', 'error', 'api', 'pod2', NULL),
	 (2000, 'id3', 'info', 'test message 3', 'info', 'web', 'pod1', NULL),
	 (3000, 'id4', 'debug', 'test message 4', 'debug', 'api', 'pod3', NULL),
	 (4000, 'id5', 'info', 'test message 5', 'info', 'web', 'pod2', NULL)`)

	// Test with matchers - only get pod values for service=api
	be := &LogLeaf{
		Matchers: []LabelMatch{
			{Label: "service", Op: MatchEq, Value: "api"},
		},
	}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues("pod")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return only pod values for service=api
	if len(rows) != 3 {
		t.Fatalf("expected 3 distinct pod values for service=api, got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	expected := map[string]bool{"pod1": true, "pod2": true, "pod3": true}
	for k, v := range expected {
		if !got[k] {
			t.Fatalf("missing expected pod value: %s", k)
		}
		if !v {
			t.Fatalf("unexpected pod value: %s", k)
		}
	}
}

func TestToWorkerSQLForTagValues_WithLineFilters(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data with different tag values
	mustExec(t, db, `INSERT INTO logs VALUES
	 (0, 'id1', 'info', 'error occurred', 'info', 'api', 'pod1', NULL),
	 (1000, 'id2', 'error', 'test message', 'error', 'api', 'pod2', NULL),
	 (2000, 'id3', 'info', 'error occurred', 'info', 'web', 'pod1', NULL),
	 (3000, 'id4', 'debug', 'test message', 'debug', 'api', 'pod3', NULL),
	 (4000, 'id5', 'info', 'error occurred', 'info', 'web', 'pod2', NULL)`)

	// Test with line filters - only get pod values for messages containing "error"
	be := &LogLeaf{
		LineFilters: []LineFilter{
			{Op: LineContains, Match: "error"},
		},
	}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues("pod")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return only pod values for messages containing "error"
	if len(rows) != 2 {
		t.Fatalf("expected 2 distinct pod values for messages containing 'error', got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	expected := map[string]bool{"pod1": true, "pod2": true}
	for k, v := range expected {
		if !got[k] {
			t.Fatalf("missing expected pod value: %s", k)
		}
		if !v {
			t.Fatalf("unexpected pod value: %s", k)
		}
	}
}

func TestToWorkerSQLForTagValues_WithLabelFilters(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data with different tag values
	mustExec(t, db, `INSERT INTO logs VALUES
	 (0, 'id1', 'info', 'test message 1', 'info', 'api', 'pod1', NULL),
	 (1000, 'id2', 'error', 'test message 2', 'error', 'api', 'pod2', NULL),
	 (2000, 'id3', 'info', 'test message 3', 'info', 'web', 'pod1', NULL),
	 (3000, 'id4', 'debug', 'test message 4', 'debug', 'api', 'pod3', NULL),
	 (4000, 'id5', 'info', 'test message 5', 'info', 'web', 'pod2', NULL)`)

	// Test with label filters - only get pod values for level=info
	be := &LogLeaf{
		LabelFilters: []LabelFilter{
			{Label: "level", Op: MatchEq, Value: "info"},
		},
	}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues("pod")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return only pod values for level=info
	if len(rows) != 2 {
		t.Fatalf("expected 2 distinct pod values for level=info, got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	expected := map[string]bool{"pod1": true, "pod2": true}
	for k, v := range expected {
		if !got[k] {
			t.Fatalf("missing expected pod value: %s", k)
		}
		if !v {
			t.Fatalf("unexpected pod value: %s", k)
		}
	}
}

func TestToWorkerSQLForTagValues_WithRegexpParser(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data with messages that can be parsed by regexp
	mustExec(t, db, `INSERT INTO logs VALUES
	 (0, 'id1', 'info', 'user=alice action=login', 'info', NULL, NULL, NULL),
	 (1000, 'id2', 'info', 'user=bob action=logout', 'info', NULL, NULL, NULL),
	 (2000, 'id3', 'error', 'user=charlie action=login', 'error', NULL, NULL, NULL),
	 (3000, 'id4', 'info', 'user=alice action=view', 'info', NULL, NULL, NULL),
	 (4000, 'id5', 'debug', 'user=david action=login', 'debug', NULL, NULL, NULL)`)

	// Test with regexp parser to extract user field
	be := &LogLeaf{
		Parsers: []ParserStage{
			{
				Type: "regexp",
				Params: map[string]string{
					"pattern": "user=(?P<user>\\w+)",
				},
			},
		},
	}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues("user")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return distinct user values extracted by regexp
	if len(rows) != 4 {
		t.Fatalf("expected 4 distinct user values, got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	expected := map[string]bool{"alice": true, "bob": true, "charlie": true, "david": true}
	for k, v := range expected {
		if !got[k] {
			t.Fatalf("missing expected user value: %s", k)
		}
		if !v {
			t.Fatalf("unexpected user value: %s", k)
		}
	}
}

func TestToWorkerSQLForTagValues_WithJSONParser(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data with JSON messages
	mustExec(t, db, `INSERT INTO logs VALUES
	 (0, 'id1', 'info', '{"user":"alice","action":"login"}', 'info', NULL, NULL, NULL),
	 (1000, 'id2', 'info', '{"user":"bob","action":"logout"}', 'info', NULL, NULL, NULL),
	 (2000, 'id3', 'error', '{"user":"charlie","action":"login"}', 'error', NULL, NULL, NULL),
	 (3000, 'id4', 'info', '{"user":"alice","action":"view"}', 'info', NULL, NULL, NULL),
	 (4000, 'id5', 'debug', '{"user":"david","action":"login"}', 'debug', NULL, NULL, NULL)`)

	// Test with JSON parser to extract user field
	be := &LogLeaf{
		Parsers: []ParserStage{
			{
				Type: "json",
				Params: map[string]string{
					"user": "user",
				},
			},
		},
	}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues("user")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return distinct user values extracted by JSON parser
	if len(rows) != 4 {
		t.Fatalf("expected 4 distinct user values, got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	expected := map[string]bool{"alice": true, "bob": true, "charlie": true, "david": true}
	for k, v := range expected {
		if !got[k] {
			t.Fatalf("missing expected user value: %s", k)
		}
		if !v {
			t.Fatalf("unexpected user value: %s", k)
		}
	}
}

func TestToWorkerSQLForTagValues_WithLogfmtParser(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data with logfmt messages
	mustExec(t, db, `INSERT INTO logs VALUES
	 (0, 'id1', 'info', 'user=alice action=login', 'info', NULL, NULL, NULL),
	 (1000, 'id2', 'info', 'user=bob action=logout', 'info', NULL, NULL, NULL),
	 (2000, 'id3', 'error', 'user=charlie action=login', 'error', NULL, NULL, NULL),
	 (3000, 'id4', 'info', 'user=alice action=view', 'info', NULL, NULL, NULL),
	 (4000, 'id5', 'debug', 'user=david action=login', 'debug', NULL, NULL, NULL)`)

	// Test with logfmt parser to extract user field
	be := &LogLeaf{
		Parsers: []ParserStage{
			{
				Type: "logfmt",
				Params: map[string]string{
					"user": "user",
				},
			},
		},
	}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues("user")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return distinct user values extracted by logfmt parser
	if len(rows) != 4 {
		t.Fatalf("expected 4 distinct user values, got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	expected := map[string]bool{"alice": true, "bob": true, "charlie": true, "david": true}
	for k, v := range expected {
		if !got[k] {
			t.Fatalf("missing expected user value: %s", k)
		}
		if !v {
			t.Fatalf("unexpected user value: %s", k)
		}
	}
}

func TestToWorkerSQLForTagValues_ComplexQuery(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data with complex structure
	mustExec(t, db, `INSERT INTO logs VALUES
	 (0, 'id1', 'info', '{"user":"alice","action":"login","status":"success","level":"info"}', 'info', 'api', NULL, NULL),
	 (1000, 'id2', 'error', '{"user":"bob","action":"login","status":"failed","level":"error"}', 'error', 'api', NULL, NULL),
	 (2000, 'id3', 'info', '{"user":"alice","action":"view","status":"success","level":"info"}', 'info', 'web', NULL, NULL),
	 (3000, 'id4', 'debug', '{"user":"charlie","action":"login","status":"success","level":"debug"}', 'debug', 'api', NULL, NULL),
	 (4000, 'id5', 'info', '{"user":"david","action":"logout","status":"success","level":"info"}', 'info', 'web', NULL, NULL)`)

	// Test complex query with matchers, line filters, and JSON parser
	be := &LogLeaf{
		Matchers: []LabelMatch{
			{Label: "service", Op: MatchEq, Value: "api"},
		},
		LineFilters: []LineFilter{
			{Op: LineContains, Match: "login"},
		},
		Parsers: []ParserStage{
			{
				Type: "json",
				Params: map[string]string{
					"user": "user",
				},
			},
		},
		LabelFilters: []LabelFilter{
			{Label: "level", Op: MatchEq, Value: "info"},
		},
	}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues("user")), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return only user values for service=api, messages containing "login", and level=info
	// This should only match the first record: alice
	if len(rows) != 1 {
		t.Fatalf("expected 1 distinct user value, got %d\nsql:\n%s", len(rows), sql)
	}

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	expected := map[string]bool{"alice": true}
	for k, v := range expected {
		if !got[k] {
			t.Fatalf("missing expected user value: %s", k)
		}
		if !v {
			t.Fatalf("unexpected user value: %s", k)
		}
	}
}

// --- Tests for ToWorkerSQLWithLimit fields parameter ---

func TestToWorkerSQLWithLimit_Fields_Basic(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data
	mustExec(t, db, `INSERT INTO logs VALUES
	 (1000, 'id1', 'info', 'test message 1', 'info', 'api', 'pod1', 'user1'),
	 (2000, 'id2', 'error', 'test message 2', 'error', 'web', 'pod2', 'user2'),
	 (3000, 'id3', 'debug', 'test message 3', 'debug', 'api', 'pod3', 'user3')`)

	// Test with fields parameter - use matchers to ensure fields are projected
	be := &LogLeaf{
		Matchers: []LabelMatch{
			{Label: "service", Op: MatchEq, Value: "api"},
		},
	}
	fields := []string{"service", "pod", "user"}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLWithLimit(0, "desc", fields)), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return only rows where service=api (2 rows)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (service=api), got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify that the specified fields are present in the results
	for i, row := range rows {
		service := getString(row["service"])
		pod := getString(row["pod"])
		user := getString(row["user"])

		// Check that fields are not empty (they should have values from our test data)
		if service == "" || pod == "" || user == "" {
			t.Fatalf("row %d missing expected field values: service=%q pod=%q user=%q", i, service, pod, user)
		}

		// Verify we get the expected values (only api service rows)
		// Note: ordered by timestamp DESC, so pod3 (3000) comes before pod1 (1000)
		expectedServices := []string{"api", "api"}
		expectedPods := []string{"pod3", "pod1"}
		expectedUsers := []string{"user3", "user1"}

		if service != expectedServices[i] {
			t.Fatalf("row %d: expected service=%q, got %q", i, expectedServices[i], service)
		}
		if pod != expectedPods[i] {
			t.Fatalf("row %d: expected pod=%q, got %q", i, expectedPods[i], pod)
		}
		if user != expectedUsers[i] {
			t.Fatalf("row %d: expected user=%q, got %q", i, expectedUsers[i], user)
		}
	}
}

func TestToWorkerSQLWithLimit_Fields_WithRegexpParser(t *testing.T) {
	db := openDuckDB(t)
	mustExec(t, db, `CREATE TABLE logs("_cardinalhq.message" TEXT);`)
	mustExec(t, db, `INSERT INTO logs VALUES 
	('user=alice action=login status=success'),
	('user=bob action=logout status=success'),
	('user=charlie action=view status=pending');`)

	// Test with regexp parser and fields parameter
	be := &LogLeaf{
		Parsers: []ParserStage{{
			Type: "regexp",
			Params: map[string]string{
				"pattern": `user=(?P<user>\w+).*action=(?P<action>\w+).*status=(?P<status>\w+)`,
			},
		}},
	}

	// Test with fields that are extracted by the regexp parser
	fields := []string{"user", "action", "status"}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLWithLimit(0, "desc", fields)), 0, 5000)
	rows := queryAll(t, db, sql)

	// Should return all rows with the extracted fields
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify that the extracted fields are present and correct
	expectedUsers := []string{"alice", "bob", "charlie"}
	expectedActions := []string{"login", "logout", "view"}
	expectedStatuses := []string{"success", "success", "pending"}

	for i, row := range rows {
		user := getString(row["user"])
		action := getString(row["action"])
		status := getString(row["status"])

		if user != expectedUsers[i] {
			t.Fatalf("row %d: expected user=%q, got %q", i, expectedUsers[i], user)
		}
		if action != expectedActions[i] {
			t.Fatalf("row %d: expected action=%q, got %q", i, expectedActions[i], action)
		}
		if status != expectedStatuses[i] {
			t.Fatalf("row %d: expected status=%q, got %q", i, expectedStatuses[i], status)
		}
	}
}

func TestToWorkerSQLWithLimit_Fields_EmptyFields(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data
	mustExec(t, db, `INSERT INTO logs VALUES
	 (1000, 'id1', 'info', 'test message 1', 'info', 'api', 'pod1', 'user1'),
	 (2000, 'id2', 'error', 'test message 2', 'error', 'web', 'pod2', 'user2')`)

	// Test with empty fields parameter
	be := &LogLeaf{}
	fields := []string{}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLWithLimit(0, "desc", fields)), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return all rows (no additional fields added)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify that basic columns are still present
	for i, row := range rows {
		message := getString(row["_cardinalhq.message"])
		if message == "" {
			t.Fatalf("row %d missing _cardinalhq.message", i)
		}
	}
}

func TestToWorkerSQLWithLimit_Fields_NonExistentFields(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data
	mustExec(t, db, `INSERT INTO logs VALUES
	 (1000, 'id1', 'info', 'test message 1', 'info', 'api', 'pod1', 'user1')`)

	// Test with non-existent fields parameter - this should fail at the SQL level
	be := &LogLeaf{}
	fields := []string{"nonexistent_field1", "nonexistent_field2"}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLWithLimit(0, "desc", fields)), 0, 5000)

	// This test expects the query to fail because the fields don't exist
	_, err := db.Query(sql)
	if err == nil {
		t.Fatalf("expected query to fail with non-existent fields, but it succeeded\nsql:\n%s", sql)
	}

	// Verify the error message indicates the field is not found
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected 'not found' error, got: %v", err)
	}
}

func TestToWorkerSQLWithLimit_Fields_WithLimit(t *testing.T) {
	db := openDuckDB(t)
	createLogsTable(t, db)

	// Insert test data
	mustExec(t, db, `INSERT INTO logs VALUES
	 (1000, 'id1', 'info', 'test message 1', 'info', 'api', 'pod1', 'user1'),
	 (2000, 'id2', 'error', 'test message 2', 'error', 'web', 'pod2', 'user2'),
	 (3000, 'id3', 'debug', 'test message 3', 'debug', 'api', 'pod3', 'user3'),
	 (4000, 'id4', 'info', 'test message 4', 'info', 'web', 'pod4', 'user4')`)

	// Test with fields parameter and limit - use matchers to ensure fields are projected
	be := &LogLeaf{
		Matchers: []LabelMatch{
			{Label: "service", Op: MatchEq, Value: "api"},
		},
	}
	fields := []string{"service", "pod"}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLWithLimit(2, "desc", fields)), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return only 2 rows due to limit (and service=api filter)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows due to limit and service=api filter, got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify that the fields are present in the limited results
	for i, row := range rows {
		service := getString(row["service"])
		pod := getString(row["pod"])

		if service == "" || pod == "" {
			t.Fatalf("row %d missing expected field values: service=%q pod=%q", i, service, pod)
		}

		// Verify service is api (due to matcher)
		if service != "api" {
			t.Fatalf("row %d: expected service=api, got %q", i, service)
		}
	}
}

func TestToWorkerSQL_LineFormat_JSONToMessage(t *testing.T) {
	db := openDuckDB(t)

	mustExec(t, db, `CREATE TABLE logs(app TEXT, level TEXT, "_cardinalhq.message" TEXT);`)
	// Note: include "level" in the JSON body because the filter runs AFTER the json stage
	mustExec(t, db, `INSERT INTO logs(app, level, "_cardinalhq.message") VALUES
        ('web',   'ERROR', '{"message":"boom","level":"ERROR","x":1}'),
        ('web',   'INFO',  '{"message":"ok","level":"INFO","x":2}'),
        ('other', 'ERROR', '{"message":"nope","level":"ERROR","x":3}')`)

	q := `{app="web"} | json msg="message" | line_format "{{.msg}}" | level="ERROR"`

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

	sql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 10_000)

	// sanity checks (optional)
	if !strings.Contains(sql, "app = 'web'") {
		t.Fatalf("missing app filter:\n%s", sql)
	}
	if !strings.Contains(sql, "json_extract_string(\"_cardinalhq.message\", '$.message') AS msg") &&
		!strings.Contains(sql, "json_extract_string(\"_cardinalhq.message\",'$.message') AS msg") {
		t.Fatalf("missing msg JSON projection:\n%s", sql)
	}
	if !strings.Contains(sql, `level = 'ERROR'`) {
		t.Fatalf("missing level filter:\n%s", sql)
	}
	if !strings.Contains(strings.ToLower(sql), `as "_cardinalhq.message"`) {
		t.Fatalf("expected rewrite of _cardinalhq.message:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (app=web & json.level=ERROR), got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}
	if got := getString(rows[0][`_cardinalhq.message`]); got != "boom" {
		t.Fatalf(`rewritten _cardinalhq.message mismatch: got %q, want "boom"`, got)
	}
}

func TestToWorkerSQL_LineFormat_IndexBaseField(t *testing.T) {
	db := openDuckDB(t)
	t.Cleanup(func() { mustDropTable(db, "logs") })

	// Base table with:
	// - selector column "resource.service.name" (because LogQL normalizes resource_service_name)
	// - top-level base column with special char: "log.@OrderResult"
	// - message column to be rewritten
	mustExec(t, db, `CREATE TABLE logs(
		"resource.service.name" TEXT,
		"_cardinalhq.message"   TEXT,
		"log.@OrderResult"      TEXT
	);`)

	// Insert 2 rows: one matches selector, the other does not.
	mustExec(t, db, `INSERT INTO logs VALUES
		('accounting', 'orig msg 1', 'SUCCESS'),
		('billing',    'orig msg 2', 'IGNORED');`)

	// Your intended query:
	// - matcher on resource_service_name (normalized to "resource.service.name")
	// - label_format derives "order_result" from top-level base column via index
	// - line_format turns that into the new _cardinalhq.message
	q := `{resource_service_name="accounting"} ` +
		`| label_format order_result=` + "`{{ index . \"log.@OrderResult\" }}`" + ` ` +
		`| line_format "{{ .order_result }}"`

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

	sql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 10_000)

	// Sanity checks: selector WHERE, dependency hoist, and message rewrite present
	if !strings.Contains(sql, `"resource.service.name" = 'accounting'`) {
		t.Fatalf("missing selector on resource.service.name:\n%s", sql)
	}
	if !strings.Contains(sql, `"log.@OrderResult"`) {
		t.Fatalf("expected hoisted base column \"log.@OrderResult\" in SQL:\n%s", sql)
	}
	if !strings.Contains(strings.ToLower(sql), `as "_cardinalhq.message"`) {
		t.Fatalf("expected rewrite of _cardinalhq.message via line_format:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (service=accounting), got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}

	// The new message should be the value of "log.@OrderResult" (SUCCESS)
	gotMsg := getString(rows[0][`_cardinalhq.message`])
	if gotMsg != "SUCCESS" {
		t.Fatalf(`rewritten _cardinalhq.message mismatch: got %q, want "SUCCESS"`, gotMsg)
	}

	// And the losing row (billing) should not appear
	if svc := getString(rows[0][`resource.service.name`]); svc != "accounting" {
		t.Fatalf("wrong row selected: resource.service.name=%q", svc)
	}
}

func TestToWorkerSQL_LineFormat_DirectIndexBaseField(t *testing.T) {
	db := openDuckDB(t)
	t.Cleanup(func() { mustDropTable(db, "logs") })

	// Base table:
	mustExec(t, db, `CREATE TABLE logs(
		"resource.service.name" TEXT,
		"_cardinalhq.message"   TEXT,
		"log.@OrderResult"      TEXT
	);`)

	// Two rows; only one should match the selector.
	mustExec(t, db, `INSERT INTO logs VALUES
		('accounting', 'orig msg 1', 'SUCCESS'),
		('billing',    'orig msg 2', 'IGNORED');`)

	// Query:
	//  - selector on resource_service_name (normalized to "resource.service.name")
	//  - line_format directly uses index to access base field with special chars
	q := `{resource_service_name="accounting"} | line_format "{{ index . \"log.@OrderResult\" }}"`

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

	sql := replaceStartEnd(replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc", nil)), 0, 10_000)

	// Sanity: selector, hoisted dep, and message rewrite present.
	if !strings.Contains(sql, `"resource.service.name" = 'accounting'`) {
		t.Fatalf("missing selector on resource.service.name:\n%s", sql)
	}
	if !strings.Contains(sql, `"log.@OrderResult"`) {
		t.Fatalf("expected hoisted base column \"log.@OrderResult\" in SQL:\n%s", sql)
	}
	if !strings.Contains(strings.ToLower(sql), `as "_cardinalhq.message"`) {
		t.Fatalf("expected rewrite of _cardinalhq.message via line_format:\n%s", sql)
	}

	// Execute and validate.
	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (service=accounting), got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}

	gotMsg := getString(rows[0][`_cardinalhq.message`])
	if gotMsg != "SUCCESS" {
		t.Fatalf(`rewritten _cardinalhq.message mismatch: got %q, want "SUCCESS"`, gotMsg)
	}
	if svc := getString(rows[0][`resource.service.name`]); svc != "accounting" {
		t.Fatalf("wrong row selected: resource.service.name=%q", svc)
	}
}
