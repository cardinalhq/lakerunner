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

// Replace {table} with a local base subquery that injects stub columns for
// timestamp/id/level/fingerprint so exemplar queries always have them available.
func replaceTable(sql string) string {
	base := `(SELECT *,
  0::BIGINT   AS "_cardinalhq.timestamp",
  ''::VARCHAR AS "_cardinalhq.id",
  ''::VARCHAR AS "_cardinalhq.level",
  ''::VARCHAR AS "_cardinalhq.fingerprint"
FROM logs) AS _t`

	// Substitute table and time placeholders.
	sql = strings.ReplaceAll(sql, "{table}", base)
	sql = strings.ReplaceAll(sql, "{start}", "0")
	sql = strings.ReplaceAll(sql, "{end}", "1") // [0,1) includes ts=0

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
	sql := replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc"))

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

	sql := replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc"))
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

	sql := replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc"))
	if !strings.Contains(sql, "AND true") {
		t.Fatalf("expected sentinel AND true in generated SQL:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	movieIds := []string{}
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

	rows := queryAll(t, db, replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc")))
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
	rows := queryAll(t, db, replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc")))
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

	rows := queryAll(t, db, replaceTable(leaf.ToWorkerSQL(0, "desc")))

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

	sql := replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc"))

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

	replacedSql := replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc"))

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

	replacedSql := replaceTable(leaf.ToWorkerSQLWithLimit(0, "desc"))

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
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues(10*time.Second, "pod")), 0, 5000)

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
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues(10*time.Second, "pod")), 0, 5000)

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
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues(10*time.Second, "pod")), 0, 5000)

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
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues(10*time.Second, "pod")), 0, 5000)

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
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues(10*time.Second, "user")), 0, 5000)

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
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues(10*time.Second, "user")), 0, 5000)

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
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues(10*time.Second, "user")), 0, 5000)

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
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagValues(10*time.Second, "user")), 0, 5000)

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
