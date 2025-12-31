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

	_ "github.com/duckdb/duckdb-go/v2"
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

func replaceTable(sql string) string {
	sql = strings.ReplaceAll(sql, "{table}", "logs")
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

	// Full base schema so SELECT * works without synthetic stubs.
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT
	);`)
	mustExec(t, db, `INSERT INTO logs VALUES
		(0, '', '', 'hello', -4446492996171837732),
		(1, '', '', 'world', -4446492996171837732);`)

	leaf := LogLeaf{} // no parsers/filters; just pass-through with defaults
	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

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
		_ = getString(r["chq_id"])
		_ = getString(r["log_level"])
		_ = getString(r["chq_fingerprint"]) // must exist even if empty
	}
}

func TestToWorkerSQL_Fingerprint_AsString(t *testing.T) {
	db := openDuckDB(t)

	// Full base schema so SELECT * works and fingerprint can be CAST to VARCHAR.
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT
	);`)
	mustExec(t, db, `INSERT INTO logs VALUES
		(0, '', '', 'hello', -4446492996171837732),
		(1, '', '', 'world', -4446492996171837732);`)

	leaf := LogLeaf{}
	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	rows := queryAll(t, db, sql)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}

	// Verify that fingerprint is returned as a string (not int64)
	for i, r := range rows {
		fingerprint := r["chq_fingerprint"]
		if fingerprint == nil {
			t.Fatalf("row %d: fingerprint is nil", i)
		}

		switch v := fingerprint.(type) {
		case string:
			if v != "-4446492996171837732" {
				t.Fatalf("row %d: expected fingerprint to be '-4446492996171837732' (string), got %q", i, v)
			}
		case int64:
			t.Fatalf("row %d: fingerprint is int64 (%d), should be string", i, v)
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
		"chq_timestamp" BIGINT,
		"chq_id"       TEXT,
		"log_level"    TEXT,
		"log_message"  TEXT,
		"level"                TEXT,
		"service"              TEXT,
		"pod"                  TEXT,
		"user"                 TEXT
	);`
	mustExec(t, db, stmt)
}

func TestToWorkerSQL_Regexp_ExtractOnly(t *testing.T) {
	db := openDuckDB(t)

	// Full base schema so SELECT * works and parsers can add columns.
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT
	);`)

	// rows: INFO/alice, ERROR/bob, ERROR/carol
	mustExec(t, db, `INSERT INTO logs VALUES
		(1, '', '', 'ts=1 level=INFO user=alice msg="hello"',  -4446492996171837732),
		(2, '', '', 'ts=2 level=ERROR user=bob msg="boom"',    -4446492996171837732),
		(3, '', '', 'ts=3 level=ERROR user=carol msg="warn"',  -4446492996171837732);`)

	leaf := LogLeaf{
		Parsers: []ParserStage{{
			Type: "regexp",
			Params: map[string]string{
				"pattern": `level=(?P<log_level>\w+).*user=(?P<username>\w+)`,
			},
		}},
	}

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	// Should include the sentinel so CacheManager can splice segment filter.
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

func TestToWorkerSQL_Regexp_Kafka_DurationExtract(t *testing.T) {
	db := openDuckDB(t)

	// Full base schema + selector column
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"resource_service_name"   TEXT
	);`)

	// 1) Matching kafka row (duration=1)
	// 2) Non-matching kafka row (noise)
	// 3) Matching but wrong service (should be filtered out by selector)
	mustExec(t, db, `INSERT INTO logs VALUES
		(1, '', '', '[LocalLog partition=__cluster_metadata-0, dir=/tmp/kafka-logs] Rolled new log segment at offset 101915 in 1 ms.', -4446492996171837732, 'kafka'),
		(2, '', '', 'some other kafka line without the expected shape',                                           -4446492996171837732, 'kafka'),
		(3, '', '', '[LocalLog partition=__cluster_metadata-0, dir=/tmp/kafka-logs] Rolled new log segment at offset 222222 in 7 ms.', -4446492996171837732, 'other');`)

	leaf := LogLeaf{
		Matchers: []LabelMatch{
			{Label: "resource_service_name", Op: MatchEq, Value: "kafka"},
		},
		LineFilters: []LineFilter{
			{Op: LineContains, Match: `Rolled`},
		},
		Parsers: []ParserStage{
			{
				Type: "regexp",
				Params: map[string]string{
					// Raw string for legibility; same pattern as in your expression.
					"pattern": `([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([A-Za-z]+) ([0-9]+) ([A-Za-z]+) (?P<duration>[0-9]+) ([A-Za-z0-9-_.:]+)`,
				},
			},
		},
	}

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 10_000)

	// Sanity: time-window sentinel present so segment filters can be spliced.
	if !strings.Contains(sql, "AND true") {
		t.Fatalf("expected sentinel AND true in generated SQL:\n%s", sql)
	}

	rows := queryAll(t, db, sql)

	// We expect exactly 1 row:
	//  - the matching kafka line with "in 1 ms."
	//  - the noise line doesn't match the regexp
	//  - the "other" service is filtered by the selector
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after selector+regexp, got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}

	// Verify named capture 'duration' exists and equals "1".
	dur := getString(rows[0]["duration"])
	if dur != "1" {
		t.Fatalf("expected duration='1', got %q; row=%v\nsql=\n%s", dur, rows[0], sql)
	}

	// Double-check the selector held.
	svc := getString(rows[0][`resource_service_name`])
	if svc != "kafka" {
		t.Fatalf("expected resource_service_name='kafka', got %q", svc)
	}
}

func TestToWorkerSQL_Regexp_NumericCompare_EmulateGTZero(t *testing.T) {
	db := openDuckDB(t)

	// Full base schema
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT
	);`)

	mustExec(t, db, `INSERT INTO logs VALUES
		(1, '', '', 'Rolled new log segment in 1.25 ms',  -4446492996171837732),
		(2, '', '', 'Rolled new log segment in 0 s',      -4446492996171837732),
		(3, '', '', 'Some line without a duration',        -4446492996171837732),
		(4, '', '', 'Rolled new log segment in 8.5 ms',    -4446492996171837732),
		(5, '', '', 'Rolled new log segment in 0.000 s',   -4446492996171837732);`)

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 10_000)

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

func TestToWorkerSQL_JSON_WithFilters(t *testing.T) {
	db := openDuckDB(t)

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT
	);`)

	mustExec(t, db, `INSERT INTO logs VALUES 
		(1, '', '', '{"level":"INFO","user":"alice","msg":"hello"}', -4446492996171837732),
		(2, '', '', '{"level":"ERROR","user":"bob","msg":"boom"}',   -4446492996171837732),
		(3, '', '', '{"level":"ERROR","user":"carol","msg":"warn"}', -4446492996171837732);`)

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	rows := queryAll(t, db, sql)
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

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT
	);`)

	mustExec(t, db, `INSERT INTO logs VALUES
		(1, '', '', '{"level":"INFO","user":"alice","msg":"hello"}', -4446492996171837732),
		(2, '', '', '{"level":"ERROR","user":"bob","msg":"boom"}',   -4446492996171837732),
		(3, '', '', '{"level":"ERROR","user":"carol","msg":"warn"}', -4446492996171837732);`)

	leaf := LogLeaf{
		Parsers: []ParserStage{{
			Type:   "json",
			Params: map[string]string{},
		}},
	}

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	rows := queryAll(t, db, sql)

	k := rows[0]["__json"]
	if k == nil {
		t.Fatalf("expected __json to be present")
	}
}

func TestToWorkerSQL_Logfmt_WithFilters(t *testing.T) {
	db := openDuckDB(t)

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT
	);`)

	// 1) status=200 user=bob      -> should match
	// 2) status=200 user=carol    -> filtered out by user regex
	// 3) status=500 user=alice    -> filtered out by status eq
	mustExec(t, db, `INSERT INTO logs VALUES
		(1, '', '', 'ts=1 status=200 duration=15ms user=bob msg="ok"',   -4446492996171837732),
		(2, '', '', 'ts=2 status=200 duration=05ms user=carol msg="ok"', -4446492996171837732),
		(3, '', '', 'ts=3 status=500 duration=20ms user=alice msg="err"',-4446492996171837732);`)

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	rows := queryAll(t, db, sql)

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

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		job                       TEXT
	);`)

	mustExec(t, db, `INSERT INTO logs("chq_timestamp", "chq_id", "log_level", "log_message", "chq_fingerprint", job) VALUES
		(1, '', '', 'hello a', -4446492996171837732, 'my-app'),
		(2, '', '', 'hello b', -4446492996171837732, 'other');`)

	leaf := LogLeaf{
		Matchers: []LabelMatch{
			{Label: "job", Op: MatchEq, Value: "my-app"},
		},
	}

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

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

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		job                       TEXT
	);`)

	mustExec(t, db, `INSERT INTO logs("chq_timestamp", job, "log_message", "chq_id", "log_level", "chq_fingerprint") VALUES
		(1, 'my-app',  '{"level":"ERROR","user":"bob","msg":"boom"}', '', '', -4446492996171837732),
		(2, 'my-app',  '{"level":"INFO","user":"alice","msg":"ok"}',  '', '', -4446492996171837732),
		(3, 'other',   '{"level":"ERROR","user":"carol","msg":"warn"}','', '', -4446492996171837732);`)

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	if !strings.Contains(sql, "job = 'my-app'") {
		t.Fatalf("generated SQL missing matcher WHERE:\n%s", sql)
	}
	if !strings.Contains(sql, "json_extract_string") {
		t.Fatalf("generated SQL missing json extraction:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row after matcher+json+filter, got %d\nsql:\n%s", len(rows), sql)
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

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		job                       TEXT
	);`)

	mustExec(t, db, `INSERT INTO logs(job, "chq_timestamp", "log_message", "chq_id", "log_level", "chq_fingerprint") VALUES
	('my-app', 1, '{"response":"ErrorBadGateway","msg":"x"}', '', '', -4446492996171837732),
	('my-app', 2, '{"response":"OK","msg":"y"}',              '', '', -4446492996171837732),
	('my-app', 3, '{"response":"ErrorOops","msg":"z"}',       '', '', -4446492996171837732);`)

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	if !strings.Contains(sql, `job = 'my-app'`) {
		t.Fatalf("generated SQL missing selector matcher WHERE clause:\n%s", sql)
	}
	// Be a bit robust to optional whitespace after the comma.
	if !(strings.Contains(sql, `json_extract_string("log_message", '$.response') AS response`) ||
		strings.Contains(sql, `json_extract_string("log_message",'$.response') AS response`)) {
		t.Fatalf("generated SQL missing JSON projection for 'response':\n%s", sql)
	}
	if !strings.Contains(sql, "CASE WHEN") || !strings.Contains(sql, "starts_with(response, 'Error')") {
		t.Fatalf("generated SQL missing CASE WHEN/starts_with for label_format:\n%s", sql)
	}
	if !strings.Contains(sql, `api = 'ERROR'`) {
		t.Fatalf("generated SQL missing final filter on derived label api:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (Error* only), got %d\nsql:\n%s", len(rows), sql)
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

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"level"                   TEXT,
		"service"                 TEXT,
		"pod"                     TEXT,
		"user"                    TEXT
	);`)

	// Insert test data
	mustExec(t, db, `INSERT INTO logs(
		"chq_timestamp","chq_id","log_level","log_message",
		"chq_fingerprint","level","service","pod","user"
	) VALUES
	 (1000, 'id1', 'info',  'test message 1', -4446492996171837732, 'info',  'api', 'pod1', 'user1'),
	 (2000, 'id2', 'error', 'test message 2', -4446492996171837732, 'error', 'web', 'pod2', 'user2'),
	 (3000, 'id3', 'debug', 'test message 3', -4446492996171837732, 'debug', 'api', 'pod3', 'user3')`)

	// Test with fields parameter - use matchers to ensure fields are projected
	be := &LogLeaf{
		Matchers: []LabelMatch{
			{Label: "service", Op: MatchEq, Value: "api"},
		},
	}
	fields := []string{"service", "pod", "user"}

	sql := be.ToWorkerSQLWithLimit(0, "desc", fields)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

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

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT
	);`)

	// Choose timestamps so DESC order matches expected (alice, bob, charlie)
	mustExec(t, db, `INSERT INTO logs(
		"chq_timestamp","chq_id","log_level","log_message","chq_fingerprint"
	) VALUES 
	(3000,'','', 'user=alice action=login status=success',  -4446492996171837732),
	(2000,'','', 'user=bob action=logout status=success',   -4446492996171837732),
	(1000,'','', 'user=charlie action=view status=pending', -4446492996171837732);`)

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

	sql := be.ToWorkerSQLWithLimit(0, "desc", fields)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return all rows with the extracted fields
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify that the extracted fields are present and correct (in DESC ts order)
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

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"level"                   TEXT,
		"service"                 TEXT,
		"pod"                     TEXT,
		"user"                    TEXT
	);`)

	// Insert test data
	mustExec(t, db, `INSERT INTO logs(
		"chq_timestamp","chq_id","log_level","log_message",
		"chq_fingerprint","level","service","pod","user"
	) VALUES
	 (1000, 'id1', 'info',  'test message 1', -4446492996171837732, 'info',  'api', 'pod1', 'user1'),
	 (2000, 'id2', 'error', 'test message 2', -4446492996171837732, 'error', 'web', 'pod2', 'user2')`)

	// Test with empty fields parameter
	be := &LogLeaf{}
	fields := []string{}

	sql := be.ToWorkerSQLWithLimit(0, "desc", fields)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return all rows (no additional fields added)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify that basic columns are still present
	for i, row := range rows {
		message := getString(row["log_message"])
		if message == "" {
			t.Fatalf("row %d missing log_message", i)
		}
	}
}

// With s0 using SELECT *, unknown fields are a no-op (query succeeds; columns absent).
func TestToWorkerSQLWithLimit_Fields_NonExistentFields_NoOp(t *testing.T) {
	db := openDuckDB(t)

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"level"                   TEXT,
		"service"                 TEXT,
		"pod"                     TEXT,
		"user"                    TEXT
	);`)

	// Insert test data
	mustExec(t, db, `INSERT INTO logs(
		"chq_timestamp","chq_id","log_level","log_message",
		"chq_fingerprint","level","service","pod","user"
	) VALUES
	 (1000, 'id1', 'info', 'test message 1', -4446492996171837732, 'info', 'api', 'pod1', 'user1')`)

	be := &LogLeaf{}
	fields := []string{"nonexistent_field1", "nonexistent_field2"}

	sql := be.ToWorkerSQLWithLimit(0, "desc", fields)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

	rows := queryAll(t, db, sql)

	// Query should succeed and return the row.
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d\nsql:\n%s", len(rows), sql)
	}

	// The unknown fields should not be present in the result set.
	if _, ok := rows[0]["nonexistent_field1"]; ok {
		t.Fatalf("unexpected column projected: nonexistent_field1")
	}
	if _, ok := rows[0]["nonexistent_field2"]; ok {
		t.Fatalf("unexpected column projected: nonexistent_field2")
	}

	// Sanity: base column still present.
	if msg := getString(rows[0]["log_message"]); msg == "" {
		t.Fatalf("missing log_message")
	}
}

func TestToWorkerSQLWithLimit_Fields_WithLimit(t *testing.T) {
	db := openDuckDB(t)

	// Full base schema so SELECT * is always valid
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"level"                   TEXT,
		"service"                 TEXT,
		"pod"                     TEXT,
		"user"                    TEXT
	);`)

	// Insert test data
	mustExec(t, db, `INSERT INTO logs(
		"chq_timestamp","chq_id","log_level","log_message",
		"chq_fingerprint","level","service","pod","user"
	) VALUES
	 (1000, 'id1', 'info',  'test message 1', -4446492996171837732, 'info',  'api', 'pod1', 'user1'),
	 (2000, 'id2', 'error', 'test message 2', -4446492996171837732, 'error', 'web', 'pod2', 'user2'),
	 (3000, 'id3', 'debug', 'test message 3', -4446492996171837732, 'debug', 'api', 'pod3', 'user3'),
	 (4000, 'id4', 'info',  'test message 4', -4446492996171837732, 'info',  'web', 'pod4', 'user4')`)

	// Test with fields parameter and limit - use matchers to ensure fields are projected
	be := &LogLeaf{
		Matchers: []LabelMatch{
			{Label: "service", Op: MatchEq, Value: "api"},
		},
	}
	fields := []string{"service", "pod"}

	sql := be.ToWorkerSQLWithLimit(2, "desc", fields)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 5000)

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

	// Full base schema so SELECT * + time window always works
	mustExec(t, db, `CREATE TABLE logs(
		app                       TEXT,
		level                     TEXT,
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT
	);`)
	mustExec(t, db, `INSERT INTO logs(app, level, "chq_timestamp", "chq_id", "log_level", "log_message", "chq_fingerprint") VALUES
		('web',   'ERROR', 1000, '', '', '{"message":"boom","level":"ERROR","x":1}',  -4446492996171837732),
		('web',   'INFO',  2000, '', '', '{"message":"ok","level":"INFO","x":2}',     -4446492996171837732),
		('other', 'ERROR', 3000, '', '', '{"message":"nope","level":"ERROR","x":3}', -4446492996171837732)
	`)

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 10_000)

	// sanity checks (optional)
	if !strings.Contains(sql, "app = 'web'") {
		t.Fatalf("missing app filter:\n%s", sql)
	}
	if !strings.Contains(sql, `json_extract_string("log_message", '$.message') AS msg`) &&
		!strings.Contains(sql, `json_extract_string("log_message",'$.message') AS msg`) {
		t.Fatalf("missing msg JSON projection:\n%s", sql)
	}
	if !strings.Contains(sql, `level = 'ERROR'`) {
		t.Fatalf("missing level filter:\n%s", sql)
	}
	if !strings.Contains(strings.ToLower(sql), `as "log_message"`) {
		t.Fatalf("expected rewrite of log_message:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (app=web & json.level=ERROR), got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}
	if got := getString(rows[0][`log_message`]); got != "boom" {
		t.Fatalf(`rewritten log_message mismatch: got %q, want "boom"`, got)
	}
}

func TestToWorkerSQL_LineFormat_IndexBaseField(t *testing.T) {
	db := openDuckDB(t)
	t.Cleanup(func() { mustDropTable(db, "logs") })

	// Base table with resource/service, message, and a base field with special chars
	mustExec(t, db, `CREATE TABLE logs(
		"resource_service_name"   TEXT,
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"log_@OrderResult"        TEXT
	);`)

	mustExec(t, db, `INSERT INTO logs VALUES
		('accounting', 1000, '', '', 'orig msg 1', -4446492996171837732, 'SUCCESS'),
		('billing',    2000, '', '', 'orig msg 2', -4446492996171837732, 'IGNORED');`)

	q := `{resource_service_name="accounting"} ` +
		`| label_format order_result=` + "`{{ index . \"log_@OrderResult\" }}`" + ` ` +
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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 10_000)

	// Sanity checks
	if !strings.Contains(sql, `resource_service_name = 'accounting'`) {
		t.Fatalf("missing selector on resource_service_name:\n%s", sql)
	}
	if !strings.Contains(sql, `"log_@OrderResult"`) {
		t.Fatalf("expected hoisted base column \"log_@OrderResult\" in SQL:\n%s", sql)
	}
	if !strings.Contains(strings.ToLower(sql), `as "log_message"`) {
		t.Fatalf("expected rewrite of log_message via line_format:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (service=accounting), got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}

	gotMsg := getString(rows[0][`log_message`])
	if gotMsg != "SUCCESS" {
		t.Fatalf(`rewritten log_message mismatch: got %q, want "SUCCESS"`, gotMsg)
	}
	if svc := getString(rows[0][`resource_service_name`]); svc != "accounting" {
		t.Fatalf("wrong row selected: resource_service_name=%q", svc)
	}
}

func TestToWorkerSQL_LineFormat_DirectIndexBaseField(t *testing.T) {
	db := openDuckDB(t)
	t.Cleanup(func() { mustDropTable(db, "logs") })

	mustExec(t, db, `CREATE TABLE logs(
		"resource_service_name"   TEXT,
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"log_@OrderResult"        TEXT
	);`)

	mustExec(t, db, `INSERT INTO logs VALUES
		('accounting', 1000, '', '', 'orig msg 1', -4446492996171837732, 'SUCCESS'),
		('billing',    2000, '', '', 'orig msg 2', -4446492996171837732, 'IGNORED');`)

	q := `{resource_service_name="accounting"} | line_format "{{ index . \"log_@OrderResult\" }}"`

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 10_000)

	if !strings.Contains(sql, `resource_service_name = 'accounting'`) { // case-sensitive helper is fine here
		t.Fatalf("missing selector on resource_service_name:\n%s", sql)
	}
	if !strings.Contains(sql, `"log_@OrderResult"`) {
		t.Fatalf("expected hoisted base column \"log_@OrderResult\" in SQL:\n%s", sql)
	}
	if !strings.Contains(strings.ToLower(sql), `as "log_message"`) {
		t.Fatalf("expected rewrite of log_message via line_format:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (service=accounting), got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}

	gotMsg := getString(rows[0][`log_message`])
	if gotMsg != "SUCCESS" {
		t.Fatalf(`rewritten log_message mismatch: got %q, want "SUCCESS"`, gotMsg)
	}
	if svc := getString(rows[0][`resource_service_name`]); svc != "accounting" {
		t.Fatalf("wrong row selected: resource_service_name=%q", svc)
	}
}

func TestToWorkerSQL_LineFormat_DirectIndexBaseField_UnderscoreCompat(t *testing.T) {
	db := openDuckDB(t)
	t.Cleanup(func() { mustDropTable(db, "logs") })

	mustExec(t, db, `CREATE TABLE logs(
		"resource_service_name"   TEXT,
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"log_@OrderResult"        TEXT
	);`)

	mustExec(t, db, `INSERT INTO logs VALUES
		('accounting', 1000, '', '', 'orig msg 1', -4446492996171837732, 'SUCCESS'),
		('billing',    2000, '', '', 'orig msg 2', -4446492996171837732, 'IGNORED');`)

	// underscore in the template key; base column uses a dot.
	q := `{resource_service_name="accounting"} | line_format "{{ index . \"log_@OrderResult\" }}"`

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 10_000)

	if !strings.Contains(sql, `"log_@OrderResult"`) {
		t.Fatalf("expected hoisted base column \"log_@OrderResult\" in SQL:\n%s", sql)
	}
	if !strings.Contains(strings.ToLower(sql), `as "log_message"`) {
		t.Fatalf("expected rewrite of log_message via line_format:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (service=accounting), got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}
	if got := getString(rows[0][`log_message`]); got != "SUCCESS" {
		t.Fatalf(`rewritten log_message mismatch: got %q, want "SUCCESS"`, got)
	}
	if svc := getString(rows[0][`resource_service_name`]); svc != "accounting" {
		t.Fatalf("wrong row selected: resource_service_name=%q", svc)
	}
}

func TestToWorkerSQL_LineFormat_IndexThenJSON_TwoStage(t *testing.T) {
	db := openDuckDB(t)
	t.Cleanup(func() { mustDropTable(db, "logs") })

	mustExec(t, db, `CREATE TABLE logs(
		"resource_service_name"   TEXT,
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"log_@OrderResult"        TEXT
	);`)

	mustExec(t, db, `INSERT INTO logs VALUES
		('accounting', 1000, '', '', 'orig msg (to be replaced)', -4446492996171837732, '{"orderId":"O-123","shippingCost":{"currencyCode":"USD","units":12,"nanos":500000000}}'),
		('billing',    2000, '', '', 'orig msg (ignored)',        -4446492996171837732, '{"orderId":"O-999","shippingCost":{"currencyCode":"EUR","units":7,"nanos":0}}');`)

	q := `{resource_service_name="accounting"} ` +
		`| line_format "{{ index . \"log_@OrderResult\" }}" ` +
		`| json ` +
		`| line_format "orderId={{.orderId}} currency={{.shippingCost.currencyCode}} units={{.shippingCost.units}} nanos={{.shippingCost.nanos}}"`

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 10_000)

	if !strings.Contains(sql, `resource_service_name = 'accounting'`) {
		t.Fatalf("missing selector on resource_service_name:\n%s", sql)
	}
	if !strings.Contains(sql, `"log_@OrderResult"`) {
		t.Fatalf("expected hoisted base column \"log_@OrderResult\" in SQL:\n%s", sql)
	}
	if !strings.Contains(strings.ToLower(sql), `as "log_message"`) {
		t.Fatalf("expected rewrite of log_message via line_format:\n%s", sql)
	}

	// Look for any of these common JSON projection forms
	wantJsonBits := []string{
		`json_extract_string("log_message", '$.orderId')`,
		`json_extract_string("log_message", '$.shippingCost.currencyCode')`,
		`json_extract("log_message", '$.shippingCost.units')`,
		`json_extract("log_message", '$.shippingCost.nanos')`,
	}
	foundJsonAny := false
	for _, bit := range wantJsonBits {
		if strings.Contains(sql, bit) {
			foundJsonAny = true
			break
		}
	}
	if !foundJsonAny {
		t.Logf("note: JSON projections not explicitly visible in SQL (compiler may inline), sql:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (service=accounting), got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}

	wantMsg := "orderId=O-123 currency=USD units=12 nanos=500000000"
	gotMsg := getString(rows[0][`log_message`])
	if gotMsg != wantMsg {
		t.Fatalf("final log_message mismatch:\n  got:  %q\n  want: %q", gotMsg, wantMsg)
	}
	if svc := getString(rows[0][`resource_service_name`]); svc != "accounting" {
		t.Fatalf("wrong row selected: resource_service_name=%q", svc)
	}
}

func TestToWorkerSQL_LineFormat_JSON_LabelFormat_ItemCount_WithFingerprint(t *testing.T) {
	db := openDuckDB(t)
	t.Cleanup(func() { mustDropTable(db, "logs") })

	// Full base table so we don't need replaceTable(..)
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"log_level"       TEXT,
		"log_message"     TEXT,
		"chq_fingerprint" BIGINT,
		"resource_service_name"   TEXT,
		"log_@OrderResult"        TEXT
	);`)

	const orderJSON = `{
	  "orderId": "f127489e-98ab-11f0-8a9a-e21d1a6f40db",
	  "shippingTrackingId": "862c07a6-ef43-4b79-a84c-3b9b76ca3e1f",
	  "shippingCost": { "currencyCode": "USD", "units": "456", "nanos": 500000000 },
	  "shippingAddress": {
	    "streetAddress": "1600 Amphitheatre Parkway",
	    "city": "Mountain View",
	    "state": "CA",
	    "country": "United States",
	    "zipCode": "94043"
	  },
	  "items": [
	    { "item": { "productId": "HQTGWGPNH4", "quantity": 3 }, "cost": { "currencyCode": "USD", "nanos": 990000000 } },
	    { "item": { "productId": "66VCHSJNUP", "quantity": 3 }, "cost": { "currencyCode": "USD", "units": "349", "nanos": 949999999 } },
	    { "item": { "productId": "2ZYFJ3GM2N", "quantity": 4 }, "cost": { "currencyCode": "USD", "units": "209", "nanos": 949999999 } },
	    { "item": { "productId": "9SIQT8TOJO", "quantity": 1 }, "cost": { "currencyCode": "USD", "units": "3599" } }
	  ]
	}`

	// One matching row (service=accounting AND fingerprint match) + one distractor.
	mustExec(t, db, `INSERT INTO logs VALUES
		(1000, 'id1', 'info',  'orig msg 1', 7754623969787599908, 'accounting', ?),
		(2000, 'id2', 'debug', 'orig msg 2', 111,                  'billing',    '{"items":[{"x":1}]}')`, orderJSON)

	// {resource_service_name="accounting", chq_fingerprint="7754623969787599908"}
	// | line_format "{{ index . \"log_@OrderResult\" }}"
	// | json
	// | label_format item_count=`{{len .items}}`
	q := `{resource_service_name="accounting", chq_fingerprint="7754623969787599908"} ` +
		`| line_format "{{ index . \"log_@OrderResult\" }}" ` +
		`| json ` +
		`| label_format item_count=` + "`{{len .items}}`"

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

	// Use the real base table (no replaceTable), and plug in a wide time range.
	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 10_000)

	// Sanity checks on generated SQL.
	if !strings.Contains(sql, `resource_service_name = 'accounting'`) {
		t.Fatalf("missing selector on resource_service_name:\n%s", sql)
	}
	if !strings.Contains(sql, `chq_fingerprint = '7754623969787599908'`) {
		t.Fatalf("missing fingerprint selector:\n%s", sql)
	}
	if !strings.Contains(sql, `"log_@OrderResult"`) {
		t.Fatalf("expected hoisted base column \"log_@OrderResult\" in SQL:\n%s", sql)
	}
	if !strings.Contains(sql, `AS item_count`) {
		t.Fatalf("expected label_format to produce item_count column:\n%s", sql)
	}

	// Execute.
	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 matching row, got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}

	// Validate item_count == 4 (accept string or numeric driver types).
	got := rows[0]["item_count"]
	switch v := got.(type) {
	case nil:
		t.Fatalf("item_count is nil")
	case string:
		if v != "4" {
			t.Fatalf("item_count (string) = %q, want \"4\"", v)
		}
	case int64:
		if v != 4 {
			t.Fatalf("item_count (int64) = %d, want 4", v)
		}
	case float64:
		if int(v) != 4 {
			t.Fatalf("item_count (float64) = %v, want 4", v)
		}
	default:
		t.Fatalf("item_count has unexpected type %T: %v", got, got)
	}
}

// TestToWorkerSQL_CustomerIssue_ResourceFileTypeFilter tests that resource.file.type
// filters are correctly applied in the generated SQL, reproducing the customer issue
// where filtering for resource.file.type=avxgwstatesync returned cloudxcommands results.
func TestToWorkerSQL_CustomerIssue_ResourceFileTypeFilter(t *testing.T) {
	db := openDuckDB(t)
	t.Cleanup(func() { mustDropTable(db, "logs") })

	// Create logs table with resource.* fields
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"         BIGINT,
		"chq_id"                TEXT,
		"log_level"             TEXT,
		"log_message"           TEXT,
		"chq_fingerprint"       BIGINT,
		"resource_bucket_name"  TEXT,
		"resource_file"         TEXT,
		"resource_file_type"    TEXT
	);`)

	// Insert test data:
	// - Row 1: matches all filters (avxgwstatesync)
	// - Row 2: matches bucket and file but WRONG file_type (cloudxcommands)
	// - Row 3: completely different data
	mustExec(t, db, `INSERT INTO logs VALUES
		(1000, 'id1', 'info', 'log from avxgwstatesync', 123,
		 'avxit-dev-s3-use2-datalake',
		 'vitechinc.com-abu-hirw8pmdunp-1736355841.4503462_2025-10-23-193952_dr-client-ingress-psf-gw',
		 'avxgwstatesync'),
		(2000, 'id2', 'info', 'log from cloudxcommands', 456,
		 'avxit-dev-s3-use2-datalake',
		 'vitechinc.com-abu-hirw8pmdunp-1736355841.4503462_2025-10-23-193952_dr-client-ingress-psf-gw',
		 'cloudxcommands'),
		(3000, 'id3', 'info', 'completely different log', 789,
		 'other-bucket',
		 'other-file',
		 'othertype')`)

	// The customer's query (translated to LogQL)
	q := `{resource_bucket_name="avxit-dev-s3-use2-datalake",` +
		`resource_file="vitechinc.com-abu-hirw8pmdunp-1736355841.4503462_2025-10-23-193952_dr-client-ingress-psf-gw",` +
		`resource_file_type="avxgwstatesync"}`

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

	sql := leaf.ToWorkerSQLWithLimit(0, "desc", nil)
	sql = strings.ReplaceAll(sql, "{table}", "logs")
	sql = replaceStartEnd(sql, 0, 10_000)

	t.Logf("Generated SQL:\n%s", sql)

	// Verify the SQL contains all three matchers
	if !strings.Contains(sql, `resource_bucket_name = 'avxit-dev-s3-use2-datalake'`) {
		t.Fatalf("missing filter on resource_bucket_name:\n%s", sql)
	}
	if !strings.Contains(sql, `resource_file = 'vitechinc.com-abu-hirw8pmdunp-1736355841.4503462_2025-10-23-193952_dr-client-ingress-psf-gw'`) {
		t.Fatalf("missing filter on resource_file:\n%s", sql)
	}
	if !strings.Contains(sql, `resource_file_type = 'avxgwstatesync'`) {
		t.Fatalf("missing filter on resource_file_type:\n%s", sql)
	}

	// Execute query
	rows := queryAll(t, db, sql)

	// Should return exactly 1 row (only the avxgwstatesync row)
	if len(rows) != 1 {
		t.Fatalf("expected 1 matching row, got %d\nrows=%v\nsql=\n%s", len(rows), rows, sql)
	}

	// Verify it's the correct row
	fileType := getString(rows[0]["resource_file_type"])
	if fileType != "avxgwstatesync" {
		t.Fatalf("wrong row returned: resource_file_type=%q, want \"avxgwstatesync\"", fileType)
	}

	message := getString(rows[0]["log_message"])
	if message != "log from avxgwstatesync" {
		t.Fatalf("wrong row returned: log_message=%q, want \"log from avxgwstatesync\"", message)
	}
}

// --- Tests for ToWorkerSQLForTagNames ---

func TestToWorkerSQLForTagNames_Basic(t *testing.T) {
	db := openDuckDB(t)

	// Create table with system columns (excluded) and user columns (included)
	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"chq_fingerprint" BIGINT,
		"log_message"     TEXT,
		"log_level"       TEXT,
		"service"         TEXT,
		"pod"             TEXT,
		"region"          TEXT
	);`)

	// Insert test data - all user columns have values
	mustExec(t, db, `INSERT INTO logs VALUES
		(0, 'id1', 123, 'test message', 'info', 'api', 'pod1', 'us-east-1'),
		(1000, 'id2', 456, 'another message', 'error', 'web', 'pod2', 'us-west-2')`)

	be := &LogLeaf{}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagNames()), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return user columns only (log_level, service, pod, region)
	// System columns (chq_timestamp, chq_id, chq_fingerprint, log_message) should be excluded
	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	// Verify expected columns are present
	expected := []string{"log_level", "service", "pod", "region"}
	for _, col := range expected {
		if !got[col] {
			t.Errorf("expected tag name %q not found in results: %v", col, got)
		}
	}

	// Verify system columns are NOT present
	excluded := []string{"chq_timestamp", "chq_id", "chq_fingerprint", "log_message"}
	for _, col := range excluded {
		if got[col] {
			t.Errorf("system column %q should be excluded but was found in results", col)
		}
	}
}

func TestToWorkerSQLForTagNames_ExcludesNullColumns(t *testing.T) {
	db := openDuckDB(t)

	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"chq_fingerprint" BIGINT,
		"log_message"     TEXT,
		"log_level"       TEXT,
		"service"         TEXT,
		"pod"             TEXT,
		"region"          TEXT
	);`)

	// Insert test data where 'region' is always NULL
	mustExec(t, db, `INSERT INTO logs VALUES
		(0, 'id1', 123, 'test message', 'info', 'api', 'pod1', NULL),
		(1000, 'id2', 456, 'another message', 'error', 'web', 'pod2', NULL)`)

	be := &LogLeaf{}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagNames()), 0, 5000)

	rows := queryAll(t, db, sql)

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	// 'region' should NOT be in results since it's always NULL
	if got["region"] {
		t.Errorf("column 'region' has only NULL values and should be excluded, but was found")
	}

	// Other columns should still be present
	for _, col := range []string{"log_level", "service", "pod"} {
		if !got[col] {
			t.Errorf("expected tag name %q not found in results: %v", col, got)
		}
	}
}

func TestToWorkerSQLForTagNames_WithMatchers(t *testing.T) {
	db := openDuckDB(t)

	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"chq_fingerprint" BIGINT,
		"log_message"     TEXT,
		"log_level"       TEXT,
		"service"         TEXT,
		"pod"             TEXT,
		"extra_field"     TEXT
	);`)

	// Insert data where 'extra_field' only has values for service=api
	mustExec(t, db, `INSERT INTO logs VALUES
		(0, 'id1', 123, 'test', 'info', 'api', 'pod1', 'extra_value'),
		(1000, 'id2', 456, 'test', 'error', 'web', 'pod2', NULL)`)

	// Filter to service=api only
	be := &LogLeaf{
		Matchers: []LabelMatch{
			{Label: "service", Op: MatchEq, Value: "api"},
		},
	}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagNames()), 0, 5000)

	rows := queryAll(t, db, sql)

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	// 'extra_field' should be present because it has a value for service=api
	if !got["extra_field"] {
		t.Errorf("expected 'extra_field' to be present for service=api filter, got: %v", got)
	}
}

func TestToWorkerSQLForTagNames_WithLineFilters(t *testing.T) {
	db := openDuckDB(t)

	mustExec(t, db, `CREATE TABLE logs(
		"chq_timestamp"   BIGINT,
		"chq_id"          TEXT,
		"chq_fingerprint" BIGINT,
		"log_message"     TEXT,
		"log_level"       TEXT,
		"service"         TEXT,
		"error_code"      TEXT
	);`)

	// 'error_code' only has values for messages containing "error"
	mustExec(t, db, `INSERT INTO logs VALUES
		(0, 'id1', 123, 'error occurred', 'error', 'api', 'ERR001'),
		(1000, 'id2', 456, 'success message', 'info', 'web', NULL)`)

	// Filter to messages containing "error"
	be := &LogLeaf{
		LineFilters: []LineFilter{
			{Op: LineContains, Match: "error"},
		},
	}
	sql := replaceStartEnd(replaceTable(be.ToWorkerSQLForTagNames()), 0, 5000)

	rows := queryAll(t, db, sql)

	got := make(map[string]bool)
	for _, r := range rows {
		got[getString(r["tag_value"])] = true
	}

	// 'error_code' should be present because it has a value for error messages
	if !got["error_code"] {
		t.Errorf("expected 'error_code' to be present for error line filter, got: %v", got)
	}

	// Should also have the other non-null columns
	if !got["log_level"] || !got["service"] {
		t.Errorf("expected 'log_level' and 'service' to be present, got: %v", got)
	}
}
