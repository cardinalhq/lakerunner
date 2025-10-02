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
	"strings"
	"testing"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// Replace {table} with a local base subquery that injects stub columns for
// spans queries so they always have the required columns available.
func replaceSpansTable(sql string) string {
	base := `(SELECT *,
  0::BIGINT   AS "chq_timestamp",
  ''::VARCHAR AS "_cardinalhq_id",
  -4446492996171837732::BIGINT   AS "chq_fingerprint",
  ''::VARCHAR AS "chq_name",
  ''::VARCHAR AS "_cardinalhq_kind",
  ''::VARCHAR AS "_cardinalhq_span_id",
  ''::VARCHAR AS "_cardinalhq_span_trace_id",
  ''::VARCHAR AS "_cardinalhq_status_code",
  0::BIGINT   AS "_cardinalhq_span_duration",
  ''::VARCHAR AS "service_name",
  ''::VARCHAR AS "service_version"
FROM spans) AS _t`

	// Substitute table and time placeholders.
	sql = strings.ReplaceAll(sql, "{table}", base)

	return sql
}

func openSpansDuckDB(t *testing.T) *sql.DB {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func mustExecSpans(t *testing.T, db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, query)
	}
}

func mustDropSpansTable(db *sql.DB, table string) {
	_, _ = db.Exec("DROP TABLE IF EXISTS " + quoteIdent(table))
}

func createSpansTable(t *testing.T, db *sql.DB) {
	t.Cleanup(func() { mustDropSpansTable(db, "spans") })

	mustExecSpans(t, db, `CREATE TABLE spans(
  "chq_timestamp" BIGINT,
  "_cardinalhq_id" VARCHAR,
  "chq_fingerprint" BIGINT,
  "chq_name" VARCHAR,
  "_cardinalhq_kind" VARCHAR,
  "_cardinalhq_span_id" VARCHAR,
  "_cardinalhq_span_trace_id" VARCHAR,
  "_cardinalhq_status_code" VARCHAR,
  "_cardinalhq_span_duration" BIGINT,
  "service_name" VARCHAR,
  "service_version" VARCHAR
);`)
}

func TestToSpansWorkerSQL_BasicFields(t *testing.T) {
	db := openSpansDuckDB(t)
	createSpansTable(t, db)

	// Insert test data
	mustExecSpans(t, db, `INSERT INTO spans VALUES
	 (1000, 'id1', -4446492996171837732, 'GET /api/users', 'server', 'span1', 'trace1', 'OK', 150000000, 'my-service', 'v1.0'),
	 (2000, 'id2', -4446492996171837732, 'POST /api/orders', 'client', 'span2', 'trace2', 'ERROR', 250000000, 'other-service', 'v2.0'),
	 (3000, 'id3', -4446492996171837732, 'GET /api/products', 'server', 'span3', 'trace3', 'OK', 100000000, 'my-service', 'v1.1')`)

	leaf := LogLeaf{} // no parsers/filters; just pass-through with defaults
	sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

	// Should include the sentinel so CacheManager can splice segment filter.
	if !strings.Contains(sql, "AND true") {
		t.Fatalf("expected sentinel AND true in generated SQL:\n%s", sql)
	}

	rows := queryAll(t, db, sql)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify the default columns exist, including spans fields
	for _, r := range rows {
		_ = getString(r["_cardinalhq_id"])
		_ = getString(r["chq_fingerprint"])
		_ = getString(r["chq_name"])
		_ = getString(r["_cardinalhq_kind"])
		_ = getString(r["_cardinalhq_span_id"])
		_ = getString(r["_cardinalhq_span_trace_id"])
		_ = getString(r["_cardinalhq_status_code"])
		_ = getInt64(r["_cardinalhq_span_duration"])
	}
}

func TestToSpansWorkerSQL_WithCardinalhqNameMatcher(t *testing.T) {
	db := openSpansDuckDB(t)
	createSpansTable(t, db)

	// Insert test data
	mustExecSpans(t, db, `INSERT INTO spans VALUES
	 (1000, 'id1', -4446492996171837732, 'GET /api/users', 'server', 'span1', 'trace1', 'OK', 150000000, 'my-service', 'v1.0'),
	 (2000, 'id2', -4446492996171837732, 'POST /api/orders', 'client', 'span2', 'trace2', 'ERROR', 250000000, 'other-service', 'v2.0'),
	 (3000, 'id3', -4446492996171837732, 'GET /api/products', 'server', 'span3', 'trace3', 'OK', 100000000, 'my-service', 'v1.1')`)

	leaf := LogLeaf{
		Matchers: []LabelMatch{
			{Label: "chq_name", Op: MatchEq, Value: "GET /api/users"},
		},
	}
	sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

	rows := queryAll(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row (chq_name=GET /api/users), got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify we get the expected span names
	for _, row := range rows {
		spanName := getString(row["chq_name"])
		if spanName != "GET /api/users" {
			t.Fatalf("expected chq_name='GET /api/users', got %q", spanName)
		}
	}
}

func TestToSpansWorkerSQL_WithCardinalhqKindMatcher(t *testing.T) {
	db := openSpansDuckDB(t)
	createSpansTable(t, db)

	// Insert test data
	mustExecSpans(t, db, `INSERT INTO spans VALUES
	 (1000, 'id1', -4446492996171837732, 'GET /api/users', 'server', 'span1', 'trace1', 'OK', 150000000, 'my-service', 'v1.0'),
	 (2000, 'id2', -4446492996171837732, 'POST /api/orders', 'client', 'span2', 'trace2', 'ERROR', 250000000, 'other-service', 'v2.0'),
	 (3000, 'id3', -4446492996171837732, 'GET /api/products', 'server', 'span3', 'trace3', 'OK', 100000000, 'my-service', 'v1.1')`)

	leaf := LogLeaf{
		Matchers: []LabelMatch{
			{Label: "_cardinalhq_kind", Op: MatchEq, Value: "server"},
		},
	}
	sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

	rows := queryAll(t, db, sql)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (_cardinalhq_kind=server), got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify we get the expected span kinds
	for _, row := range rows {
		spanKind := getString(row["_cardinalhq_kind"])
		if spanKind != "server" {
			t.Fatalf("expected _cardinalhq_kind='server', got %q", spanKind)
		}
	}
}

func TestToSpansWorkerSQL_WithMultipleMatchers(t *testing.T) {
	db := openSpansDuckDB(t)
	createSpansTable(t, db)

	// Insert test data
	mustExecSpans(t, db, `INSERT INTO spans VALUES
	 (1000, 'id1', -4446492996171837732, 'GET /api/users', 'server', 'span1', 'trace1', 'OK', 150000000, 'my-service', 'v1.0'),
	 (2000, 'id2', -4446492996171837732, 'POST /api/orders', 'client', 'span2', 'trace2', 'ERROR', 250000000, 'other-service', 'v2.0'),
	 (3000, 'id3', -4446492996171837732, 'GET /api/products', 'server', 'span3', 'trace3', 'OK', 100000000, 'my-service', 'v1.1')`)

	leaf := LogLeaf{
		Matchers: []LabelMatch{
			{Label: "chq_name", Op: MatchRe, Value: "GET.*"},
			{Label: "_cardinalhq_kind", Op: MatchEq, Value: "server"},
		},
	}
	sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

	rows := queryAll(t, db, sql)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (chq_name matching GET.* AND _cardinalhq_kind=server), got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify we get the expected combinations
	for _, row := range rows {
		spanName := getString(row["chq_name"])
		spanKind := getString(row["_cardinalhq_kind"])
		if !strings.HasPrefix(spanName, "GET") || spanKind != "server" {
			t.Fatalf("expected chq_name starting with 'GET' AND _cardinalhq_kind='server', got chq_name=%q _cardinalhq_kind=%q", spanName, spanKind)
		}
	}
}

func TestToSpansWorkerSQL_WithFieldsParameter(t *testing.T) {
	db := openSpansDuckDB(t)
	createSpansTable(t, db)

	// Insert test data
	mustExecSpans(t, db, `INSERT INTO spans VALUES
	 (1000, 'id1', -4446492996171837732, 'GET /api/users', 'server', 'span1', 'trace1', 'OK', 150000000, 'my-service', 'v1.0'),
	 (2000, 'id2', -4446492996171837732, 'POST /api/orders', 'client', 'span2', 'trace2', 'ERROR', 250000000, 'other-service', 'v2.0'),
	 (3000, 'id3', -4446492996171837732, 'GET /api/products', 'server', 'span3', 'trace3', 'OK', 100000000, 'my-service', 'v1.1')`)

	// Test with fields parameter
	leaf := LogLeaf{
		Matchers: []LabelMatch{
			{Label: "chq_name", Op: MatchRe, Value: "GET.*"},
		},
	}
	fields := []string{"chq_name", "_cardinalhq_kind", "service_name"}
	sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", fields)), 0, 5000)

	rows := queryAll(t, db, sql)

	// Should return only rows where chq_name matches GET.* (2 rows)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (chq_name matching GET.*), got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify that the specified fields are present in the results
	for i, row := range rows {
		spanName := getString(row["chq_name"])
		spanKind := getString(row["_cardinalhq_kind"])
		serviceName := getString(row["service_name"])

		// Check that fields are not empty (they should have values from our test data)
		if spanName == "" || spanKind == "" || serviceName == "" {
			t.Fatalf("row %d missing expected field values: chq_name=%q _cardinalhq_kind=%q service_name=%q", i, spanName, spanKind, serviceName)
		}

		// Verify we get the expected values (only GET.* rows)
		if !strings.HasPrefix(spanName, "GET") {
			t.Fatalf("row %d: expected chq_name to start with 'GET', got %q", i, spanName)
		}
		if spanKind != "server" {
			t.Fatalf("row %d: expected _cardinalhq_kind='server', got %q", i, spanKind)
		}
		if serviceName != "my-service" {
			t.Fatalf("row %d: expected service_name='my-service', got %q", i, serviceName)
		}
	}
}

func TestToSpansWorkerSQL_WithRegexMatcher(t *testing.T) {
	db := openSpansDuckDB(t)
	createSpansTable(t, db)

	// Insert test data
	mustExecSpans(t, db, `INSERT INTO spans VALUES
	 (1000, 'id1', -4446492996171837732, 'GET /api/users', 'server', 'span1', 'trace1', 'OK', 150000000, 'my-service', 'v1.0'),
	 (2000, 'id2', -4446492996171837732, 'POST /api/orders', 'client', 'span2', 'trace2', 'ERROR', 250000000, 'other-service', 'v2.0'),
	 (3000, 'id3', -4446492996171837732, 'GET /api/products', 'server', 'span3', 'trace3', 'OK', 100000000, 'my-service', 'v1.1')`)

	leaf := LogLeaf{
		Matchers: []LabelMatch{
			{Label: "chq_name", Op: MatchRe, Value: "GET.*"},
		},
	}
	sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil)), 0, 5000)

	rows := queryAll(t, db, sql)
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows (chq_name matching 'GET.*'), got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify we get the expected span names
	for _, row := range rows {
		spanName := getString(row["chq_name"])
		if !strings.HasPrefix(spanName, "GET") {
			t.Fatalf("expected chq_name to start with 'GET', got %q", spanName)
		}
	}
}

func getInt64(v any) int64 {
	switch x := v.(type) {
	case int64:
		return x
	case int:
		return int64(x)
	case float64:
		return int64(x)
	default:
		return 0
	}
}
