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
	"database/sql"
	"strings"
	"testing"

	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/cardinalhq/lakerunner/logql"
)

// TestSpansQueryIntegration tests the full spans query flow from HTTP request to SQL execution
func TestSpansQueryIntegration(t *testing.T) {
	// This test validates that spans queries work end-to-end
	// It creates a spans table, inserts test data, and verifies that the spans SQL generation works correctly

	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Create spans table with the expected schema
	mustExecSpans(t, db, `CREATE TABLE spans(
  "chq_timestamp" BIGINT,
  "chq_id" VARCHAR,
  "chq_fingerprint" BIGINT,
  "span_name" VARCHAR,
  "span_kind" VARCHAR,
  "span_id" VARCHAR,
  "span_trace_id" VARCHAR,
  "span_status_code" VARCHAR,
  "span_duration" BIGINT,
  "service_name" VARCHAR,
  "service_version" VARCHAR
);`)

	// Insert test spans data
	mustExecSpans(t, db, `INSERT INTO spans VALUES
	 (1000, 'span1', -4446492996171837732, 'GET /api/users', 'server', 'span1', 'trace1', 'OK', 150000000, 'my-service', 'v1.0'),
	 (2000, 'span2', -4446492996171837732, 'POST /api/orders', 'client', 'span2', 'trace2', 'ERROR', 250000000, 'other-service', 'v2.0'),
	 (3000, 'span3', -4446492996171837732, 'GET /api/products', 'server', 'span3', 'trace3', 'OK', 100000000, 'my-service', 'v1.1'),
	 (4000, 'span4', -4446492996171837732, 'PUT /api/users/123', 'client', 'span4', 'trace4', 'OK', 300000000, 'my-service', 'v1.0');`)

	// Test 1: Basic spans query with span_name matcher
	t.Run("BasicSpanNameQuery", func(t *testing.T) {
		leaf := logql.LogLeaf{
			Matchers: []logql.LabelMatch{
				{Label: "span_name", Op: logql.MatchEq, Value: "GET /api/users"},
			},
		}
		sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil)))

		rows := queryAllSpans(t, db, sql)
		if len(rows) != 1 {
			t.Fatalf("expected 1 row (span_name=GET /api/users), got %d\nsql:\n%s", len(rows), sql)
		}

		// Verify all returned rows have span_name = "GET /api/users"
		for _, row := range rows {
			spanName := getString(row["span_name"])
			if spanName != "GET /api/users" {
				t.Fatalf("expected span_name='GET /api/users', got %q", spanName)
			}
		}
	})

	// Test 2: Spans query with span_kind matcher
	t.Run("SpanKindQuery", func(t *testing.T) {
		leaf := logql.LogLeaf{
			Matchers: []logql.LabelMatch{
				{Label: "span_kind", Op: logql.MatchEq, Value: "server"},
			},
		}
		sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil)))

		rows := queryAllSpans(t, db, sql)
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows (span_kind=server), got %d\nsql:\n%s", len(rows), sql)
		}

		// Verify all returned rows have span_kind = "server"
		for _, row := range rows {
			spanKind := getString(row["span_kind"])
			if spanKind != "server" {
				t.Fatalf("expected span_kind='server', got %q", spanKind)
			}
		}
	})

	// Test 3: Spans query with multiple matchers
	t.Run("MultipleMatchersQuery", func(t *testing.T) {
		leaf := logql.LogLeaf{
			Matchers: []logql.LabelMatch{
				{Label: "span_name", Op: logql.MatchRe, Value: "GET.*"},
				{Label: "span_kind", Op: logql.MatchEq, Value: "server"},
			},
		}
		sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil)))

		rows := queryAllSpans(t, db, sql)
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows (span_name matching GET.* AND span_kind=server), got %d\nsql:\n%s", len(rows), sql)
		}

		// Verify all returned rows have both conditions
		for _, row := range rows {
			spanName := getString(row["span_name"])
			spanKind := getString(row["span_kind"])
			if !strings.HasPrefix(spanName, "GET") || spanKind != "server" {
				t.Fatalf("expected span_name starting with 'GET' AND span_kind='server', got span_name=%q span_kind=%q", spanName, spanKind)
			}
		}
	})

	// Test 4: Spans query with fields parameter
	t.Run("FieldsParameterQuery", func(t *testing.T) {
		leaf := logql.LogLeaf{
			Matchers: []logql.LabelMatch{
				{Label: "span_name", Op: logql.MatchRe, Value: "GET.*"},
			},
		}
		fields := []string{"span_name", "span_kind", "service_name", "span_trace_id"}
		sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", fields)))

		rows := queryAllSpans(t, db, sql)
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows (span_name matching GET.*), got %d\nsql:\n%s", len(rows), sql)
		}

		// Verify that the specified fields are present and have values
		for i, row := range rows {
			spanName := getString(row["span_name"])
			spanKind := getString(row["span_kind"])
			serviceName := getString(row["service_name"])
			traceID := getString(row["span_trace_id"])

			if spanName == "" || spanKind == "" || serviceName == "" || traceID == "" {
				t.Fatalf("row %d missing expected field values: span_name=%q span_kind=%q service_name=%q span_trace_id=%q",
					i, spanName, spanKind, serviceName, traceID)
			}
		}
	})

	// Test 5: Spans query with regex matcher
	t.Run("RegexMatcherQuery", func(t *testing.T) {
		leaf := logql.LogLeaf{
			Matchers: []logql.LabelMatch{
				{Label: "span_name", Op: logql.MatchRe, Value: "GET.*"},
			},
		}
		sql := replaceStartEnd(replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil)))

		rows := queryAllSpans(t, db, sql)
		if len(rows) != 2 {
			t.Fatalf("expected 2 rows (span_name matching 'GET.*'), got %d\nsql:\n%s", len(rows), sql)
		}

		// Verify all returned rows have span_name starting with "GET"
		for _, row := range rows {
			spanName := getString(row["span_name"])
			if !strings.HasPrefix(spanName, "GET") {
				t.Fatalf("expected span_name to start with 'GET', got %q", spanName)
			}
		}
	})

}

// TestSpansQueryWithTimeRange tests spans queries with time range filtering
func TestSpansQueryWithTimeRange(t *testing.T) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer func() { _ = db.Close() }()

	// Create spans table
	mustExecSpans(t, db, `CREATE TABLE spans(
  "chq_timestamp" BIGINT,
  "chq_id" VARCHAR,
  "chq_fingerprint" BIGINT,
  "span_name" VARCHAR,
  "span_kind" VARCHAR,
  "span_id" VARCHAR,
  "span_trace_id" VARCHAR,
  "span_status_code" VARCHAR,
  "span_duration" BIGINT,
  "service_name" VARCHAR,
  "service_version" VARCHAR
);`)

	// Insert test data with different timestamps
	mustExecSpans(t, db, `INSERT INTO spans VALUES
	 (1000, 'span1', -4446492996171837732, 'GET /api/users', 'server', 'span1', 'trace1', 'OK', 150000000, 'my-service', 'v1.0'),
	 (2000, 'span2', -4446492996171837732, 'POST /api/orders', 'client', 'span2', 'trace2', 'ERROR', 250000000, 'my-service', 'v1.0'),
	 (3000, 'span3', -4446492996171837732, 'GET /api/products', 'server', 'span3', 'trace3', 'OK', 100000000, 'my-service', 'v1.0'),
	 (4000, 'span4', -4446492996171837732, 'PUT /api/users/123', 'client', 'span4', 'trace4', 'OK', 300000000, 'my-service', 'v1.0');`)

	// Test time range filtering
	leaf := logql.LogLeaf{
		Matchers: []logql.LabelMatch{
			{Label: "span_name", Op: logql.MatchRe, Value: "GET.*"},
		},
	}

	// Query with time range 1500ms-3500ms in milliseconds (should return span 3 - GET /api/products)
	sql := replaceSpansTable(leaf.ToSpansWorkerSQLWithLimit(0, "desc", nil))
	sql = strings.ReplaceAll(sql, "{start}", "1500") // 1500ms
	sql = strings.ReplaceAll(sql, "{end}", "3500")   // 3500ms

	rows := queryAllSpans(t, db, sql)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row in time range [1500ms, 3500ms) matching GET.*, got %d\nsql:\n%s", len(rows), sql)
	}

	// Verify the returned spans are within the time range (chq_timestamp is in milliseconds)
	for _, row := range rows {
		timestamp := getInt64(row["chq_timestamp"])
		if timestamp < 1500 || timestamp >= 3500 {
			t.Fatalf("expected timestamp in range [1500ms, 3500ms), got %dms", timestamp)
		}
	}
}

// Helper functions for spans tests
func mustExecSpans(t *testing.T, db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		t.Fatalf("exec failed: %v\nquery: %s", err, query)
	}
}

func replaceSpansTable(sql string) string {
	// Compute chq_tsns (nanoseconds) from chq_timestamp (milliseconds)
	base := `(SELECT *,
  COALESCE("chq_id", ''::VARCHAR) AS "chq_id",
  COALESCE("span_kind", ''::VARCHAR) AS "span_kind",
  COALESCE("span_name", ''::VARCHAR) AS "span_name",
  COALESCE("span_id", ''::VARCHAR) AS "span_id",
  COALESCE("span_trace_id", ''::VARCHAR) AS "span_trace_id",
  COALESCE("span_status_code", ''::VARCHAR) AS "span_status_code",
  COALESCE("span_duration", 0::BIGINT) AS "span_duration",
  COALESCE("chq_timestamp", 0::BIGINT) AS "chq_timestamp",
  COALESCE("chq_timestamp", 0::BIGINT) * 1000000 AS "chq_tsns",
  COALESCE("chq_fingerprint", -4446492996171837732::BIGINT) AS "chq_fingerprint",
  COALESCE("service_name", ''::VARCHAR) AS "service_name",
  COALESCE("service_version", ''::VARCHAR) AS "service_version"
FROM spans) AS _t`

	// Substitute table and time placeholders.
	sql = strings.ReplaceAll(sql, "{table}", base)
	return sql
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

// Helper functions from ddb_harness_test.go
func replaceStartEnd(sql string) string {
	// Use millisecond values (filtering uses chq_timestamp in ms)
	sql = strings.ReplaceAll(sql, "{start}", "500")
	sql = strings.ReplaceAll(sql, "{end}", "5000")
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

func queryAllSpans(t *testing.T, db *sql.DB, q string) []map[string]any {
	rows, err := db.Query(q)
	if err != nil {
		t.Fatalf("query failed: %v\nsql:\n%s", err, q)
	}
	defer func() { _ = rows.Close() }()

	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("columns failed: %v", err)
	}

	var out []map[string]any
	for rows.Next() {
		raw := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range raw {
			ptrs[i] = &raw[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		m := make(map[string]any, len(cols))
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
