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

package queryapi

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cardinalhq/lakerunner/logql"
	_ "github.com/marcboeker/go-duckdb/v2"
)

func TestIngestExemplarLogsJSONToDuckDB_Smoke(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar data from JSON file
	exemplarData, err := loadExemplarFromJSON("testdata/exemplar.json")
	if err != nil {
		t.Fatalf("load exemplar.json: %v", err)
	}

	// 2) Open in-memory DuckDB and run ingest.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_exemplar"

	n, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, exemplarData)
	if err != nil {
		t.Fatalf("ingest exemplar: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected >0 rows inserted, got %d", n)
	}

	// 3) Verify table exists and row count matches.
	var cnt int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&cnt); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	if cnt < int64(n) {
		t.Fatalf("row count mismatch: table=%d, inserted=%d", cnt, n)
	}

	// 4) Ensure the anchor timestamp column exists.
	hasTS, err := duckHasColumn(ctx, db, table, "chq_timestamp")
	if err != nil {
		t.Fatalf("duckHasColumn: %v", err)
	}
	if !hasTS {
		t.Fatalf("missing required column chq_timestamp")
	}

	// 5) Re-ingest same exemplar; should not error on ALTER, and row count should increase.
	n2, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, exemplarData)
	if err != nil {
		t.Fatalf("re-ingest exemplar: %v", err)
	}
	if n2 <= 0 {
		t.Fatalf("expected >0 rows on re-ingest, got %d", n2)
	}
	var cnt2 int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+quoteIdent(table)).Scan(&cnt2); err != nil {
		t.Fatalf("count rows (2): %v", err)
	}
	if cnt2 < cnt+int64(n2) {
		t.Fatalf("row count did not increase as expected: before=%d after=%d n2=%d", cnt, cnt2, n2)
	}

	// 6) Optional: ensure table has >1 column (ALTERs happened)
	ok, err := duckHasAnyNonTSColumn(ctx, db, table)
	if err != nil {
		t.Fatalf("duckHasAnyNonTSColumn: %v", err)
	}
	if !ok {
		t.Fatalf("expected at least one non-timestamp column after ingest")
	}
}

func TestValidateLogQLAgainstExemplar_AggregateRateCounter(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar data from JSON file
	exemplarData, err := loadExemplarFromJSON("testdata/exemplar1.json")
	if err != nil {
		t.Fatalf("load exemplar1.json: %v", err)
	}

	// 2) Open in-memory DuckDB so we can keep full control in the test.
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_agg_unit"

	// 3) Aggregate LogQL:
	// - Extract 3-digit code (e.g., 401) from the log body
	// - unwrap it as numeric
	// - compute per-second rate of the window sums
	// - sum(...) to force aggregate path
	//
	// This matches the earlier style you use for unwrap+rate_counter.
	q := `sum(rate({resource_service_name="lakerunner-ingest-logs"} | regexp "(?P<code>[0-9]{3})" | unwrap code [10s]))`

	res, err := ValidateLogQLAgainstExemplar(
		ctx,
		q,
		exemplarData,
		WithDB(db),
		WithTable(table),
		WithAggStep(10*time.Second),
	)
	if err != nil {
		t.Fatalf("ValidateLogQLAgainstExemplar: %v", err)
	}

	if res == nil {
		t.Fatalf("nil result")
	}
	if !res.IsAggregate {
		t.Fatalf("expected aggregate path (sum(...)), got non-aggregate")
	}
	if res.InsertedRows <= 0 {
		t.Fatalf("expected >0 inserted rows, got %d", res.InsertedRows)
	}
	if len(res.Rows) == 0 {
		t.Fatalf("expected worker SQL to return rows")
	}
	if res.WorkerSQL == "" {
		t.Fatalf("empty WorkerSQL")
	}
	if strings.Contains(res.WorkerSQL, "{table}") ||
		strings.Contains(res.WorkerSQL, "{start}") ||
		strings.Contains(res.WorkerSQL, "{end}") {
		t.Fatalf("unresolved placeholders in WorkerSQL:\n%s", res.WorkerSQL)
	}
	if res.StartMillis <= 0 || res.EndMillis <= 0 || res.EndMillis < res.StartMillis {
		t.Fatalf("bad [start,end] in result: start=%d end=%d", res.StartMillis, res.EndMillis)
	}

	lcSQL := strings.ToLower(res.WorkerSQL)
	if !strings.Contains(lcSQL, "regexp_extract") || !strings.Contains(lcSQL, "__unwrap_value") {
		t.Fatalf("expected regexp+unwrap pipeline in worker SQL, got:\n%s", res.WorkerSQL)
	}

	t.Logf("worker SQL:\n%s", res.WorkerSQL)
}

func duckHasColumn(ctx context.Context, db *sql.DB, table, col string) (bool, error) {
	// Use function form; only scan 'name' to avoid bool/int scanning issues.
	q := `SELECT name FROM pragma_table_info(` + quoteStringLit(table) + `);`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if name == col {
			return true, nil
		}
	}
	return false, rows.Err()
}

func duckHasAnyNonTSColumn(ctx context.Context, db *sql.DB, table string) (bool, error) {
	q := `SELECT name FROM pragma_table_info(` + quoteStringLit(table) + `);`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return false, err
	}
	defer func() { _ = rows.Close() }()

	hasOther := false
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return false, err
		}
		if name != "chq_timestamp" {
			hasOther = true
			break
		}
	}
	return hasOther, rows.Err()
}

func TestEqualityMatcherValidation_UnitTest(t *testing.T) {
	tests := []struct {
		name          string
		query         string
		shouldFail    bool
		expectedError string
	}{
		{
			name:          "regex-only matcher should fail",
			query:         `{resource_file_name=~".*viasat.*"}`,
			shouldFail:    true,
			expectedError: "at least one equality matcher is required in selector",
		},
		{
			name:          "equality matcher should pass",
			query:         `{resource_file_name="test.log"}`,
			shouldFail:    false,
			expectedError: "",
		},
		{
			name:          "mixed matchers with equality should pass",
			query:         `{resource_file_name="test.log", level=~".*error.*"}`,
			shouldFail:    false,
			expectedError: "",
		},
		{
			name:          "multiple regex matchers should fail",
			query:         `{resource_file_name=~".*viasat.*", level=~".*error.*"}`,
			shouldFail:    true,
			expectedError: "at least one equality matcher is required in selector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the query to get the AST
			ast, err := logql.FromLogQL(tt.query)
			if err != nil {
				t.Fatalf("failed to parse query %q: %v", tt.query, err)
			}

			// Test the validation logic directly
			if ast.LogSel != nil {
				foundAtleastOneEq := false
				for _, m := range ast.LogSel.Matchers {
					foundAtleastOneEq = foundAtleastOneEq || m.Op == logql.MatchEq
				}
				if !foundAtleastOneEq {
					if !tt.shouldFail {
						t.Errorf("expected query %q to pass validation, but it failed", tt.query)
					} else if tt.expectedError != "" {
						// This is the expected behavior - validation should fail
						t.Logf("✓ Query %q correctly failed validation as expected", tt.query)
					}
				} else {
					if tt.shouldFail {
						t.Errorf("expected query %q to fail validation, but it passed", tt.query)
					} else {
						t.Logf("✓ Query %q correctly passed validation as expected", tt.query)
					}
				}
			} else {
				t.Errorf("query %q should have a LogSel", tt.query)
			}
		})
	}
}

func TestHandleLogQLValidate_EqualityMatcherValidation(t *testing.T) {
	qs := &QuerierService{}

	tests := []struct {
		name          string
		query         string
		exemplar      map[string]any
		expectedValid bool
		expectedError string
	}{
		{
			name:          "regex-only matcher without exemplar should fail",
			query:         `{resource_file_name=~".*viasat.*"}`,
			exemplar:      nil,
			expectedValid: false,
			expectedError: "at least one equality matcher is required in selector",
		},
		{
			name:          "equality matcher without exemplar should pass",
			query:         `{resource_file_name="test.log"}`,
			exemplar:      nil,
			expectedValid: true,
			expectedError: "",
		},
		{
			name:          "mixed matchers without exemplar should pass",
			query:         `{resource_file_name="test.log", level=~".*error.*"}`,
			exemplar:      nil,
			expectedValid: true,
			expectedError: "",
		},
		{
			name:          "multiple regex matchers without exemplar should fail",
			query:         `{resource_file_name=~".*viasat.*", level=~".*error.*"}`,
			exemplar:      nil,
			expectedValid: false,
			expectedError: "at least one equality matcher is required in selector",
		},
		{
			name:          "regex-only matcher with empty exemplar should fail",
			query:         `{resource_file_name=~".*viasat.*"}`,
			exemplar:      map[string]any{},
			expectedValid: false,
			expectedError: "at least one equality matcher is required in selector",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create request body
			reqBody := logQLValidateRequest{
				Query:    tt.query,
				Exemplar: tt.exemplar,
			}
			jsonBody, _ := json.Marshal(reqBody)

			// Create HTTP request
			req := httptest.NewRequest("POST", "/api/v1/logql/validate", bytes.NewReader(jsonBody))
			req.Header.Set("Content-Type", "application/json")

			// Create response recorder
			w := httptest.NewRecorder()

			// Call the handler
			qs.handleLogQLValidate(w, req)

			// Parse response - handle both success and error response types
			responseBody := w.Body.Bytes()

			if tt.expectedValid {
				// For valid responses, expect logQLValidateResponse
				var response logQLValidateResponse
				if err := json.Unmarshal(responseBody, &response); err != nil {
					t.Fatalf("Failed to parse success response JSON: %v", err)
				}
				if response.Valid != tt.expectedValid {
					t.Errorf("Expected valid=%v, got valid=%v", tt.expectedValid, response.Valid)
				}
				if response.Error != "" {
					t.Errorf("Expected no error, got: %s", response.Error)
				}
			} else {
				// For invalid responses, expect APIError
				var response APIError
				if err := json.Unmarshal(responseBody, &response); err != nil {
					t.Fatalf("Failed to parse error response JSON: %v", err)
				}
				if w.Code != 400 {
					t.Errorf("Expected status 400, got %d", w.Code)
				}
				if response.Code != ValidationFailed {
					t.Errorf("Expected code VALIDATION_FAILED, got %s", response.Code)
				}
				if !strings.Contains(response.Message, tt.expectedError) {
					t.Errorf("Expected error containing %q, got: %s", tt.expectedError, response.Message)
				}
			}
		})
	}
}
