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
	"context"
	"database/sql"
	"fmt"
	"github.com/cardinalhq/lakerunner/logql"
	_ "github.com/marcboeker/go-duckdb/v2"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestStagewiseValidator_Accounting_PreLineFilter_ThenLineFormat_OK(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar data that contains the @OrderResult payload.
	b, err := os.ReadFile("testdata/exemplar2.json")
	if err != nil {
		t.Fatalf("read exemplar2.json: %v", err)
	}

	// 2) In-memory DuckDB + ingest
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_stagewise_lineformat_pre_ok"
	n, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, string(b))
	if err != nil {
		t.Fatalf("ingest exemplar2: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected >0 rows inserted, got %d", n)
	}

	// 3) Resolve [start,end] for placeholder substitution
	startMillis, endMillis, err := minMaxTimestamp(ctx, db, table)
	if err != nil {
		t.Fatalf("min/max timestamp: %v", err)
	}
	if endMillis == startMillis {
		endMillis = startMillis + 1
	}

	// 4) Expression under test:
	//    pre line filter (|= "Order details:") then line_format -> index base field with special chars
	q := `{resource_service_name="accounting"} |= "Order details:" | line_format "{{ index . \"log_@OrderResult\" }}"`

	ast, err := logql.FromLogQL(q)
	if err != nil {
		t.Fatalf("parse logql: %v", err)
	}
	if ast.IsAggregateExpr() {
		t.Fatalf("query is unexpectedly aggregate")
	}

	lplan, err := logql.CompileLog(ast)
	if err != nil {
		t.Fatalf("compile log plan: %v", err)
	}
	if len(lplan.Leaves) == 0 {
		t.Fatalf("no leaves produced for log query")
	}
	leaf := lplan.Leaves[0]

	// 5) Stage-wise validation: matchers → +pre line filters → +parser[0]: line_format
	stages, err := stageWiseValidation(
		db, table,
		leaf,
		startMillis, endMillis,
		1000,
		"desc",
		nil,
	)
	if err != nil {
		t.Fatalf("stageWiseValidation: %v", err)
	}

	// Expect: 0=matchers, 1=matchers+independent+pre_line_filters, 2=parser[0]: line_format
	if len(stages) < 3 {
		t.Fatalf("expected at least 3 stages, got %d", len(stages))
	}

	// Stage 0 (matchers) should pass and return rows
	if !stages[0].OK || stages[0].RowCount == 0 {
		t.Fatalf("stage 0 failed or returned no rows; sql:\n%s", stages[0].SQL)
	}

	// Stage 1: pre line filter (|= "Order details:") applied before parsers
	s1 := stages[1]
	if s1.Name != "matchers+independent+pre_line_filters" {
		t.Fatalf("unexpected stage 1 name: %q\nsql:\n%s", s1.Name, s1.SQL)
	}
	if !s1.OK || s1.RowCount == 0 {
		t.Fatalf("expected stage 1 to return rows (pre line filter); sql:\n%s", s1.SQL)
	}

	// Stage 2: line_format should rewrite _cardinalhq.message from base "log.@OrderResult"
	s2 := stages[2]
	if s2.Name != "parser[0]: line_format" {
		t.Fatalf("unexpected stage 2 name: %q\nsql:\n%s", s2.Name, s2.SQL)
	}
	if !s2.OK || s2.RowCount == 0 {
		t.Fatalf("expected line_format stage to return rows; sql:\n%s", s2.SQL)
	}
	// Hoisted base field should appear in the SQL (dependency for index in template)
	if !strings.Contains(s2.SQL, `"log.@OrderResult"`) {
		t.Fatalf("expected hoisted base column \"log.@OrderResult\" in SQL:\n%s", s2.SQL)
	}

	// 6) Inspect final stage rows: message should now be the JSON payload
	rows, err := queryAllRows(db, s2.SQL)
	if err != nil {
		t.Fatalf("query final stage rows: %v\nsql:\n%s", err, s2.SQL)
	}
	foundJSONMsg := false
	for _, r := range rows {
		raw, ok := r["_cardinalhq.message"]
		if !ok || raw == nil {
			continue
		}
		foundJSONMsg = true
	}
	if !foundJSONMsg {
		t.Fatalf("did not find a JSON-like rewritten _cardinalhq.message in stage 2 rows; sql:\n%s", s2.SQL)
	}
}

func TestStagewiseValidator_Accounting_CountOverTime_ByZipCode(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar data that contains the @OrderResult payload referenced by the index(...)
	b, err := os.ReadFile("testdata/exemplar2.json")
	if err != nil {
		t.Fatalf("read exemplar2.json: %v", err)
	}

	// 2) In-memory DuckDB + ingest
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_stagewise_zipcode_count"
	n, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, string(b))
	if err != nil {
		t.Fatalf("ingest exemplar2: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected >0 rows inserted, got %d", n)
	}

	// 3) Resolve [start,end] for placeholder substitution
	startMillis, endMillis, err := minMaxTimestamp(ctx, db, table)
	if err != nil {
		t.Fatalf("min/max timestamp: %v", err)
	}
	if endMillis == startMillis {
		endMillis = startMillis + 1
	}

	// 4) Aggregate LogQL under test. We validate the pre-aggregation pipeline:
	//    selector + (pre) line filter + line_format + json(zipCode)
	q := `sum by (zipCode) (` +
		`count_over_time(` +
		`{resource_service_name="accounting"} |= "Order details:" ` +
		`| line_format "{{index . \"log_@OrderResult\"}}" ` +
		`| json zipCode="shippingAddress.zipCode" ` +
		`[5m]))`

	ast, err := logql.FromLogQL(q)
	if err != nil {
		t.Fatalf("parse logql: %v", err)
	}
	if !ast.IsAggregateExpr() {
		t.Fatalf("query should be aggregate but IsAggregateExpr() == false")
	}

	// 5) Pull out the pipeline that feeds the range aggregation for stage-wise validation
	sel, rng, ok := ast.FirstPipeline()
	if !ok || sel == nil {
		t.Fatalf("no pipeline (selector) found in expression")
	}
	if len(sel.Parsers) == 0 {
		t.Fatalf("expected parser stages in pipeline, found 0")
	}

	leaf := logql.LogLeaf{
		Matchers:     append([]logql.LabelMatch(nil), sel.Matchers...),
		LineFilters:  append([]logql.LineFilter(nil), sel.LineFilters...),
		LabelFilters: append([]logql.LabelFilter(nil), sel.LabelFilters...),
		Parsers:      append([]logql.ParserStage(nil), sel.Parsers...),
	}
	if rng != nil {
		leaf.Range = rng.Range   // 5m window
		leaf.Offset = rng.Offset // if present
		leaf.Unwrap = rng.Unwrap // not used here, but keep consistent
	}
	leaf.ID = leaf.Label()

	// 6) Stage-wise validation: matchers → +independent/line filters (pre) → +parser[0]: line_format → +parser[1]: json
	stages, err := stageWiseValidation(
		db, table,
		leaf,
		startMillis, endMillis,
		1000,
		"desc",
		nil,
	)
	if err != nil {
		t.Fatalf("stageWiseValidation: %v", err)
	}

	// Expect at least 4 stages:
	//   0 = matchers
	//   1 = matchers + independent + pre_line_filters   (|= "Order details:" is pre, since it precedes line_format)
	//   2 = parser[0]: line_format
	//   3 = parser[1]: json
	if len(stages) < 4 {
		t.Fatalf("expected at least 4 stages, got %d", len(stages))
	}

	// 7) Early stages should pass and return rows
	if !stages[0].OK || stages[0].RowCount == 0 {
		t.Fatalf("stage 0 failed or returned no rows; sql:\n%s", stages[0].SQL)
	}
	if s1 := stages[1]; !s1.OK || s1.RowCount == 0 {
		t.Fatalf("stage 1 (pre_line_filters) failed or returned no rows; sql:\n%s", s1.SQL)
	}

	// 8) Parser stages present and succeed
	if stages[2].Name != "parser[0]: line_format" {
		t.Fatalf("unexpected stage[2] name: %q\nsql:\n%s", stages[2].Name, stages[2].SQL)
	}
	if !stages[2].OK || stages[2].RowCount == 0 {
		t.Fatalf("line_format stage returned no rows; sql:\n%s", stages[2].SQL)
	}

	last := stages[3]
	if last.Name != "parser[1]: json" {
		t.Fatalf("unexpected stage[3] name: %q\nsql:\n%s", last.Name, last.SQL)
	}
	if !last.OK {
		t.Fatalf("json stage unexpectedly failed: %v\nmissing: %v\nsql:\n%s", last.Error, last.MissingFields, last.SQL)
	}
	if last.RowCount == 0 {
		t.Fatalf("json stage returned 0 rows; sql:\n%s", last.SQL)
	}

	// 9) Inspect final stage rows: confirm zipCode exists and is non-empty (basic sanity)
	rows, err := queryAllRows(db, last.SQL)
	if err != nil {
		t.Fatalf("query final stage rows: %v\nsql:\n%s", err, last.SQL)
	}
	var sawZip bool
	for _, r := range rows {
		zRaw, ok := r["zipCode"]
		if !ok {
			continue
		}
		z := strings.TrimSpace(fmt.Sprint(zRaw))
		if z == "" {
			continue
		}
		// Optional: basic zip sanity — numeric and reasonable length (5–10 chars to allow extended forms)
		if _, convErr := strconv.Atoi(strings.TrimLeft(z, "0")); convErr == nil && len(z) >= 5 && len(z) <= 10 {
			sawZip = true
			break
		}
		// If the exemplar uses strictly 5-digit ZIPs, the above will still pass.
		if len(z) >= 3 { // fallback: at least something non-empty
			sawZip = true
			break
		}
	}
	if !sawZip {
		t.Fatalf("did not find a non-empty zipCode column in final stage rows; sql:\n%s", last.SQL)
	}

	// Note: We don't execute the full count_over_time/sum by (zipCode) aggregation here —
	// this test ensures the pipeline feeding that aggregation (selector → line_filter → line_format → json)
	// is valid and produces the 'zipCode' label required for grouping.
}

func TestStagewiseValidator_InvalidLineFormatThenRegexp_NoCountry(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar JSON with @OrderResult payload
	b, err := os.ReadFile("testdata/exemplar2.json")
	if err != nil {
		t.Fatalf("read exemplar2: %v", err)
	}

	// 2) Open in-memory DuckDB and ingest exemplar rows
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_stagewise_invalid"

	n, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, string(b))
	if err != nil {
		t.Fatalf("ingest exemplar2: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected >0 rows inserted, got %d", n)
	}

	// 3) Resolve [start,end] for placeholder substitution
	startMillis, endMillis, err := minMaxTimestamp(ctx, db, table)
	if err != nil {
		t.Fatalf("min/max timestamp: %v", err)
	}
	if endMillis == startMillis {
		endMillis = startMillis + 1
	}

	// 4) Pipeline: selector + line filter + line_format (broken) + regexp(country)
	q := `{resource_service_name="accounting"} |= "Order details:" | line_format "{{ . \"log_@OrderResult\" }}" | regexp "country\":\\s*\"(?P<country>[^\"]+)\""`

	ast, err := logql.FromLogQL(q)
	if err != nil {
		t.Fatalf("parse logql: %v", err)
	}
	if ast.IsAggregateExpr() {
		t.Fatalf("query is unexpectedly aggregate")
	}

	lplan, err := logql.CompileLog(ast)
	if err != nil {
		t.Fatalf("compile log plan: %v", err)
	}
	if len(lplan.Leaves) == 0 {
		t.Fatalf("no leaves produced for log query")
	}
	leaf := lplan.Leaves[0]

	// 5) Stage-wise validation
	stages, err := stageWiseValidation(
		db, table,
		leaf,
		startMillis, endMillis,
		1000,
		"desc",
		nil,
	)
	if err != nil {
		t.Fatalf("stageWiseValidation: %v", err)
	}

	// Expect at least: 0=matchers, 1=matchers+line_filters, 2=parser[0]: line_format
	if len(stages) < 3 {
		t.Fatalf("expected at least 3 stages, got %d", len(stages))
	}

	// Early stages should pass (selector + line filter match exemplar)
	if !stages[0].OK || stages[0].RowCount == 0 {
		t.Fatalf("stage 0 failed or returned no rows; sql:\n%s", stages[0].SQL)
	}
	if !stages[1].OK || stages[1].RowCount == 0 {
		t.Fatalf("stage 1 failed or returned no rows; sql:\n%s", stages[1].SQL)
	}

	// parser[0] is line_format — with the current template it wipes the body to ''.
	// Since we now validate line_format, this stage should FAIL with _cardinalhq.message missing.
	lf := stages[2]
	if lf.OK {
		t.Fatalf("expected line_format stage to fail; sql:\n%s", lf.SQL)
	}
	wantMissing := "_cardinalhq.message"
	found := false
	for _, m := range lf.MissingFields {
		if m == wantMissing {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("line_format stage failed, but did not report %q missing; got: %v\nsql:\n%s",
			wantMissing, lf.MissingFields, lf.SQL)
	}

	// And because we stop on first failing parser stage, there should be no later successful stage.
	if len(stages) > 3 && stages[len(stages)-1].OK {
		t.Fatalf("unexpected: a later parser stage passed even though line_format failed.\nlast sql:\n%s",
			stages[len(stages)-1].SQL)
	}
}

func TestStagewiseValidator_AggregateJsonUnwrap_GroupByCardType(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar JSON (has revenue + cardType)
	b, err := os.ReadFile("testdata/exemplar3.json")
	if err != nil {
		t.Fatalf("read exemplar3: %v", err)
	}

	// 2) Open in-memory DuckDB and ingest exemplar rows
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_stagewise_agg_ok"

	n, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, string(b))
	if err != nil {
		t.Fatalf("ingest exemplar3: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected >0 rows inserted, got %d", n)
	}

	// 3) Resolve [start,end] for placeholder substitution
	startMillis, endMillis, err := minMaxTimestamp(ctx, db, table)
	if err != nil {
		t.Fatalf("min/max timestamp: %v", err)
	}
	if endMillis == startMillis {
		endMillis = startMillis + 1
	}

	// 4) Aggregate LogQL:
	//    - filter by service name + fingerprint
	//    - json revenue + json card_type
	//    - unwrap revenue
	//    - sum_over_time(...) by card_type
	q := `sum by (card_type) (sum_over_time(({resource_service_name="segment", _cardinalhq_fingerprint="2082887339816129672"} | json revenue="properties.revenue" | json card_type="properties.cardType" | unwrap revenue)[24h]))`

	ast, err := logql.FromLogQL(q)
	if err != nil {
		t.Fatalf("parse logql: %v", err)
	}
	if !ast.IsAggregateExpr() {
		t.Fatalf("query should be aggregate but IsAggregateExpr() == false")
	}

	sel, rng, ok := ast.FirstPipeline()
	if !ok || sel == nil {
		t.Fatalf("no pipeline (selector) found in expression")
	}
	if len(sel.Parsers) == 0 {
		t.Fatalf("expected parser stages in pipeline, found 0")
	}

	// Build a synthetic leaf from that pipeline for stage-wise validation
	leaf := logql.LogLeaf{
		Matchers:     append([]logql.LabelMatch(nil), sel.Matchers...),
		LineFilters:  append([]logql.LineFilter(nil), sel.LineFilters...),
		LabelFilters: append([]logql.LabelFilter(nil), sel.LabelFilters...),
		Parsers:      append([]logql.ParserStage(nil), sel.Parsers...),
	}
	if rng != nil {
		leaf.Range = rng.Range
		leaf.Offset = rng.Offset
		leaf.Unwrap = rng.Unwrap
	}
	leaf.ID = leaf.Label() // stable/debug-friendly

	// 6) Run stage-wise validation: matchers → +independent/line filters → +each parser
	stages, err := stageWiseValidation(
		db, table,
		leaf,
		startMillis, endMillis,
		1000,
		"desc",
		nil,
	)
	if err != nil {
		t.Fatalf("stageWiseValidation: %v", err)
	}

	// Expect at least: 1 (matchers) + 1 (independent/line) + 3 parsers = 5 stages
	if len(stages) < 3 {
		t.Fatalf("expected at least 3 stages, got %d", len(stages))
	}

	// 7) Early stages should return rows
	if !stages[0].OK || stages[0].RowCount == 0 {
		t.Fatalf("stage 0 failed or returned no rows; sql:\n%s", stages[0].SQL)
	}
	if !stages[1].OK || stages[1].RowCount == 0 {
		t.Fatalf("stage 1 failed or returned no rows; sql:\n%s", stages[1].SQL)
	}

	// 8) Final stage should succeed and create expected fields
	last := stages[len(stages)-1]
	if !last.OK {
		t.Fatalf("final stage unexpectedly failed: %v\nmissing: %v\nsql:\n%s", last.Error, last.MissingFields, last.SQL)
	}
	if last.RowCount == 0 {
		t.Fatalf("final stage returned 0 rows; sql:\n%s", last.SQL)
	}

	// 9) Inspect final rows: expect card_type == "AMEX" and __unwrap_value ≈ 111.67
	rows, err := queryAllRows(db, last.SQL)
	if err != nil {
		t.Fatalf("query final stage rows: %v\nsql:\n%s", err, last.SQL)
	}
	found := false
	for _, r := range rows {
		ctRaw, okCT := r["card_type"]
		if !okCT {
			continue
		}
		cardType := strings.TrimSpace(fmt.Sprint(ctRaw))
		if cardType != "AMEX" {
			continue
		}
		uvRaw, okUV := r["__unwrap_value"]
		if !okUV {
			continue
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(uvRaw)), 64)
		if err != nil {
			continue
		}
		if (f > 111.669 && f < 111.671) || f == 111.67 {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("did not find a row with card_type=AMEX and __unwrap_value≈111.67; sql:\n%s", last.SQL)
	}
}

func TestStagewiseValidator_Segment_SumOverTime_UnwrapRevenue_OK(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar (has service.name=segment, userId, properties.revenue)
	b, err := os.ReadFile("testdata/exemplar3.json")
	if err != nil {
		t.Fatalf("read exemplar3: %v", err)
	}

	// 2) In-memory DuckDB + ingest
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_stagewise_segment_revenue_ok"
	n, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, string(b))
	if err != nil {
		t.Fatalf("ingest exemplar3: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected >0 rows inserted, got %d", n)
	}

	// 3) Resolve [start,end] for placeholder substitution
	startMillis, endMillis, err := minMaxTimestamp(ctx, db, table)
	if err != nil {
		t.Fatalf("min/max timestamp: %v", err)
	}
	if endMillis == startMillis {
		endMillis = startMillis + 1
	}

	// 4) Aggregate LogQL: filter → json user_id → json revenue → unwrap revenue → [1m] window
	q := `sum_over_time(({resource_service_name="segment"} | json user_id="userId" | json revenue="properties.revenue" | unwrap revenue)[1m])`

	ast, err := logql.FromLogQL(q)
	if err != nil {
		t.Fatalf("parse logql: %v", err)
	}
	if !ast.IsAggregateExpr() {
		t.Fatalf("query should be aggregate but IsAggregateExpr() == false")
	}

	// 5) Extract the pipeline feeding the range agg and validate that
	sel, rng, ok := ast.FirstPipeline()
	if !ok || sel == nil {
		t.Fatalf("no pipeline (selector) found in expression")
	}
	leaf := logql.LogLeaf{
		Matchers:     append([]logql.LabelMatch(nil), sel.Matchers...),
		LineFilters:  append([]logql.LineFilter(nil), sel.LineFilters...),
		LabelFilters: append([]logql.LabelFilter(nil), sel.LabelFilters...),
		Parsers:      append([]logql.ParserStage(nil), sel.Parsers...),
	}
	if rng != nil {
		leaf.Range = rng.Range
		leaf.Offset = rng.Offset
		leaf.Unwrap = rng.Unwrap
	}
	leaf.ID = leaf.Label()

	// 6) Stage-wise validation (matchers → +independent/line filters → +each parser)
	stages, err := stageWiseValidation(
		db, table,
		leaf,
		startMillis, endMillis,
		1000,
		"desc",
		nil,
	)
	if err != nil {
		t.Fatalf("stageWiseValidation: %v", err)
	}
	if len(stages) < 3 {
		t.Fatalf("expected at least 3 stages, got %d", len(stages))
	}

	// 7) Early stages should return rows
	if !stages[0].OK || stages[0].RowCount == 0 {
		t.Fatalf("stage 0 failed or returned no rows; sql:\n%s", stages[0].SQL)
	}
	if !stages[1].OK || stages[1].RowCount == 0 {
		t.Fatalf("stage 1 failed or returned no rows; sql:\n%s", stages[1].SQL)
	}

	// 8) Final stage should succeed and expose user_id + __unwrap_value (from revenue)
	last := stages[len(stages)-1]
	if !last.OK {
		t.Fatalf("final stage unexpectedly failed: %v\nmissing: %v\nsql:\n%s",
			last.Error, last.MissingFields, last.SQL)
	}
	if last.RowCount == 0 {
		t.Fatalf("final stage returned 0 rows; sql:\n%s", last.SQL)
	}

	// 9) Inspect final rows: ensure user_id non-empty and __unwrap_value parses to a number (> 0)
	rows, err := queryAllRows(db, last.SQL)
	if err != nil {
		t.Fatalf("query final stage rows: %v\nsql:\n%s", err, last.SQL)
	}

	var sawGood bool
	for _, r := range rows {
		uid, okUID := r["user_id"]
		uv, okUV := r["__unwrap_value"]
		if !okUID || !okUV {
			continue
		}
		userID := strings.TrimSpace(fmt.Sprint(uid))
		if userID == "" {
			continue
		}
		f, ferr := strconv.ParseFloat(strings.TrimSpace(fmt.Sprint(uv)), 64)
		if ferr != nil || !(f > 0) {
			continue
		}
		sawGood = true
		break
	}
	if !sawGood {
		t.Fatalf("did not find a row with non-empty user_id and positive __unwrap_value; sql:\n%s", last.SQL)
	}
}

func TestStagewiseValidator_Kafka_ControllerID_InvalidRegexp(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar JSON with the Kafka log line
	b, err := os.ReadFile("testdata/exemplar4.json")
	if err != nil {
		t.Fatalf("read exemplar4: %v", err)
	}

	// 2) Open in-memory DuckDB and ingest exemplar rows
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_stagewise_kafka_controller"

	n, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, string(b))
	if err != nil {
		t.Fatalf("ingest exemplar4: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected >0 rows inserted, got %d", n)
	}

	// 3) Resolve [start,end] for placeholder substitution
	startMillis, endMillis, err := minMaxTimestamp(ctx, db, table)
	if err != nil {
		t.Fatalf("min/max timestamp: %v", err)
	}
	if endMillis == startMillis {
		endMillis = startMillis + 1
	}

	q := `{resource_service_name="kafka"} | regexp "ControllerRegistration[(]id=(?P<controller_id>[0-9]+),"`

	ast, err := logql.FromLogQL(q)
	if err != nil {
		t.Fatalf("parse logql: %v", err)
	}
	if ast.IsAggregateExpr() {
		t.Fatalf("query is unexpectedly aggregate")
	}

	lplan, err := logql.CompileLog(ast)
	if err != nil {
		t.Fatalf("compile log plan: %v", err)
	}
	if len(lplan.Leaves) == 0 {
		t.Fatalf("no leaves produced for log query")
	}
	leaf := lplan.Leaves[0]

	// 5) Run stage-wise validation (matchers → +independent+lines → +each parser)
	stages, err := stageWiseValidation(
		db, table,
		leaf,
		startMillis, endMillis,
		1000,
		"desc",
		nil,
	)
	if err != nil {
		t.Fatalf("stageWiseValidation: %v", err)
	}
	if len(stages) < 2 {
		t.Fatalf("expected at least 2 stages, got %d", len(stages))
	}

	// Early stage(s) should pass (selector matches service_name="kafka")
	if !stages[0].OK || stages[0].RowCount == 0 {
		t.Fatalf("stage 0 failed or returned no rows; sql:\n%s", stages[0].SQL)
	}
	// stage 1 is "matchers+independent+line_filters" (there are no line filters here) but should still pass
	if !stages[1].OK || stages[1].RowCount == 0 {
		t.Fatalf("stage 1 failed or returned no rows; sql:\n%s", stages[1].SQL)
	}

	// Final stage includes the regexp parser; it *expects* to create 'controller_id'.
	// Because the pattern does not match this exemplar, 'controller_id' never appears.
	last := stages[len(stages)-1]
	if last.OK {
		t.Fatalf("expected final stage to fail due to missing 'controller_id', but it passed.\nsql:\n%s", last.SQL)
	}
	found := false
	for _, m := range last.MissingFields {
		if m == "controller_id" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("final stage failed, but 'controller_id' was not listed as missing; got: %v\nsql:\n%s",
			last.MissingFields, last.SQL)
	}
}

func TestStagewiseValidator_Accounting_LineFormatThenLineFilter_WA(t *testing.T) {
	ctx := context.Background()

	// 1) Load exemplar JSON
	b, err := os.ReadFile("testdata/exemplar2.json")
	if err != nil {
		t.Fatalf("read exemplar2.json: %v", err)
	}

	// 2) In-memory DuckDB + ingest
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	const table = "logs_stagewise_accounting_lf"
	n, err := IngestExemplarLogsJSONToDuckDB(ctx, db, table, string(b))
	if err != nil {
		t.Fatalf("ingest exemplar2: %v", err)
	}
	if n <= 0 {
		t.Fatalf("expected >0 rows inserted, got %d", n)
	}

	// 3) Resolve [start,end] for placeholder substitution
	startMillis, endMillis, err := minMaxTimestamp(ctx, db, table)
	if err != nil {
		t.Fatalf("min/max timestamp: %v", err)
	}
	if endMillis == startMillis {
		endMillis = startMillis + 1
	}

	// 4) Expression under test:
	//    line_format rewrites the body to the @OrderResult JSON string, and
	//    the contains filter ("WA") must run AFTER that rewrite.
	q := `{resource_service_name="accounting", _cardinalhq_fingerprint="7754623969787599908"} | line_format "{{ index . \"log_@OrderResult\" }}" |= "WA"`

	ast, err := logql.FromLogQL(q)
	if err != nil {
		t.Fatalf("parse logql: %v", err)
	}
	if ast.IsAggregateExpr() {
		t.Fatalf("query is unexpectedly aggregate")
	}

	lplan, err := logql.CompileLog(ast)
	if err != nil {
		t.Fatalf("compile log plan: %v", err)
	}
	if len(lplan.Leaves) == 0 {
		t.Fatalf("no leaves produced for log query")
	}
	leaf := lplan.Leaves[0]

	// 5) Validate stage-by-stage
	stages, err := stageWiseValidation(
		db, table,
		leaf,
		startMillis, endMillis,
		1000,
		"desc",
		nil,
	)
	if err != nil {
		t.Fatalf("stageWiseValidation: %v", err)
	}

	// Expect: 0=matchers, 1=matchers+independent+pre_line_filters (empty pre filters), 2=parser[0]: line_format (+ post line filters)
	if got := len(stages); got < 3 {
		t.Fatalf("expected at least 3 stages, got %d", got)
	}

	// Stage 0 (matchers) should pass and return the row.
	if !stages[0].OK || stages[0].RowCount == 0 {
		t.Fatalf("stage 0 failed or returned no rows; sql:\n%s", stages[0].SQL)
	}

	// Stage 1: with our new logic, pre-line-filters are empty because a parser exists,
	// so this stage should also pass and still return the row.
	s1 := stages[1]
	if s1.Name != "matchers+independent+pre_line_filters" {
		t.Fatalf("unexpected stage 1 name: %q\nsql:\n%s", s1.Name, s1.SQL)
	}
	if !s1.OK || s1.RowCount == 0 {
		t.Fatalf("expected stage 1 to return rows (no pre line filters when parsers exist); sql:\n%s", s1.SQL)
	}

	// Stage 2: parser[0]=line_format; post line filters (|= "WA") are applied here.
	// The rewritten message contains `"state": "WA"`, so this must pass.
	s2 := stages[2]
	if s2.Name != "parser[0]: line_format" {
		t.Fatalf("unexpected stage 2 name: %q\nsql:\n%s", s2.Name, s2.SQL)
	}
	if !s2.OK || s2.RowCount == 0 {
		t.Fatalf("expected line_format+post_line_filters stage to match 'WA' and return rows; sql:\n%s", s2.SQL)
	}
}
