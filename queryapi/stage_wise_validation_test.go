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

	// 4) Invalid pipeline: line_format sets the message incorrectly,
	//    then regexp tries to capture `country` from (now wrong) message.
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
	if len(stages) < 3 {
		t.Fatalf("expected at least 3 stages, got %d", len(stages))
	}

	// 6) Early stages should pass (selector + line filter match exemplar)
	if !stages[0].OK || stages[0].RowCount == 0 {
		t.Fatalf("stage 0 failed or returned no rows; sql:\n%s", stages[0].SQL)
	}
	if !stages[1].OK || stages[1].RowCount == 0 {
		t.Fatalf("stage 1 failed or returned no rows; sql:\n%s", stages[1].SQL)
	}

	// 7) Final stage has the regexp parser; it *expects* to create 'country'.
	//    Because the preceding line_format broke the message, 'country' never appears.
	last := stages[len(stages)-1]
	if last.OK {
		t.Fatalf("expected final stage to fail due to missing 'country', but it passed.\nsql:\n%s", last.SQL)
	}
	foundCountryInMissing := false
	for _, c := range last.MissingFields {
		if c == "country" {
			foundCountryInMissing = true
			break
		}
	}
	if !foundCountryInMissing {
		t.Fatalf("final stage failed, but 'country' was not listed as missing; got: %v\nsql:\n%s", last.MissingFields, last.SQL)
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
