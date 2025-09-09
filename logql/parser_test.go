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
	"strings"
	"testing"
	"time"
)

// helper: find label matcher (label -> value) in a selector
func hasMatcher(ms []LabelMatch, label, value string) bool {
	for _, m := range ms {
		if m.Label == label && m.Value == value {
			return true
		}
	}
	return false
}

func TestLogRange(t *testing.T) {
	q := `count_over_time({app="api"}[5m] offset 1m)`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	if ast.Kind != KindRangeAgg {
		t.Fatalf("kind = %s, want %s", ast.Kind, KindRangeAgg)
	}
	if ast.RangeAgg == nil || ast.RangeAgg.Left.Range != "5m" || ast.RangeAgg.Left.Offset != "1m" {
		t.Fatalf("bad range/offset: %+v", ast.RangeAgg.Left)
	}
	if !hasMatcher(ast.RangeAgg.Left.Selector.Matchers, "app", "api") {
		t.Fatalf("missing matcher app=api!")
	}
}

func TestNumericComparisonsOnParserFilters(t *testing.T) {
	expressions := []string{
		`{resource_service_name="kafka"} | regexp "(?P<dur>[0-9]+(?:\\.[0-9]+)?)\\s*(?:ns|us|µs|ms|s|m|h)" | dur > 0`,
		`{resource_service_name="kafka"} | regexp "(?P<dur>[0-9]+(?:\\.[0-9]+)?)\\s*(?:ns|us|µs|ms|s|m|h)" | dur < 0`,
		`{resource_service_name="kafka"} | regexp "(?P<dur>[0-9]+(?:\\.[0-9]+)?)\\s*(?:ns|us|µs|ms|s|m|h)" | dur >= 0`,
		`{resource_service_name="kafka"} | regexp "(?P<dur>[0-9]+(?:\\.[0-9]+)?)\\s*(?:ns|us|µs|ms|s|m|h)" | dur <= 0`,
		`{resource_service_name="kafka"} | regexp "(?P<dur>[0-9]+(?:\\.[0-9]+)?)\\s*(?:ns|us|µs|ms|s|m|h)" | dur = 0`,
	}

	for _, q := range expressions {
		ast, err := FromLogQL(q)
		if err != nil {
			t.Fatalf("FromLogQL() error: %v", err)
		}
		if ast.Kind != KindLogSelector {
			t.Fatalf("kind = %s, want %s", ast.Kind, KindLogSelector)
		}
		if ast.LogSel == nil {
			t.Fatalf("LogSel is nil")
		}
		if len(ast.LogSel.Parsers) != 1 {
			t.Fatalf("expected 1 parser, got %d: %#v", len(ast.LogSel.Parsers), ast.LogSel.Parsers)
		}
		p := ast.LogSel.Parsers[0]
		if len(p.Filters) != 1 {
			t.Fatalf("expected 1 filter, got %d: %#v", len(p.Filters), p.Filters)
		}
	}
}

func TestRangeAggregationRate(t *testing.T) {
	q := `rate({app="a"}[5m])`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	if ast.Kind != KindRangeAgg {
		t.Fatalf("kind = %s, want %s", ast.Kind, KindRangeAgg)
	}
	if ast.RangeAgg == nil {
		t.Fatalf("RangeAgg is nil")
	}
	if ast.RangeAgg.Op != "rate" {
		t.Fatalf("op = %q, want %q", ast.RangeAgg.Op, "rate")
	}
	if ast.RangeAgg.Left.Range != "5m" {
		t.Fatalf("left.range = %q, want %q", ast.RangeAgg.Left.Range, "5m")
	}
}

func TestVectorAggregationSumBy(t *testing.T) {
	q := `sum by (cluster) (rate({app="a"}[5m]))`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	if ast.Kind != KindVectorAgg {
		t.Fatalf("kind = %s, want %s", ast.Kind, KindVectorAgg)
	}
	if ast.VectorAgg == nil {
		t.Fatalf("VectorAgg is nil")
	}
	// We set Op from the first token of the string, which will be "sum".
	if ast.VectorAgg.Op != "sum" {
		t.Fatalf("op = %q, want %q", ast.VectorAgg.Op, "sum")
	}
	// Left should be a RangeAgg(rate(...)).
	if ast.VectorAgg.Left.Kind != KindRangeAgg {
		t.Fatalf("left.kind = %s, want %s", ast.VectorAgg.Left.Kind, KindRangeAgg)
	}
	// Grouping
	foundCluster := false
	for _, l := range ast.VectorAgg.By {
		if l == "cluster" {
			foundCluster = true
			break
		}
	}
	if !foundCluster {
		t.Fatalf("sum by (cluster): cluster not found in By = %#v", ast.VectorAgg.By)
	}
}

func TestBinaryOpDivideByScalar(t *testing.T) {
	q := `sum(rate({app="a"}[1m])) / 2`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	if ast.Kind != KindBinOp {
		t.Fatalf("kind = %s, want %s", ast.Kind, KindBinOp)
	}
	if ast.BinOp == nil {
		t.Fatalf("BinOp is nil")
	}
	if ast.BinOp.Op != "/" {
		t.Fatalf("op = %q, want %q", ast.BinOp.Op, "/")
	}
	// Left is a sample expression; our converter returns a nested LogAST.
	if ast.BinOp.LHS.Kind == KindOpaque {
		t.Fatalf("unexpected opaque LHS")
	}
	// Right should be a numeric vector literal.
	if ast.BinOp.RHS.Kind != KindVector || ast.BinOp.RHS.Vector == nil || ast.BinOp.RHS.Vector.Literal == nil {
		t.Fatalf("rhs not a numeric vector literal: %#v", ast.BinOp.RHS)
	}
	if *ast.BinOp.RHS.Vector.Literal != 2 {
		t.Fatalf("rhs literal = %v, want 2", *ast.BinOp.RHS.Vector.Literal)
	}
}

func TestLineFiltersExtraction(t *testing.T) {
	// Includes all 4 kinds: |=  !=  |~  !~
	q := `{app="a"} |= "foo" != "bar" |~ "ba.*" !~ "qux"`

	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	if ast.Kind != KindLogSelector {
		t.Fatalf("kind = %s, want %s", ast.Kind, KindLogSelector)
	}
	if ast.LogSel == nil {
		t.Fatalf("LogSel is nil")
	}
	var haveContains, haveNotContains, haveRegex, haveNotRegex bool
	for _, lf := range ast.LogSel.LineFilters {
		switch lf.Op {
		case LineContains:
			haveContains = true
		case LineNotContains:
			haveNotContains = true
		case LineRegex:
			haveRegex = true
		case LineNotRegex:
			haveNotRegex = true
		}
	}
	if !(haveContains && haveNotContains && haveRegex && haveNotRegex) {
		t.Fatalf("missing some line filter kinds: %#v", ast.LogSel.LineFilters)
	}
}

func TestVectorLiteral(t *testing.T) {
	q := `2`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	if ast.Kind != KindVector {
		t.Fatalf("kind = %s, want %s", ast.Kind, KindVector)
	}
	if ast.Vector == nil || ast.Vector.Literal == nil {
		t.Fatalf("missing literal")
	}
	if *ast.Vector.Literal != 2 {
		t.Fatalf("literal = %v, want 2", *ast.Vector.Literal)
	}
	if ast.Scalar == nil || *ast.Scalar != 2 {
		t.Fatalf("scalar mirror = %v, want 2", ast.Scalar)
	}
}

func TestPromDur(t *testing.T) {
	if got := promDur(0); got != "" {
		t.Fatalf("promDur(0) = %q, want empty", got)
	}
	if got := promDur(5 * time.Minute); got != "5m" {
		t.Fatalf("promDur(5m) = %q, want %q", got, "5m")
	}
}

func TestFromLogQL_LabelFormat_BadTemplate_ReturnsError(t *testing.T) {
	// Missing closing `end`
	bad := `{job="x"} | json | label_format api=` + "`{{ if hasPrefix \"Error\" .response }}ERROR{{else}}{{.response}}`"
	_, err := FromLogQL(bad)
	if err == nil {
		t.Fatalf("expected error for invalid label_format template")
	}
	if !strings.Contains(err.Error(), "label_format") {
		t.Fatalf("unexpected error: %v", err)
	}
}

// tiny helper so we don’t pull in strings for a one-off
func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || (len(sub) > 0 && (indexOf(s, sub) >= 0)))
}

// naive substring search (sufficient for tests)
func indexOf(s, sub string) int {
outer:
	for i := 0; i+len(sub) <= len(s); i++ {
		for j := 0; j < len(sub); j++ {
			if s[i+j] != sub[j] {
				continue outer
			}
		}
		return i
	}
	return -1
}

func TestParse_Unwrap_Identity(t *testing.T) {
	ast, err := FromLogQL(`avg_over_time({job="svc"} | json | unwrap latency_ms [1m])`)
	if err != nil {
		t.Fatal(err)
	}
	sel, _, ok := ast.FirstPipeline()
	if !ok {
		t.Fatalf("no pipeline")
	}
	found := false
	for _, p := range sel.Parsers {
		if p.Type == "unwrap" {
			if p.Params["func"] != "identity" || p.Params["field"] != "latency_ms" {
				t.Fatalf("unwrap params = %#v", p.Params)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("unwrap stage not found")
	}
}

func TestParse_Unwrap_Duration_Quoted(t *testing.T) {
	ast, err := FromLogQL(`min_over_time({job="svc"} | json | unwrap duration(latency_ms) [5m])`)
	if err != nil {
		t.Fatal(err)
	}
	sel, _, ok := ast.FirstPipeline()
	if !ok {
		t.Fatalf("no pipeline")
	}
	var got map[string]string
	for _, p := range sel.Parsers {
		if p.Type == "unwrap" {
			got = p.Params
		}
	}
	if got == nil || got["func"] != "duration" || got["field"] != "latency_ms" {
		t.Fatalf("unwrap params = %#v", got)
	}
}

func TestParse_Unwrap_Base_Case(t *testing.T) {
	ast, err := FromLogQL("max_over_time({job=\"svc\"} | json | unwrap payload_size [2m])")
	if err != nil {
		t.Fatal(err)
	}
	sel, _, ok := ast.FirstPipeline()
	if !ok {
		t.Fatalf("no pipeline")
	}
	var got map[string]string
	for _, p := range sel.Parsers {
		if p.Type == "unwrap" {
			got = p.Params
		}
	}
	if got == nil || got["field"] != "payload_size" {
		t.Fatalf("unwrap params = %#v", got)
	}
}

func TestVectorAggregation_Grouping_NormalizesLabelNames_By(t *testing.T) {
	q := `sum by (resource_cluster, _cardinalhq_foo, log_level) (rate({app="a"}[5m]))`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	if ast.Kind != KindVectorAgg || ast.VectorAgg == nil {
		t.Fatalf("expected VectorAgg, got: %#v", ast)
	}
	want := []string{"resource.cluster", "_cardinalhq.foo", "log.level"}

	if len(ast.VectorAgg.By) != len(want) {
		t.Fatalf("By len = %d, want %d; By = %#v", len(ast.VectorAgg.By), len(want), ast.VectorAgg.By)
	}
	for i, got := range ast.VectorAgg.By {
		if got != want[i] {
			t.Fatalf("By[%d] = %q, want %q; full By = %#v", i, got, want[i], ast.VectorAgg.By)
		}
	}
}

func TestVectorAggregation_Grouping_NormalizesLabelNames_Without(t *testing.T) {
	q := `sum without (resource_cluster, _cardinalhq_foo, log_level) (rate({app="a"}[5m]))`
	ast, err := FromLogQL(q)
	if err != nil {
		t.Fatalf("FromLogQL() error: %v", err)
	}
	if ast.Kind != KindVectorAgg || ast.VectorAgg == nil {
		t.Fatalf("expected VectorAgg, got: %#v", ast)
	}
	want := []string{"resource.cluster", "_cardinalhq.foo", "log.level"}

	if len(ast.VectorAgg.Without) != len(want) {
		t.Fatalf("Without len = %d, want %d; Without = %#v", len(ast.VectorAgg.Without), len(want), ast.VectorAgg.Without)
	}
	for i, got := range ast.VectorAgg.Without {
		if got != want[i] {
			t.Fatalf("Without[%d] = %q, want %q; full Without = %#v", i, got, want[i], ast.VectorAgg.Without)
		}
	}
}
