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

package promql

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false) // <-- keep > < & as-is
	if err := enc.Encode(v); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return strings.TrimRight(buf.String(), "\n")
}

func containsAll(t *testing.T, s string, subs ...string) {
	t.Helper()
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			t.Fatalf("expected JSON to contain %q\n--- got ---\n%s", sub, s)
		}
	}
}

func TestScalarLiteral(t *testing.T) {
	expr, err := FromPromQL(`42`)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"func"`,
		`"name":"scalar"`,
		`"q":42`,
	)
}

func TestScalarCallOnExpr(t *testing.T) {
	expr, err := FromPromQL(`scalar(sum(rate(http_requests_total[1m])))`)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"func"`, `"name":"scalar"`,
		`"kind":"agg"`, `"op":"sum"`,
		`"name":"rate"`,
		`"range":"1m"`,
		`"metric":"http_requests_total"`,
	)
}

func TestClampMinDenominator(t *testing.T) {
	q := `100 * (
		sum by (svc)(rate(http_requests_total{status=~"5.."}[5m])) /
		clamp_min(sum by (svc)(rate(http_requests_total[5m])), 1)
	)`
	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"binary"`,
		`"op":"*"`,
		`"kind":"clamp_min"`,
		`"min":1`,
		`"kind":"agg"`, `"op":"sum"`, `"by":["svc"]`,
		`"name":"rate"`,
		`"range":"5m"`,
		`"metric":"http_requests_total"`,
		`"op":"/"`,
	)
}

func TestHistogramQuantile(t *testing.T) {
	q := `histogram_quantile(0.99, sum by (le,service)(rate(http_request_duration_seconds_bucket[5m])))`
	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"histogram_quantile"`,
		`"q":0.99`,
		`"kind":"agg"`, `"op":"sum"`,
		`"by":["le","service"]`,
		`"name":"rate"`,
		`"range":"5m"`,
		`"metric":"http_request_duration_seconds_bucket"`,
	)
}

func TestRangeAndOffset(t *testing.T) {
	q := `rate(http_requests_total{job="api"}[2m] offset 5m)`
	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"func"`, `"name":"rate"`,
		`"kind":"range"`, `"range":"2m"`,
		`"kind":"selector"`,
		`"metric":"http_requests_total"`,
		`"label":"job"`, `"op":"="`, `"value":"api"`,
		`"offset":"5m"`,
	)
}

func TestBinaryArithmetic(t *testing.T) {
	q := `sum(rate(a_total[1m])) / sum(rate(b_total[1m]))`
	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"binary"`, `"op":"/"`,
		`"metric":"a_total"`, `"range":"1m"`,
		`"metric":"b_total"`, `"range":"1m"`,
		`"op":"sum"`, `"name":"rate"`,
	)
}

func TestSubqueryStep(t *testing.T) {
	q := `sum(rate(http_requests_total[10m:1m]))`
	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"range"`,
		`"range":"10m"`,
		`"subqueryStep":"1m"`,
	)
}

func TestTopKOverAggRate(t *testing.T) {
	q := `topk(3, sum by (job)(rate(http_requests_total[5m])))`
	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"topk"`,
		`"k":3`,
		`"kind":"agg"`, `"op":"sum"`,
		`"by":["job"]`,
		`"name":"rate"`,
		`"range":"5m"`,
		`"metric":"http_requests_total"`,
	)
}

func TestBottomKOnSelector(t *testing.T) {
	q := `bottomk(2, http_requests_inflight)`
	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"bottomk"`,
		`"k":2`,
		`"kind":"selector"`,
		`"metric":"http_requests_inflight"`,
	)
}

func TestCompare_MaxSelector_GT_Scalar(t *testing.T) {
	q := `max({__name__="k8s.container.cpu_limit_utilization"}) > 0.9`
	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)

	containsAll(t, js,
		`"kind":"binary"`,
		`"op":">"`,

		// LHS: vector aggregation max(...) over a selector
		`"kind":"agg"`,
		`"op":"max"`,
		`"kind":"selector"`,
		`"metric":"k8s.container.cpu_limit_utilization"`,

		// RHS: scalar literal 0.9 (our AST encodes literals as scalar(...) with q)
		`"kind":"func"`,
		`"name":"scalar"`,
		`"q":0.9`,
	)
}

func TestParser_ReturnBool(t *testing.T) {
	e, err := FromPromQL(`max(foo) > bool 0.9`)
	if err != nil {
		t.Fatal(err)
	}
	if e.Kind != KindBinary || e.BinOp == nil || !e.BinOp.ReturnBool {
		t.Fatalf("ReturnBool not set: %#v", e)
	}
}

func TestLastOverTime_RangeAndOffset(t *testing.T) {
	q := `last_over_time(http_request_duration_seconds_sum{job="api"}[24h] offset 1h)`
	expr, err := FromPromQL(q)
	if err != nil {
		t.Fatal(err)
	}
	js := mustJSON(t, expr)
	containsAll(t, js,
		`"kind":"func"`, `"name":"last_over_time"`,
		`"kind":"range"`, `"range":"1d"`,
		`"offset":"1h"`,
		`"kind":"selector"`,
		`"metric":"http_request_duration_seconds_sum"`,
		`"label":"job"`, `"op":"="`, `"value":"api"`,
	)
}
