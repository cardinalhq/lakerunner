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
	"reflect"
	"testing"
)

func TestFinalGrouping_SimpleBy(t *testing.T) {
	q := `sum by (job) (rate(http_requests_total[5m]))`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	by, wo, ok := FinalGroupingFromPlan(plan)
	if !ok {
		t.Fatalf("expected grouping, got none")
	}
	if len(wo) != 0 {
		t.Fatalf("unexpected Without=%v", wo)
	}
	if got, want := sorted(by), []string{"job"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("By=%v, want %v", got, want)
	}
}

func TestFinalGrouping_SimpleWithout(t *testing.T) {
	q := `sum without (instance) (rate(http_requests_total[5m]))`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	by, wo, ok := FinalGroupingFromPlan(plan)
	if !ok {
		t.Fatalf("expected grouping, got none")
	}
	if len(by) != 0 {
		t.Fatalf("unexpected By=%v", by)
	}
	if got, want := sorted(wo), []string{"instance"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Without=%v, want %v", got, want)
	}
}

func TestFinalGrouping_Global(t *testing.T) {
	q := `sum(rate(http_requests_total[5m]))`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	by, wo, ok := FinalGroupingFromPlan(plan)
	if !ok {
		t.Fatalf("expected global grouping, got none")
	}
	if len(by) != 0 || len(wo) != 0 {
		t.Fatalf("expected global (no by/without), got By=%v Without=%v", by, wo)
	}
}

func TestFinalGrouping_PeelsWrappers(t *testing.T) {
	q := `topk(3, clamp_min(sum by (svc)(rate(http_requests_total[5m])), 1))`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	by, wo, ok := FinalGroupingFromPlan(plan)
	if !ok {
		t.Fatalf("expected grouping, got none")
	}
	if len(wo) != 0 {
		t.Fatalf("unexpected Without=%v", wo)
	}
	if got, want := sorted(by), []string{"svc"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("By=%v, want %v", got, want)
	}
}

func TestFinalGrouping_Binary_SameByOnBothSides(t *testing.T) {
	q := `
		sum by (job) (rate(a_metric[5m])) /
		sum by (job) (rate(b_metric[5m]))
	`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	by, wo, ok := FinalGroupingFromPlan(plan)
	if !ok {
		t.Fatalf("expected grouping, got none")
	}
	if len(wo) != 0 {
		t.Fatalf("unexpected Without=%v", wo)
	}
	if got, want := sorted(by), []string{"job"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("By=%v, want %v", got, want)
	}
}

func TestFinalGrouping_Binary_GlobalOnBothSides(t *testing.T) {
	q := `
		sum(rate(a_metric[5m])) /
		sum(rate(b_metric[5m]))
	`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	by, wo, ok := FinalGroupingFromPlan(plan)
	if !ok {
		t.Fatalf("expected global grouping, got none")
	}
	if len(by) != 0 || len(wo) != 0 {
		t.Fatalf("expected global (no by/without), got By=%v Without=%v", by, wo)
	}
}

func TestFinalGrouping_Binary_MismatchedBy(t *testing.T) {
	q := `
		sum by (job) (rate(a_metric[5m])) /
		sum by (instance) (rate(b_metric[5m]))
	`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	if by, wo, ok := FinalGroupingFromPlan(plan); ok {
		t.Fatalf("expected no definitive grouping; got By=%v Without=%v", by, wo)
	}
}

func TestFinalGrouping_Binary_OneSideNoGrouping(t *testing.T) {
	q := `
		sum by (job) (rate(a_metric[5m])) / 2
	`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	if by, wo, ok := FinalGroupingFromPlan(plan); ok {
		t.Fatalf("expected no definitive grouping; got By=%v Without=%v", by, wo)
	}
}

func TestFinalGrouping_Binary_PeelsWrappersOnBothSides(t *testing.T) {
	q := `
		clamp_min(sum by (job) (rate(a_metric[5m])), 1) /
		clamp_min(sum by (job) (rate(b_metric[5m])), 1)
	`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	by, wo, ok := FinalGroupingFromPlan(plan)
	if !ok {
		t.Fatalf("expected grouping, got none")
	}
	if len(wo) != 0 {
		t.Fatalf("unexpected Without=%v", wo)
	}
	if got, want := sorted(by), []string{"job"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("By=%v, want %v", got, want)
	}
}

func TestFinalGrouping_Binary_DifferentKinds(t *testing.T) {
	// Left: by(job); Right: global
	q := `
		sum by (job) (rate(a_metric[5m])) /
		sum(rate(b_metric[5m]))
	`
	root := mustParse(t, q)

	plan, err := Compile(root)
	if err != nil {
		t.Fatalf("Compile error: %v", err)
	}

	if by, wo, ok := FinalGroupingFromPlan(plan); ok {
		t.Fatalf("expected no definitive grouping; got By=%v Without=%v", by, wo)
	}
}
