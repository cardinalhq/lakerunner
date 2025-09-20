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

package promql

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cardinalhq/lakerunner/logql"
)

// ---------- Exec nodes & plumbing (planning-time only) ----------

type BaseExpr struct {
	ID           string       `json:"id"`
	Metric       string       `json:"metric,omitempty"`
	Matchers     []LabelMatch `json:"matchers,omitempty"`
	Range        string       `json:"range,omitempty"`
	SubqueryStep string       `json:"subqueryStep,omitempty"`
	Offset       string       `json:"offset,omitempty"`

	// Final result grouping the worker should aggregate to for this leaf
	GroupBy []string `json:"groupBy,omitempty"`
	Without []string `json:"without,omitempty"`

	// Series function intent
	FuncName string `json:"func,omitempty"`

	// Hints
	WantTopK    bool `json:"wantTopK,omitempty"`
	WantBottomK bool `json:"wantBottomK,omitempty"`
	WantCount   bool `json:"wantCount,omitempty"`
	WantDDS     bool `json:"wantDDS,omitempty"`

	// Identity hints for COUNT (what to keep from the parent's "by")
	CountOnBy []string `json:"countOnBy,omitempty"`

	LogLeaf *logql.LogLeaf `json:"logLeaf,omitempty"`
}

// Synthetic metric families used by the PromQL rewriter.
const (
	SynthLogCount  = "__logql_logs_total"
	SynthLogBytes  = "__logql_log_bytes_total"
	SynthLogUnwrap = "__logql_unwrap_value" // numeric samples produced by `unwrap`
	LeafMatcher    = "__leaf"
)

func (be *BaseExpr) isSyntheticLogMetric() bool {
	return be.LogLeaf != nil
}

type ExecHints struct {
	WantTopK    bool
	WantBottomK bool
	WantCount   bool
	WantDDS     bool
	IsLogLeaf   bool
}

type ExecNode interface {
	Hints() ExecHints
	Eval(sg SketchGroup, step time.Duration) map[string]EvalResult
	Label(tags map[string]any) string
}

// ---------- Planner ----------

type QueryPlan struct {
	Root    ExecNode
	Leaves  []BaseExpr
	TagName string // Set this to a tag name to get distinct values for that tag
}

// nearestAggInfo returns info if the expr is *immediately* an Agg node.
func nearestAggInfo(e Expr) (AggOp, []string, []string, bool) {
	if e.Kind == KindAgg && e.Agg != nil {
		return e.Agg.Op, e.Agg.By, e.Agg.Without, true
	}
	return "", nil, nil, false
}

// parentKeep keeps all child identity labels?
func keepsAll(parentKeep, childID []string) bool {
	if len(childID) == 0 {
		return true
	}
	keep := map[string]struct{}{}
	for _, k := range parentKeep {
		keep[k] = struct{}{}
	}
	for _, id := range childID {
		if _, ok := keep[id]; !ok {
			return false
		}
	}
	return true
}

func Compile(root Expr) (QueryPlan, error) {
	var leaves []BaseExpr

	type ctx struct {
		rng      *RangeExpr
		funcName string

		// “current” child grouping (identity of the immediate child agg, if any)
		curGroup []string
		curWO    []string

		// final result grouping for this node
		outGroup []string
		outWO    []string

		// COUNT hints (persist until leaf)
		wantCount       bool
		wantDDS         bool
		countParentBy   []string
		countIdentityBy []string
		countIdentityWo []string

		// TOPK/BOTTOMK hints
		wantTopK    bool
		wantBottomK bool
	}

	buildLeaf := func(sel Selector, c ctx) (*LeafNode, BaseExpr) {
		be := BaseExpr{
			Metric:      sel.Metric,
			Matchers:    append([]LabelMatch(nil), sel.Matchers...),
			Offset:      sel.Offset,
			FuncName:    c.funcName,
			WantTopK:    c.wantTopK,
			WantBottomK: c.wantBottomK,
			WantCount:   c.wantCount,
			WantDDS:     c.wantDDS,
		}
		if c.rng != nil {
			be.Range = c.rng.Range
			be.SubqueryStep = c.rng.SubqueryStep
		}

		// Default leaf grouping = parent’s desired output grouping
		be.GroupBy = append([]string(nil), c.outGroup...)
		be.Without = append([]string(nil), c.outWO...)

		// COUNT specialization:
		//   - leaf.GroupBy   = child identity (e.g. job,instance)
		//   - leaf.CountOnBy = parent keep-set (e.g. job)
		if c.wantCount {
			if len(c.countIdentityBy) > 0 {
				be.GroupBy = append([]string(nil), c.countIdentityBy...)
				be.Without = nil
			} else if len(c.countIdentityWo) > 0 {
				be.Without = append([]string(nil), c.countIdentityWo...)
				be.GroupBy = nil
			}
			be.CountOnBy = append([]string(nil), c.countParentBy...)
		}

		be.ID = baseExprID(be)
		return &LeafNode{BE: be}, be
	}

	var compile func(e Expr, c ctx) (ExecNode, error)

	compile = func(e Expr, c ctx) (ExecNode, error) {
		switch e.Kind {
		case KindSelector:
			n, be := buildLeaf(*e.Selector, c)
			leaves = append(leaves, be)
			return n, nil

		case KindRange:
			c2 := c
			c2.rng = e.Range
			return compile(e.Range.Expr, c2)

		case KindFunc:
			c2 := c

			switch e.Func.Name {
			case "scalar":
				// scalar(<number>) -> literal scalar
				if e.Func.ArgQ != nil {
					return &ScalarNode{Value: *e.Func.ArgQ}, nil
				}
				// scalar(expr) -> ScalarOfNode over compiled child
				if e.Func.Expr != nil {
					child, err := compile(*e.Func.Expr, c2)
					if err != nil {
						return nil, err
					}
					return &ScalarOfNode{Child: child}, nil
				}
				return nil, fmt.Errorf("scalar() missing argument")

			case "abs", "ceil", "floor", "exp", "ln", "log2", "log10", "sqrt", "sgn":
				if e.Func.Expr == nil {
					return nil, fmt.Errorf("%s() missing argument", e.Func.Name)
				}
				child, err := compile(*e.Func.Expr, c2)
				if err != nil {
					return nil, err
				}
				return &UnaryNode{Func: e.Func.Name, Child: child}, nil

			case "quantile_over_time":
				// Ask workers to return DDSketches for quantiles.
				c2.funcName = e.Func.Name
				c2.wantDDS = true
				if e.Func.Expr == nil {
					return nil, fmt.Errorf("quantile_over_time requires a range vector")
				}
				return compile(*e.Func.Expr, c2)

			// Series-producing funcs
			case "rate", "irate", "increase",
				"sum_over_time", "avg_over_time", "min_over_time", "max_over_time", "count_over_time", "last_over_time":
				c2.funcName = e.Func.Name
				if e.Func.Expr != nil {
					return compile(*e.Func.Expr, c2)
				}
				return nil, fmt.Errorf("%s() missing argument", e.Func.Name)

			default:
				// Unknown/unsupported function
				return nil, fmt.Errorf("unsupported function: %s", e.Func.Name)
			}

		case KindAgg:
			c2 := c
			// Parent output grouping
			if len(e.Agg.By) > 0 {
				c2.outGroup = e.Agg.By
				c2.outWO = nil
			} else if len(e.Agg.Without) > 0 {
				c2.outWO = e.Agg.Without
				c2.outGroup = nil
			}

			// Peek one level for the child identity (e.g. sum by (job,instance) …)
			if _, by, wo, ok := nearestAggInfo(e.Agg.Expr); ok {
				if len(by) > 0 {
					c2.curGroup, c2.curWO = by, nil
				} else if len(wo) > 0 {
					c2.curWO, c2.curGroup = wo, nil
				}
			} else {
				c2.curGroup, c2.curWO = nil, nil
			}

			if e.Agg.Op == AggCount {
				c2.wantCount = true
				// parent keep-set: from `count by (...)`
				c2.countParentBy = append([]string(nil), c2.outGroup...)
				// child identity: from immediate child agg (sum by (job,instance))
				if len(c2.curGroup) > 0 {
					c2.countIdentityBy = append([]string(nil), c2.curGroup...)
					c2.countIdentityWo = nil
				} else if len(c2.curWO) > 0 {
					c2.countIdentityWo = append([]string(nil), c2.curWO...)
					c2.countIdentityBy = nil
				} else {
					c2.countIdentityBy, c2.countIdentityWo = nil, nil
				}
			}

			child, err := compile(e.Agg.Expr, c2)
			if err != nil {
				return nil, err
			}
			return &AggNode{Op: e.Agg.Op, By: c2.outGroup, Without: c2.outWO, Child: child}, nil

		case KindTopK:
			c2 := c
			// Inspect the immediate child for identity
			var childBy []string
			if _, by, _, ok := nearestAggInfo(e.TopK.Expr); ok {
				childBy = by
			}
			// Parent keep-set (if any)
			parentKeep := c2.outGroup
			// If parent drops any child identity label → API-side topk (no worker sketch)
			if len(parentKeep) > 0 && len(childBy) > 0 && !keepsAll(parentKeep, childBy) {
				c2.wantTopK = false
				child, err := compile(e.TopK.Expr, c2)
				if err != nil {
					return nil, err
				}
				return &TopKNode{K: e.TopK.K, Child: child}, nil
			}
			// Worker-side topk
			c2.wantTopK = true
			child, err := compile(e.TopK.Expr, c2)
			if err != nil {
				return nil, err
			}
			return &TopKNode{K: e.TopK.K, Child: child}, nil

		case KindBottomK:
			c2 := c
			// For symmetry; same rule as TopK
			var childBy []string
			if _, by, _, ok := nearestAggInfo(e.BottomK.Expr); ok {
				childBy = by
			}
			parentKeep := c2.outGroup
			if len(parentKeep) > 0 && len(childBy) > 0 && !keepsAll(parentKeep, childBy) {
				c2.wantBottomK = false
				child, err := compile(e.BottomK.Expr, c2)
				if err != nil {
					return nil, err
				}
				return &BottomKNode{K: e.BottomK.K, Child: child}, nil
			}
			c2.wantBottomK = true
			child, err := compile(e.BottomK.Expr, c2)
			if err != nil {
				return nil, err
			}
			return &BottomKNode{K: e.BottomK.K, Child: child}, nil

		case KindHistogramQuantile:
			c2 := c
			c2.wantDDS = true
			child, err := compile(e.HistQuant.Expr, c2)
			if err != nil {
				return nil, err
			}
			return &QuantileNode{Q: e.HistQuant.Q, Child: child}, nil

		case KindClampMin:
			child, err := compile(e.ClampMin.Expr, c)
			if err != nil {
				return nil, err
			}
			return &ClampMinNode{Min: e.ClampMin.Min, Child: child}, nil

		case KindClampMax:
			child, err := compile(e.ClampMax.Expr, c)
			if err != nil {
				return nil, err
			}
			return &ClampMaxNode{Max: e.ClampMax.Max, Child: child}, nil

		case KindBinary:
			lhs, err := compile(e.BinOp.LHS, c)
			if err != nil {
				return nil, err
			}
			rhs, err := compile(e.BinOp.RHS, c)
			if err != nil {
				return nil, err
			}
			return &BinaryNode{Op: e.BinOp.Op, LHS: lhs, RHS: rhs, Match: e.BinOp.Match,
				ReturnBool: e.BinOp.ReturnBool}, nil
		}
		return nil, fmt.Errorf("exec compile: unknown kind %q", e.Kind)
	}

	rootNode, err := compile(root, ctx{})
	if err != nil {
		return QueryPlan{}, err
	}
	return QueryPlan{Root: rootNode, Leaves: dedupeBaseExprs(leaves)}, nil
}

// --- helpers ---

func dedupeBaseExprs(in []BaseExpr) []BaseExpr {
	if len(in) <= 1 {
		return in
	}
	seen := map[string]BaseExpr{}
	for _, b := range in {
		seen[b.ID] = b
	}
	out := make([]BaseExpr, 0, len(seen))
	for _, v := range seen {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// baseExprID makes a stable content hash to use as an identifier.
func baseExprID(b BaseExpr) string {
	var sb strings.Builder
	sb.WriteString("m=" + b.Metric + ";")
	sb.WriteString("off=" + b.Offset + ";")
	sb.WriteString("rng=" + b.Range + ";")
	sb.WriteString("step=" + b.SubqueryStep + ";")
	sb.WriteString("fn=" + b.FuncName + ";")
	if b.WantTopK {
		sb.WriteString("topk=1;")
	}
	if b.WantBottomK {
		sb.WriteString("bottomk=1;")
	}
	if b.WantCount {
		sb.WriteString("count=1;")
	}
	if b.WantDDS {
		sb.WriteString("dds=1;")
	}
	writeCSV := func(tag string, ss []string) {
		if len(ss) == 0 {
			return
		}
		cp := append([]string(nil), ss...)
		sort.Strings(cp)
		sb.WriteString(tag)
		sb.WriteString(strings.Join(cp, ","))
		sb.WriteString(";")
	}
	writeCSV("by=", b.GroupBy)
	writeCSV("wo=", b.Without)
	writeCSV("countOnBy=", b.CountOnBy)

	if len(b.Matchers) > 0 {
		cp := append([]LabelMatch(nil), b.Matchers...)
		sort.Slice(cp, func(i, j int) bool {
			if cp[i].Label != cp[j].Label {
				return cp[i].Label < cp[j].Label
			}
			if cp[i].Op != cp[j].Op {
				return cp[i].Op < cp[j].Op
			}
			return cp[i].Value < cp[j].Value
		})
		for _, m := range cp {
			sb.WriteString("lm:")
			sb.WriteString(m.Label)
			sb.WriteString(string(m.Op))
			sb.WriteString(m.Value)
			sb.WriteString(";")
		}
	}
	sum := sha1.Sum([]byte(sb.String()))
	return hex.EncodeToString(sum[:8])
}

// AttachLogLeaves updates LogLeaf on both the flat plan.Leaves slice and
func (p *QueryPlan) AttachLogLeaves(rr RewriteResult) {
	logLeafByBaseExprID := make(map[string]logql.LogLeaf, len(rr.Leaves))
	for i := range p.Leaves {
		// Take address so we mutate the element in the slice.
		be := &p.Leaves[i]

		kept := make([]LabelMatch, 0, len(be.Matchers))

		for _, m := range be.Matchers {
			if m.Label == LeafMatcher {
				leaf := rr.Leaves[m.Value]
				lcopy := leaf
				logLeafByBaseExprID[be.ID] = lcopy
				be.LogLeaf = &lcopy
				continue
			}
			kept = append(kept, m)
		}

		// Remove __leaf matcher so it doesn’t appear in downstream SQL.
		be.Matchers = kept
	}

	var walk func(ExecNode)
	walk = func(n ExecNode) {
		switch t := n.(type) {
		case *LeafNode:
			if lf, ok := logLeafByBaseExprID[t.BE.ID]; ok {
				lcopy := lf
				t.BE.LogLeaf = &lcopy
			}
		case *AggNode:
			walk(t.Child)
		case *TopKNode:
			walk(t.Child)
		case *BottomKNode:
			walk(t.Child)
		case *QuantileNode:
			walk(t.Child)
		case *ScalarOfNode:
			walk(t.Child)
		case *UnaryNode:
			walk(t.Child)
		case *ClampMinNode:
			walk(t.Child)
		case *ClampMaxNode:
			walk(t.Child)
		case *BinaryNode:
			walk(t.LHS)
			walk(t.RHS)
		}
	}
	walk(p.Root)
}

func FinalGroupingFromPlan(p QueryPlan) (by []string, without []string, has bool) {
	type gmode int
	const (
		modeNone gmode = iota
		modeBy
		modeWithout
		modeGlobal // agg with neither by nor without
	)

	type grouping struct {
		mode   gmode
		labels []string // for by/without only
	}

	clone := func(ss []string) []string {
		if len(ss) == 0 {
			return nil
		}
		cp := make([]string, len(ss))
		copy(cp, ss)
		return cp
	}
	sortCopy := func(ss []string) []string {
		cp := clone(ss)
		if len(cp) > 1 {
			sort.Strings(cp)
		}
		return cp
	}
	equalSet := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		if len(a) == 0 {
			return true
		}
		ac, bc := sortCopy(a), sortCopy(b)
		for i := range ac {
			if ac[i] != bc[i] {
				return false
			}
		}
		return true
	}

	var walk func(n ExecNode) (grouping, bool)
	walk = func(n ExecNode) (grouping, bool) {
		switch t := n.(type) {
		case *AggNode:
			if len(t.By) > 0 {
				return grouping{mode: modeBy, labels: clone(t.By)}, true
			}
			if len(t.Without) > 0 {
				return grouping{mode: modeWithout, labels: clone(t.Without)}, true
			}
			// Global aggregation (no by/without)
			return grouping{mode: modeGlobal}, true

		case *TopKNode:
			return walk(t.Child)
		case *BottomKNode:
			return walk(t.Child)
		case *QuantileNode:
			return walk(t.Child)
		case *ScalarOfNode:
			return walk(t.Child)
		case *UnaryNode:
			return walk(t.Child)
		case *ClampMinNode:
			return walk(t.Child)
		case *ClampMaxNode:
			return walk(t.Child)

		case *BinaryNode:
			lg, lok := walk(t.LHS)
			rg, rok := walk(t.RHS)
			if !lok || !rok {
				return grouping{}, false
			}
			if lg.mode != rg.mode {
				return grouping{}, false
			}
			switch lg.mode {
			case modeGlobal:
				return grouping{mode: modeGlobal}, true
			case modeBy, modeWithout:
				if !equalSet(lg.labels, rg.labels) {
					return grouping{}, false
				}
				return grouping{mode: lg.mode, labels: clone(lg.labels)}, true
			default:
				return grouping{}, false
			}

		default:
			return grouping{}, false
		}
	}

	g, ok := walk(p.Root)
	if !ok {
		return nil, nil, false
	}
	switch g.mode {
	case modeBy:
		return g.labels, nil, true
	case modeWithout:
		return nil, g.labels, true
	case modeGlobal:
		return nil, nil, true
	default:
		return nil, nil, false
	}
}
