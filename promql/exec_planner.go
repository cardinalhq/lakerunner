package promql

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
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
	WorkerSQL string   `json:"workerSql,omitempty"`
}

type ExecHints struct {
	WantTopK    bool
	WantBottomK bool
	WantCount   bool
	WantDDS     bool
}

type ExecNode interface {
	Hints() ExecHints
	Eval(sg SketchGroup, step time.Duration) map[string]EvalResult
}

// ---------- Planner ----------

type CompileResult struct {
	Root   ExecNode
	Leaves []BaseExpr
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

func Compile(root Expr) (CompileResult, error) {
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
				"sum_over_time", "avg_over_time", "min_over_time", "max_over_time":
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
			return &BinaryNode{Op: e.BinOp.Op, LHS: lhs, RHS: rhs, Match: e.BinOp.Match}, nil
		}
		return nil, fmt.Errorf("exec compile: unknown kind %q", e.Kind)
	}

	rootNode, err := compile(root, ctx{})
	if err != nil {
		return CompileResult{}, err
	}
	return CompileResult{Root: rootNode, Leaves: dedupeBaseExprs(leaves)}, nil
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
