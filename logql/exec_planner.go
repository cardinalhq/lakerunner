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
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// -------- Exec nodes (planning time only) --------

type LExecNode interface{}

type LAggNode struct {
	// Vector aggregation (sum/avg/min/max/count … by/without)
	Op      string
	Param   *int // e.g. 3 for topk(3, ...)
	By      []string
	Without []string
	Child   LExecNode
}

type LRangeAggNode struct {
	// Range aggregation over logs (rate/bytes_rate/count_over_time/…)
	Op    string
	Param *float64
	Child LExecNode // should be an LLeafNode or a subtree that ends at a leaf
}

type LBinOpNode struct {
	Op       string
	LHS, RHS LExecNode
}

type LScalarNode struct {
	Value float64
}

type LLeafNode struct {
	Leaf LogLeaf
}

// -------- Leaf payload (the thing workers push down) --------

type LogLeaf struct {
	ID string `json:"id"`

	// Pipeline (left→right) captured at selector level
	Matchers     []LabelMatch  `json:"matchers,omitempty"`
	LineFilters  []LineFilter  `json:"lineFilters,omitempty"`
	LabelFilters []LabelFilter `json:"labelFilters,omitempty"`
	Parsers      []ParserStage `json:"parsers,omitempty"`

	// Time shape attached to this pipeline (from enclosing LogRange)
	Range  string `json:"range,omitempty"`  // e.g. "5m"
	Offset string `json:"offset,omitempty"` // e.g. "1m"
	Unwrap bool   `json:"unwrap,omitempty"` // rare, but carry it

	// If this leaf is the input to a range aggregation, note the op
	RangeAggOp string   `json:"rangeAggOp,omitempty"` // e.g. "count_over_time", "rate"
	RangeParam *float64 `json:"rangeParam,omitempty"`

	// Optional: the immediate vector-agg goal above this leaf (helps worker decide grouping)
	OutBy      []string `json:"outBy,omitempty"`
	OutWithout []string `json:"outWithout,omitempty"`
}

// -------- Plan --------

type LQueryPlan struct {
	Root    LExecNode
	Leaves  []LogLeaf
	TagName string // Set this to a tag name to get distinct values for that tag
}

// -------- Planner --------

func CompileLog(root LogAST) (LQueryPlan, error) {
	var leaves []LogLeaf

	// A tiny context carried down
	type ctx struct {
		// vector-agg goal for the current parent (sum by/without …)
		outBy, outWO []string

		// range-agg we’re currently under (rate/count_over_time …)
		rangeAggOp string
		rangeParam *float64

		// the current LogRange shape (if any)
		rng string
		off string
		unw bool
	}

	var compile func(LogAST, ctx) (LExecNode, error)

	// Build a leaf from a selector + ctx
	buildLeaf := func(sel LogSelector, c ctx) LogLeaf {
		lf := LogLeaf{
			Matchers:     append([]LabelMatch(nil), sel.Matchers...),
			LineFilters:  append([]LineFilter(nil), sel.LineFilters...),
			LabelFilters: append([]LabelFilter(nil), sel.LabelFilters...),
			Parsers:      append([]ParserStage(nil), sel.Parsers...),

			Range:  c.rng,
			Offset: c.off,
			Unwrap: c.unw,

			RangeAggOp: c.rangeAggOp,
			RangeParam: c.rangeParam,

			OutBy:      append([]string(nil), c.outBy...),
			OutWithout: append([]string(nil), c.outWO...),
		}
		lf.ID = logLeafID(lf)
		return lf
	}

	compile = func(ast LogAST, c ctx) (LExecNode, error) {
		switch ast.Kind {

		case KindLogSelector:
			if ast.LogSel == nil {
				return nil, fmt.Errorf("nil selector")
			}
			leaf := buildLeaf(*ast.LogSel, c)
			leaves = append(leaves, leaf)
			return &LLeafNode{Leaf: leaf}, nil

		case KindLogRange:
			if ast.LogRange == nil {
				return nil, fmt.Errorf("nil log range")
			}
			c2 := c
			c2.rng, c2.off, c2.unw = ast.LogRange.Range, ast.LogRange.Offset, ast.LogRange.Unwrap
			// descend into the selector that the range wraps
			return compile(LogAST{Kind: KindLogSelector, LogSel: &ast.LogRange.Selector}, c2)

		case KindRangeAgg:
			if ast.RangeAgg == nil {
				return nil, fmt.Errorf("nil range agg")
			}
			c2 := c
			c2.rangeAggOp = ast.RangeAgg.Op
			c2.rangeParam = ast.RangeAgg.Param
			// RangeAgg always wraps a LogRange; we compiled that to a leaf
			child, err := compile(LogAST{Kind: KindLogRange, LogRange: &ast.RangeAgg.Left}, c2)
			if err != nil {
				return nil, err
			}
			return &LRangeAggNode{Op: ast.RangeAgg.Op, Param: ast.RangeAgg.Param, Child: child}, nil

		case KindVectorAgg:
			if ast.VectorAgg == nil {
				return nil, fmt.Errorf("nil vector agg")
			}
			c2 := c
			if len(ast.VectorAgg.By) > 0 {
				c2.outBy, c2.outWO = ast.VectorAgg.By, nil
			} else if len(ast.VectorAgg.Without) > 0 {
				c2.outBy, c2.outWO = nil, ast.VectorAgg.Without
			}
			child, err := compile(ast.VectorAgg.Left, c2)
			if err != nil {
				return nil, err
			}
			return &LAggNode{Op: ast.VectorAgg.Op, Param: ast.VectorAgg.Param, By: c2.outBy, Without: c2.outWO, Child: child}, nil

		case KindBinOp:
			if ast.BinOp == nil {
				return nil, fmt.Errorf("nil binop")
			}
			lhs, err := compile(ast.BinOp.LHS, c)
			if err != nil {
				return nil, err
			}
			rhs, err := compile(ast.BinOp.RHS, c)
			if err != nil {
				return nil, err
			}
			return &LBinOpNode{Op: ast.BinOp.Op, LHS: lhs, RHS: rhs}, nil

		case KindVector:
			// scalar literal
			if ast.Scalar != nil {
				return &LScalarNode{Value: *ast.Scalar}, nil
			}
			return &LScalarNode{Value: 0}, nil

		default:
			// Treat anything else as non-pushdown (opaque); but try to surface a leaf if possible
			if ast.LogSel != nil {
				leaf := buildLeaf(*ast.LogSel, c)
				leaves = append(leaves, leaf)
				return &LLeafNode{Leaf: leaf}, nil
			}
			return nil, fmt.Errorf("unsupported AST kind: %s", ast.Kind)
		}
	}

	compiledRoot, err := compile(root, ctx{})
	if err != nil {
		return LQueryPlan{}, err
	}
	return LQueryPlan{Root: compiledRoot, Leaves: dedupeLeaves(leaves)}, nil
}

// -------- helpers --------

func dedupeLeaves(in []LogLeaf) []LogLeaf {
	if len(in) <= 1 {
		return in
	}
	m := make(map[string]LogLeaf, len(in))
	for _, l := range in {
		m[l.ID] = l
	}
	out := make([]LogLeaf, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func logLeafID(l LogLeaf) string {
	var sb strings.Builder
	w := func(s string) { sb.WriteString(s); sb.WriteString(";") }

	// time & op
	w("rng=" + l.Range)
	w("off=" + l.Offset)
	w("unw=" + fmt.Sprintf("%t", l.Unwrap))
	w("op=" + l.RangeAggOp)

	if l.RangeParam != nil {
		w("param=" + fmt.Sprintf("%g", *l.RangeParam))
	}
	if len(l.OutBy) > 0 {
		cp := append([]string(nil), l.OutBy...)
		sort.Strings(cp)
		w("by=" + strings.Join(cp, ","))
	}
	if len(l.OutWithout) > 0 {
		cp := append([]string(nil), l.OutWithout...)
		sort.Strings(cp)
		w("wo=" + strings.Join(cp, ","))
	}

	// matchers
	if len(l.Matchers) > 0 {
		cp := append([]LabelMatch(nil), l.Matchers...)
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
			w("lm:" + m.Label + string(m.Op) + m.Value)
		}
	}

	// line filters
	if len(l.LineFilters) > 0 {
		cp := append([]LineFilter(nil), l.LineFilters...)
		sort.Slice(cp, func(i, j int) bool {
			if cp[i].Op != cp[j].Op {
				return cp[i].Op < cp[j].Op
			}
			return cp[i].Match < cp[j].Match
		})
		for _, f := range cp {
			w("lf:" + string(f.Op) + "|" + f.Match)
		}
	}

	// label filters
	if len(l.LabelFilters) > 0 {
		cp := append([]LabelFilter(nil), l.LabelFilters...)
		sort.Slice(cp, func(i, j int) bool {
			if cp[i].Label != cp[j].Label {
				return cp[i].Label < cp[j].Label
			}
			if cp[i].Op != cp[j].Op {
				return cp[i].Op < cp[j].Op
			}
			return cp[i].Value < cp[j].Value
		})
		for _, lf := range cp {
			w("labf:" + lf.Label + string(lf.Op) + lf.Value)
		}
	}

	// parsers (types + stable params)
	if len(l.Parsers) > 0 {
		cp := append([]ParserStage(nil), l.Parsers...)
		sort.Slice(cp, func(i, j int) bool { return cp[i].Type < cp[j].Type })
		for _, p := range cp {
			if len(p.Params) == 0 {
				w("ps:" + p.Type)
				continue
			}
			keys := make([]string, 0, len(p.Params))
			for k := range p.Params {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var kv []string
			for _, k := range keys {
				kv = append(kv, k+"="+p.Params[k])
			}
			w("ps:" + p.Type + "{" + strings.Join(kv, ",") + "}")
		}
	}

	sum := sha1.Sum([]byte(sb.String()))
	return hex.EncodeToString(sum[:8])
}

// IsSimpleAggregation returns true if this leaf can use an optimized flat SQL
// query for aggregation (no CTE pipeline needed). This is the case when:
// - No parsers (json, regexp, logfmt, label_format, unwrap, etc.)
// - No line filters (which require scanning log_message content)
// - No label filters (which might depend on parsed labels)
//
// For simple aggregations like count_over_time grouped by existing columns,
// we can generate a single SELECT with GROUP BY that only reads the columns
// we actually need, rather than SELECT * through a CTE pipeline.
func (be *LogLeaf) IsSimpleAggregation() bool {
	return len(be.Parsers) == 0 && len(be.LineFilters) == 0 && len(be.LabelFilters) == 0
}

// RequiredColumns returns the minimal set of columns needed for this leaf
// when used in a simple aggregation context. Returns nil if the leaf is not
// simple (i.e., requires the full CTE pipeline).
func (be *LogLeaf) RequiredColumns() []string {
	if !be.IsSimpleAggregation() {
		return nil
	}

	cols := make(map[string]struct{})

	// Always need timestamp for time filtering and bucketing
	cols["chq_timestamp"] = struct{}{}

	// Add matcher columns (for WHERE clause)
	for _, m := range be.Matchers {
		cols[m.Label] = struct{}{}
	}

	// Add group-by columns
	for _, g := range be.OutBy {
		cols[g] = struct{}{}
	}

	// Convert to sorted slice for deterministic output
	result := make([]string, 0, len(cols))
	for c := range cols {
		result = append(result, c)
	}
	sort.Strings(result)
	return result
}

func (be *LogLeaf) Label() string {
	// --- helpers ---
	quote := func(s string) string {
		s = strings.ReplaceAll(s, `"`, `\"`)
		return `"` + s + `"`
	}
	// stable sort for nice deterministic output
	type kv struct{ k, v string }
	sortKVs := func(xs []kv) []kv {
		out := append([]kv(nil), xs...)
		sort.Slice(out, func(i, j int) bool {
			if out[i].k != out[j].k {
				return out[i].k < out[j].k
			}
			return out[i].v < out[j].v
		})
		return out
	}

	// --- selector: {k op "v"} ---
	selector := "{"
	if len(be.Matchers) > 0 {
		kvs := make([]kv, 0, len(be.Matchers))
		for _, m := range be.Matchers {
			kvs = append(kvs, kv{
				k: m.Label,
				v: fmt.Sprintf("%s%s", string(m.Op), quote(m.Value)),
			})
		}
		kvs = sortKVs(kvs)
		parts := make([]string, len(kvs))
		for i, p := range kvs {
			parts[i] = p.k + p.v
		}
		selector += strings.Join(parts, ",")
	}
	selector += "}"

	// --- line filters: |=, !=, |~, !~ ---
	lineOp := func(op LineFilterOp) string {
		switch op {
		case LineContains:
			return "|="
		case LineNotContains:
			return "!="
		case LineRegex:
			return "|~"
		case LineNotRegex:
			return "!~"
		default:
			return "|?"
		}
	}
	var lineFilters []string
	for _, lf := range be.LineFilters {
		lineFilters = append(lineFilters, fmt.Sprintf(`%s %s`, lineOp(lf.Op), quote(lf.Match)))
	}

	// --- parsers ---
	var parsers []string
	for _, p := range be.Parsers {
		switch strings.ToLower(p.Type) {
		case "json", "logfmt":
			parsers = append(parsers, "| "+strings.ToLower(p.Type))
		case "regexp":
			if pat, ok := p.Params["pattern"]; ok && pat != "" {
				parsers = append(parsers, `| regexp `+quote(pat))
			} else {
				parsers = append(parsers, "| regexp")
			}
		case "label_format", "label-format", "labelformat":
			// Try to surface what gets created; we may not have the original template, so just list keys.
			var keys []string
			if len(p.LabelFormats) > 0 {
				for _, lf := range p.LabelFormats {
					keys = append(keys, lf.Out)
				}
			} else {
				for k := range p.Params {
					keys = append(keys, k)
				}
			}
			sort.Strings(keys)
			if len(keys) > 0 {
				parsers = append(parsers, "| label_format("+strings.Join(keys, ",")+")")
			} else {
				parsers = append(parsers, "| label_format")
			}
		default:
			// unknown – show type
			parsers = append(parsers, "| "+p.Type)
		}
	}

	// --- label filters (after parsers), e.g. level="ERROR" user=~"a|b" ---
	var lblFilters []string
	for _, lf := range be.LabelFilters {
		var op string
		switch lf.Op {
		case MatchEq:
			op = "="
		case MatchNe:
			op = "!="
		case MatchRe:
			op = "=~"
		case MatchNre:
			op = "!~"
		case MatchGt:
			op = ">"
		case MatchLt:
			op = "<"
		case MatchGte:
			op = ">="
		case MatchLte:
			op = "<="
		default:
			op = string(lf.Op)
		}
		lblFilters = append(lblFilters, fmt.Sprintf(`%s%s%s`, lf.Label, op, quote(lf.Value)))
	}

	// --- stitch pieces together in LogQL-ish order ---
	var buf strings.Builder
	buf.WriteString(selector)
	if len(lineFilters) > 0 {
		buf.WriteByte(' ')
		buf.WriteString(strings.Join(lineFilters, " "))
	}
	if len(parsers) > 0 {
		buf.WriteByte(' ')
		buf.WriteString(strings.Join(parsers, " "))
	}
	if len(lblFilters) > 0 {
		buf.WriteByte(' ')
		buf.WriteString(strings.Join(lblFilters, " "))
	}
	// range selector applies to the whole stream/pipeline
	if be.Range != "" {
		buf.WriteString("[" + be.Range + "]")
	}
	if be.Offset != "" {
		buf.WriteString(" offset " + be.Offset)
	}
	if be.Unwrap {
		buf.WriteString(" | unwrap")
	}
	return buf.String()
}
