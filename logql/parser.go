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
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	lqllog "github.com/grafana/loki/v3/pkg/logql/log"
	logql "github.com/grafana/loki/v3/pkg/logql/syntax"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
)

// ------------------------------
// Public, simplified LogQL LogAST
// ------------------------------

type Kind string

const (
	KindLogSelector Kind = "log_selector" // `{app="api"}` (maybe with pipeline)
	KindLogRange    Kind = "log_range"    // `{...} ... [5m] offset 1m`
	KindRangeAgg    Kind = "range_agg"    // `rate|count_over_time|... ( <log_range> )`
	KindVector      Kind = "vector"       // number literal
	KindVectorAgg   Kind = "vector_agg"   // `sum|avg|... by(...) ( <vector/sample> )`
	KindBinOp       Kind = "binop"        // `<sample> <op> <sample>`
	KindScalar      Kind = "scalar_literal"
	KindOpaque      Kind = "opaque"
)

type LogAST struct {
	Kind      Kind         `json:"kind"`
	LogSel    *LogSelector `json:"logSelector,omitempty"`
	LogRange  *LogRange    `json:"logRange,omitempty"`
	RangeAgg  *RangeAgg    `json:"rangeAgg,omitempty"`
	Vector    *Vector      `json:"vector,omitempty"`
	VectorAgg *VectorAgg   `json:"vectorAgg,omitempty"`
	BinOp     *BinOp       `json:"binop,omitempty"`
	Scalar    *float64     `json:"scalar,omitempty"`
	Raw       string       `json:"raw"`
}

type LabelFilter struct {
	Label string  `json:"label"`
	Op    MatchOp `json:"op"` // =, !=, =~, !~
	Value string  `json:"value"`
}

type LogSelector struct {
	Matchers     []LabelMatch  `json:"matchers"`
	LineFilters  []LineFilter  `json:"lineFilters,omitempty"`
	LabelFilters []LabelFilter `json:"labelFilters,omitempty"` // NEW
	Parsers      []ParserStage `json:"parsers,omitempty"`      // json, logfmt, label_fmt, keep/drop, etc.
}

type LogRange struct {
	Selector LogSelector `json:"selector"`
	Range    string      `json:"range"`  // e.g. "5m"
	Offset   string      `json:"offset"` // e.g. "1m"
	Unwrap   bool        `json:"unwrap"`
}

type RangeAgg struct {
	Op      string   `json:"op"`              // rate, count_over_time, bytes_rate, quantile_over_time, ...
	Param   *float64 `json:"param,omitempty"` // e.g. 0.99
	By      []string `json:"by,omitempty"`
	Without []string `json:"without,omitempty"`
	Left    LogRange `json:"left"`
}

type Vector struct {
	Literal *float64 `json:"literal,omitempty"`
}

type VectorAgg struct {
	Op      string   `json:"op"`
	By      []string `json:"by,omitempty"`
	Without []string `json:"without,omitempty"`
	Left    LogAST   `json:"left"` // vector or sample expression
}

type BinOp struct {
	Op  string `json:"op"`
	LHS LogAST `json:"lhs"`
	RHS LogAST `json:"rhs"`
}

type LabelMatch struct {
	Label string  `json:"label"`
	Op    MatchOp `json:"op"`
	Value string  `json:"value"`
}

type MatchOp string

const (
	MatchEq  MatchOp = "="
	MatchNe  MatchOp = "!="
	MatchRe  MatchOp = "=~"
	MatchNre MatchOp = "!~"
)

type LineFilter struct {
	Op    LineFilterOp `json:"op"`    // contains|not_contains|regex|not_regex
	Match string       `json:"match"` // substring or regex
}

type LineFilterOp string

const (
	LineContains    LineFilterOp = "contains"     // |= "foo"
	LineNotContains LineFilterOp = "not_contains" // != "foo"
	LineRegex       LineFilterOp = "regex"        // |~ "re"
	LineNotRegex    LineFilterOp = "not_regex"    // !~ "re"
)

type ParserStage struct {
	Type   string            `json:"type"`   // json|logfmt|label_fmt|keep_labels|drop_labels|...
	Params map[string]string `json:"params"` // placeholder for stage options
}

// ------------------------------
// Public API
// ------------------------------

// FromLogQL parses input with the official Loki LogQL parser and returns a simplified LogAST.
func FromLogQL(input string) (LogAST, error) {
	e, err := logql.ParseExpr(input)
	if err != nil {
		return LogAST{}, err
	}
	ast, err := fromSyntax(e)
	if err != nil {
		return LogAST{}, err
	}
	ast.Raw = e.String()
	return ast, nil
}

// ------------------------------
// Internal conversion
// ------------------------------

func fromSyntax(e logql.Expr) (LogAST, error) {
	switch v := e.(type) {
	// {selector} [range] offset unwrap?
	case *logql.LogRangeExpr:
		ls, err := buildSelector(v.Left)
		if err != nil {
			return LogAST{}, err
		}
		lr := LogRange{
			Selector: ls,
			Range:    promDur(v.Interval),
			Offset:   promDur(v.Offset),
			Unwrap:   v.Unwrap != nil,
		}
		return LogAST{Kind: KindLogRange, LogRange: &lr, Raw: e.String()}, nil

	// rate/count_over_time/…(<log_range>) by/without (...)
	case *logql.RangeAggregationExpr:
		left, err := fromSyntax(v.Left)
		if err != nil {
			return LogAST{}, err
		}
		if left.LogRange == nil {
			return LogAST{}, fmt.Errorf("range_agg left is not a log range")
		}
		ra := RangeAgg{
			Op:    v.Operation,
			Param: v.Params, // *float64
			Left:  *left.LogRange,
		}
		if v.Grouping != nil && len(v.Grouping.Groups) > 0 {
			if v.Grouping.Without {
				ra.Without = append([]string(nil), v.Grouping.Groups...)
			} else {
				ra.By = append([]string(nil), v.Grouping.Groups...)
			}
		}
		return LogAST{Kind: KindRangeAgg, RangeAgg: &ra, Raw: e.String()}, nil

	// sum/avg/… by(...) ( <vector/sample> )
	case *logql.VectorAggregationExpr:
		left, err := fromSyntax(v.Left)
		if err != nil {
			return LogAST{}, err
		}
		va := VectorAgg{
			// Loki does have v.Operation, but fallback to first token keeps us resilient.
			Op:   firstToken(e.String()),
			Left: left,
		}
		if v.Grouping != nil && len(v.Grouping.Groups) > 0 {
			if v.Grouping.Without {
				va.Without = append([]string(nil), v.Grouping.Groups...)
			} else {
				va.By = append([]string(nil), v.Grouping.Groups...)
			}
		}
		return LogAST{Kind: KindVectorAgg, VectorAgg: &va, Raw: e.String()}, nil

	// <sample> <op> <sample>
	case *logql.BinOpExpr:
		// Left side is the embedded SampleExpr field (not LHS).
		lhs, err := fromSyntax(v.SampleExpr)
		if err != nil {
			return LogAST{}, err
		}
		rhs, err := fromSyntax(v.RHS)
		if err != nil {
			return LogAST{}, err
		}
		return LogAST{Kind: KindBinOp, BinOp: &BinOp{Op: v.Op, LHS: lhs, RHS: rhs}, Raw: e.String()}, nil

	case *logql.LiteralExpr:
		// Loki v3.5+: numeric literal (e.g., `2`)
		val, err := v.Value()
		if err != nil {
			return LogAST{}, err
		}
		vec := Vector{Literal: &val}
		return LogAST{
			Kind:   KindVector, // treat as numeric vector literal
			Vector: &vec,
			Scalar: &val, // optional convenience
			Raw:    e.String(),
		}, nil

	case *logql.VectorExpr:
		// Older Loki: numeric literal was VectorExpr
		val, err := v.Value()
		if err != nil {
			return LogAST{}, err
		}
		vec := Vector{Literal: &val}
		return LogAST{
			Kind:   KindVector,
			Vector: &vec,
			Scalar: &val,
			Raw:    e.String(),
		}, nil

	// Pure selector (matchers, maybe with pipeline / stages).
	// Handle as a *last* specific case so it doesn’t swallow other kinds.
	case logql.LogSelectorExpr:
		ls, err := buildSelector(v)
		if err != nil {
			return LogAST{}, err
		}
		return LogAST{Kind: KindLogSelector, LogSel: &ls, Raw: e.String()}, nil

	default:
		// Fallback: keep the rendered form; you can extend coverage here incrementally.
		return LogAST{Kind: KindOpaque, Raw: e.String()}, nil
	}
}

func toLineFilterOp(t lqllog.LineMatchType) LineFilterOp {
	switch t {
	case lqllog.LineMatchEqual:
		return LineContains // |=
	case lqllog.LineMatchNotEqual:
		return LineNotContains // !=
	case lqllog.LineMatchRegexp:
		return LineRegex // |~
	case lqllog.LineMatchNotRegexp:
		return LineNotRegex // !~
	// Loki also has pattern variants; treat them like regex by default:
	case lqllog.LineMatchPattern:
		return LineRegex
	case lqllog.LineMatchNotPattern:
		return LineNotRegex
	default:
		return LineContains
	}
}

// addLineFilterFromSyntax walks LineFilterExpr chains left→right and de-dupes.
func addLineFilterFromSyntax(ls *LogSelector, lineFilterList []logql.LineFilterExpr) {
	seen := make(map[string]struct{})

	var visit func(logql.LineFilterExpr)
	visit = func(lf logql.LineFilterExpr) {
		// walk left first to preserve pipeline order (left → right)
		if lf.Left != nil {
			visit(*lf.Left)
		}
		// de-dupe by (type, match); adjust key if you need to distinguish same match with different positions
		key := fmt.Sprintf("%d\x00%s", lf.Ty, lf.Match)
		if _, ok := seen[key]; ok {
			return
		}
		seen[key] = struct{}{}

		ls.LineFilters = append(ls.LineFilters, LineFilter{
			Op:    toLineFilterOp(lf.Ty),
			Match: lf.Match,
		})
	}

	for _, lf := range lineFilterList {
		visit(lf)
	}
}

// Build selector: call this along with your existing line-filter / parser extraction.
func buildSelector(sel logql.LogSelectorExpr) (LogSelector, error) {
	ls := LogSelector{
		Matchers: toLabelMatches(sel.Matchers()),
	}

	// --- line filters you already do ---
	lineFilterList := logql.ExtractLineFilters(sel)
	addLineFilterFromSyntax(&ls, lineFilterList)

	// --- label filters before parsers ---
	addLabelFiltersFromSyntax(&ls, sel)

	// --- parser stages ---
	addParsersFromString(sel.String(), &ls)

	sort.Slice(ls.Matchers, func(i, j int) bool { return ls.Matchers[i].Label < ls.Matchers[j].Label })
	return ls, nil
}

var (
	rePipeRegexp     = regexp.MustCompile(`\|\s*regexp\b`)
	rePipeJSON       = regexp.MustCompile(`\|\s*json\b`)
	rePipeLogfmt     = regexp.MustCompile(`\|\s*logfmt\b`)
	rePipeLabelFmt   = regexp.MustCompile(`\|\s*label_(?:format|fmt)\b`)
	rePipeLineFmt    = regexp.MustCompile(`\|\s*line_(?:format|fmt)\b`)
	rePipeKeepLabels = regexp.MustCompile(`\|\s*keep_labels\b`)
	rePipeDropLabels = regexp.MustCompile(`\|\s*drop_labels\b`)
	rePipeLabelRepl  = regexp.MustCompile(`\|\s*label_replace\b`)
	rePipeDecolorize = regexp.MustCompile(`\|\s*decolorize\b`)
)

func addParsersFromString(s string, ls *LogSelector) {
	add := func(n int, typ string) {
		for i := 0; i < n; i++ {
			ls.Parsers = append(ls.Parsers, ParserStage{Type: typ, Params: map[string]string{}})
		}
	}
	add(len(rePipeRegexp.FindAllStringIndex(s, -1)), "regexp")
	add(len(rePipeJSON.FindAllStringIndex(s, -1)), "json")
	add(len(rePipeLogfmt.FindAllStringIndex(s, -1)), "logfmt")
	add(len(rePipeLabelFmt.FindAllStringIndex(s, -1)), "label_fmt")
	add(len(rePipeLineFmt.FindAllStringIndex(s, -1)), "line_fmt")
	add(len(rePipeKeepLabels.FindAllStringIndex(s, -1)), "keep_labels")
	add(len(rePipeDropLabels.FindAllStringIndex(s, -1)), "drop_labels")
	add(len(rePipeLabelRepl.FindAllStringIndex(s, -1)), "label_replace")
	add(len(rePipeDecolorize.FindAllStringIndex(s, -1)), "decolorize")
}

// ------------------------------
// Small helpers
// ------------------------------

func toLabelMatches(ms []*labels.Matcher) []LabelMatch {
	out := make([]LabelMatch, 0, len(ms))
	for _, m := range ms {
		if m == nil {
			continue
		}
		if m.Name == "__name__" {
			continue
		}
		out = append(out, LabelMatch{Label: m.Name, Op: toMatchOp(m.Type), Value: m.Value})
	}
	return out
}

func toMatchOp(t labels.MatchType) MatchOp {
	switch t {
	case labels.MatchEqual:
		return MatchEq
	case labels.MatchNotEqual:
		return MatchNe
	case labels.MatchRegexp:
		return MatchRe
	case labels.MatchNotRegexp:
		return MatchNre
	default:
		return MatchEq
	}
}

// promDur prints a time.Duration in Prometheus-style ("5m", "1h3m").
func promDur(d time.Duration) string {
	if d == 0 {
		return ""
	}
	return model.Duration(d).String()
}

// very small helper to get the function/keyword name at the start of an expr string
var fnNameRe = regexp.MustCompile(`^\s*([a-zA-Z_][a-zA-Z0-9_]*)`)

func firstToken(s string) string {
	if m := fnNameRe.FindStringSubmatch(s); len(m) > 1 {
		return m[1]
	}
	return ""
}

// FirstPipeline is a generic variant that can start from any AST node.
func (a *LogAST) FirstPipeline() (*LogSelector, *LogRange, bool) {
	if a == nil {
		return nil, nil, false
	}
	return pipelineFromNode(a)
}

// CollectPipelines returns every pipeline (LogRange) in evaluation order (left→right).
// Useful when expressions contain multiple subqueries (e.g., binops).
func (a *LogAST) CollectPipelines() []*LogRange {
	var out []*LogRange
	collectPipelines(a, &out)
	return out
}

// --- internals ---

// pipelineFromNode finds the first LogRange (pipeline) under node and returns its selector and range.
func pipelineFromNode(node *LogAST) (*LogSelector, *LogRange, bool) {
	if node == nil {
		return nil, nil, false
	}
	switch node.Kind {
	case KindRangeAgg:
		// VectorAgg -> RangeAgg -> LogRange -> LogSelector
		if node.RangeAgg != nil {
			return &node.RangeAgg.Left.Selector, &node.RangeAgg.Left, true
		}
	case KindLogRange:
		// In case someone passed a Range sub-tree directly
		if node.LogRange != nil {
			return &node.LogRange.Selector, node.LogRange, true
		}
	case KindVectorAgg:
		// Recurse through nested vector aggs
		if node.VectorAgg != nil {
			return pipelineFromNode(&node.VectorAgg.Left)
		}
	case KindBinOp:
		// Try LHS, then RHS (left→right evaluation order)
		if node.BinOp != nil {
			if sel, rng, ok := pipelineFromNode(&node.BinOp.LHS); ok {
				return sel, rng, true
			}
			return pipelineFromNode(&node.BinOp.RHS)
		}
	case KindLogSelector:
		// Bare selector (no [range]) — not a pipeline; expose it anyway if you want
		if node.LogSel != nil {
			return node.LogSel, nil, true
		}
	}
	return nil, nil, false
}

// collectPipelines appends all LogRanges in-order into out.
func collectPipelines(node *LogAST, out *[]*LogRange) {
	if node == nil {
		return
	}
	switch node.Kind {
	case KindRangeAgg:
		if node.RangeAgg != nil {
			*out = append(*out, &node.RangeAgg.Left)
		}
	case KindLogRange:
		if node.LogRange != nil {
			*out = append(*out, node.LogRange)
		}
	case KindVectorAgg:
		if node.VectorAgg != nil {
			collectPipelines(&node.VectorAgg.Left, out)
		}
	case KindBinOp:
		if node.BinOp != nil {
			collectPipelines(&node.BinOp.LHS, out)
			collectPipelines(&node.BinOp.RHS, out)
		}
		// other node kinds do not contain pipelines directly
	}
}

// Walks label-filter pipeline (before parser stages) via the public API and
// turns them into our simple {label, op, value}.
func addLabelFiltersFromSyntax(ls *LogSelector, sel logql.LogSelectorExpr) {
	for _, lf := range logql.ExtractLabelFiltersBeforeParser(sel) {
		// The only stable/public thing we can read is the string form.
		// Examples: `| level="ERROR"`, `| duration > 10s`, etc.
		s := strings.TrimSpace(lf.String()) // LabelFilterExpr implements fmt.Stringer

		// Normalize: drop a leading pipe if present.
		if strings.HasPrefix(s, "|") {
			s = strings.TrimSpace(strings.TrimPrefix(s, "|"))
		}

		// Parse: label, operator, value (quoted or bare). Keep it conservative.
		if lab, op, val, ok := parseLabelFilterString(s); ok {
			ls.LabelFilters = append(ls.LabelFilters, LabelFilter{
				Label: lab,
				Op:    toMatchOpString(op),
				Value: val,
			})
		}
	}
}

// Parses strings like: level="ERROR", level!="error", level=~"re", level!~"re"
// Also allows bare values (numbers/durations) and spaces around tokens.
var reLabelFilter = regexp.MustCompile(`^\s*([A-Za-z_][A-Za-z0-9_:.]*)\s*(=~|!~|!=|=)\s*(.+?)\s*$`)

func parseLabelFilterString(s string) (label, op, value string, ok bool) {
	m := reLabelFilter.FindStringSubmatch(s)
	if len(m) != 4 {
		return "", "", "", false
	}
	label, op, value = m[1], m[2], m[3]
	// Trim quotes if quoted
	if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
		value = strings.Trim(value, `"'`)
	}
	return label, op, value, true
}

func toMatchOpString(op string) MatchOp {
	switch op {
	case "=":
		return MatchEq
	case "!=":
		return MatchNe
	case "=~":
		return MatchRe
	case "!~":
		return MatchNre
	default:
		return MatchEq
	}
}
