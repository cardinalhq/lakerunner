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
	"strconv"
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

	// NEW: where did this appear in the pipeline?
	AfterParser bool `json:"afterParser,omitempty"` // true if it appeared after some parser
	ParserIdx   *int `json:"parserIdx,omitempty"`   // index in LogSelector.Parsers (0-based), if AfterParser
}

type LabelFormatExpr struct {
	Out  string `json:"out"`            // target label/column name
	Tmpl string `json:"tmpl,omitempty"` // original template (normalized, optional for debug)
	SQL  string `json:"sql"`            // compiled DuckDB SQL expression
}

type ParserStage struct {
	Type         string            `json:"type"`                   // json|logfmt|regexp|label_fmt|keep_labels|...
	Params       map[string]string `json:"params"`                 // optional (e.g. regexp pattern)
	Filters      []LabelFilter     `json:"filters,omitempty"`      // NEW: label filters that follow this parser
	LabelFormats []LabelFormatExpr `json:"labelFormats,omitempty"` // ONLY for label_format
}
type LogSelector struct {
	Matchers     []LabelMatch  `json:"matchers"`
	LineFilters  []LineFilter  `json:"lineFilters,omitempty"`
	LabelFilters []LabelFilter `json:"labelFilters,omitempty"`
	Parsers      []ParserStage `json:"parsers,omitempty"` // json, logfmt, regexp, label_fmt, keep/drop, etc.
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
		// de-dupe by (type, match)
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

// Let this now return error
func addParsersAndLabelFiltersFromString(s string, ls *LogSelector) error {
	lastParser := -1

	for _, chunk := range splitPipelineStages(s) {
		// Parser?
		if typ, params, ok := looksLikeParser(chunk); ok {
			ps := ParserStage{Type: typ, Params: params}

			if typ == "label_format" {
				compiled := make([]LabelFormatExpr, 0, len(params))
				for outName, raw := range params {
					tmpl := normalizeLabelFormatLiteral(raw)

					// parse+validate+compile to SQL (returns error on bad template)
					sqlExpr, err := buildLabelFormatExprTemplate(tmpl, func(col string) string { return quoteIdent(col) })
					if err != nil {
						return fmt.Errorf("label_format %s: %w", outName, err)
					}
					compiled = append(compiled, LabelFormatExpr{
						Out:  outName,
						Tmpl: tmpl,
						SQL:  sqlExpr,
					})
				}
				ps.LabelFormats = compiled
			}

			ls.Parsers = append(ls.Parsers, ps)
			lastParser = len(ls.Parsers) - 1
			continue
		}

		// Label filter?
		if lf, ok := tryParseLabelFilter(chunk); ok {
			if lastParser >= 0 {
				idx := lastParser
				lf.AfterParser = true
				lf.ParserIdx = &idx
				ls.Parsers[lastParser].Filters = append(ls.Parsers[lastParser].Filters, lf)
				ls.LabelFilters = append(ls.LabelFilters, lf)
			} else {
				ls.LabelFilters = append(ls.LabelFilters, lf)
			}
		}
	}
	return nil
}

func buildSelector(sel logql.LogSelectorExpr) (LogSelector, error) {
	ls := LogSelector{
		Matchers: toLabelMatches(sel.Matchers()),
	}
	addLineFilterFromSyntax(&ls, logql.ExtractLineFilters(sel))
	if err := addParsersAndLabelFiltersFromString(sel.String(), &ls); err != nil {
		return LogSelector{}, err
	}
	sort.Slice(ls.Matchers, func(i, j int) bool { return ls.Matchers[i].Label < ls.Matchers[j].Label })
	return ls, nil
}

// ------------------------------
// Pipeline scanning (order-preserving, quote/paren aware)
// ------------------------------

// splitPipelineStages splits the string form of a selector on top-level '|' after the matcher block.
func splitPipelineStages(selStr string) []string {
	// Keep only pipeline tail after the first closing '}'.
	if i := strings.IndexByte(selStr, '}'); i >= 0 && i+1 < len(selStr) {
		selStr = selStr[i+1:]
	}

	var out []string
	var buf []rune
	inStr, esc := false, false
	paren := 0

	flush := func() {
		s := strings.TrimSpace(string(buf))
		if s != "" {
			out = append(out, strings.TrimSpace(strings.TrimPrefix(s, "|")))
		}
		buf = buf[:0]
	}

	for _, r := range selStr {
		if inStr {
			buf = append(buf, r)
			if esc {
				esc = false
			} else if r == '\\' {
				esc = true
			} else if r == '"' {
				inStr = false
			}
			continue
		}
		switch r {
		case '"':
			inStr = true
			buf = append(buf, r)
		case '(':
			paren++
			buf = append(buf, r)
		case ')':
			if paren > 0 {
				paren--
			}
			buf = append(buf, r)
		case '|':
			if paren == 0 {
				flush()
				continue
			}
			buf = append(buf, r)
		default:
			buf = append(buf, r)
		}
	}
	flush()
	return out
}

// looksLikeParser returns (type, params, ok) for known parser heads.
func looksLikeParser(stage string) (string, map[string]string, bool) {
	head := strings.Fields(stage)
	if len(head) == 0 {
		return "", nil, false
	}
	switch head[0] {
	case "json", "logfmt", "line_format", "label_replace", "keep_labels", "drop_labels", "decolorize":
		return head[0], map[string]string{}, true
	case "regexp":
		params := map[string]string{}
		// Extract first quoted string (pattern) if present
		if i := strings.Index(stage, "\""); i >= 0 {
			if j := strings.LastIndex(stage, "\""); j > i {
				if pat, err := strconv.Unquote(stage[i : j+1]); err == nil {
					params["pattern"] = pat
				}
			}
		}
		return "regexp", params, true

	case "label_format", "label-format", "labelformat":
		// Normalize to label_format and parse assignments
		return "label_format", parseLabelFormatParams(stage), true

	default:
		return "", nil, false
	}
}

// parseLabelFormatParams parses: label_format a=`tmpl` b="str" c=unquoted
// Supports multiple assignments separated by spaces and/or commas.
// Values may be backtick-quoted templates or double-quoted strings.
func parseLabelFormatParams(stage string) map[string]string {
	params := make(map[string]string)
	s := strings.TrimSpace(stage)

	// strip the head token (label_format / label-format / labelformat)
	// find first whitespace after the head
	i := strings.IndexAny(s, " \t")
	if i < 0 {
		return params
	}
	s = strings.TrimSpace(s[i:]) // remainder after the head

	var (
		key          strings.Builder
		val          strings.Builder
		haveKey      bool
		inBacktick   bool
		inQuote      bool
		esc          bool
		readingValue bool
		flushPair    = func() {
			k := strings.TrimSpace(key.String())
			v := strings.TrimSpace(val.String())
			if k != "" {
				params[k] = v
			}
			key.Reset()
			val.Reset()
			haveKey = false
			readingValue = false
			inBacktick = false
			inQuote = false
			esc = false
		}
	)

	for _, r := range s {
		switch {
		case readingValue:
			// inside value
			if inBacktick {
				val.WriteRune(r)
				if !esc && r == '`' {
					inBacktick = false
				}
				esc = !esc && r == '\\'
				continue
			}
			if inQuote {
				val.WriteRune(r)
				if !esc && r == '"' {
					inQuote = false
				}
				esc = !esc && r == '\\'
				continue
			}

			// not in quotes: check for separator
			if r == ',' || r == ' ' || r == '\t' {
				flushPair()
				continue
			}

			// opening quote types
			if r == '`' {
				inBacktick = true
				val.WriteRune(r) // keep wrapper; builder knows to strip
				continue
			}
			if r == '"' {
				inQuote = true
				val.WriteRune(r) // keep wrapper; builder knows to strip
				continue
			}

			val.WriteRune(r)

		default:
			// reading the key until '=' outside quotes
			if r == '=' && !haveKey {
				haveKey = true
				readingValue = true
				// skip whitespace is handled naturally in value loop
				continue
			}
			// separators without a value → ignore
			if r == ',' {
				continue
			}
			if !(r == ' ' || r == '\t') || key.Len() > 0 {
				key.WriteRune(r)
			}
		}
	}
	// finalize last pair
	if readingValue || key.Len() > 0 {
		flushPair()
	}
	return params
}

// tryParseLabelFilter parses `<ident> <op> <value>` (value may be quoted).
func tryParseLabelFilter(stage string) (LabelFilter, bool) {
	s := strings.TrimSpace(stage)
	if s == "" {
		return LabelFilter{}, false
	}
	// Prefer longest ops first
	for _, op := range []string{"=~", "!~", "!=", "="} {
		if i := strings.Index(s, op); i >= 0 {
			lab := strings.TrimSpace(s[:i])
			val := strings.TrimSpace(s[i+len(op):])
			if lab == "" || val == "" {
				return LabelFilter{}, false
			}
			// Unquote value if it's "...".
			if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
				if u, err := strconv.Unquote(val); err == nil {
					val = u
				}
			}
			return LabelFilter{Label: lab, Op: toMatchOpString(op), Value: val}, true
		}
	}
	return LabelFilter{}, false
}

// addParsersFromString: order-preserving parser detection with optional params.
func addParsersFromString(s string, ls *LogSelector) {
	for _, chunk := range splitPipelineStages(s) {
		if typ, params, ok := looksLikeParser(chunk); ok {
			ls.Parsers = append(ls.Parsers, ParserStage{Type: typ, Params: params})
		}
	}
}

// addLabelFiltersFromString: picks up label filters anywhere in the pipeline (incl. after parsers).
func addLabelFiltersFromString(s string, ls *LogSelector) {
	for _, chunk := range splitPipelineStages(s) {
		if lf, ok := tryParseLabelFilter(chunk); ok {
			ls.LabelFilters = append(ls.LabelFilters, lf)
		}
	}
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
		if node.RangeAgg != nil {
			return &node.RangeAgg.Left.Selector, &node.RangeAgg.Left, true
		}
	case KindLogRange:
		if node.LogRange != nil {
			return &node.LogRange.Selector, node.LogRange, true
		}
	case KindVectorAgg:
		if node.VectorAgg != nil {
			return pipelineFromNode(&node.VectorAgg.Left)
		}
	case KindBinOp:
		if node.BinOp != nil {
			if sel, rng, ok := pipelineFromNode(&node.BinOp.LHS); ok {
				return sel, rng, true
			}
			return pipelineFromNode(&node.BinOp.RHS)
		}
	case KindLogSelector:
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
	}
}

func parseLabelFilterString(s string) (string, string, string, bool) {
	s = strings.TrimSpace(s)
	for _, op := range []string{"=~", "!~", "!=", "="} {
		if i := strings.Index(s, op); i >= 0 {
			lab := strings.TrimSpace(s[:i])
			val := strings.TrimSpace(s[i+len(op):])
			if lab == "" || val == "" {
				return "", "", "", false
			}
			if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
				if u, err := strconv.Unquote(val); err == nil {
					val = u
				}
			}
			return lab, op, val, true
		}
	}
	return "", "", "", false
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

// Normalize backticks / \" etc. (reuse this in both parser & builder if you want)
func normalizeLabelFormatLiteral(s string) string {
	if n := len(s); n >= 2 {
		if (s[0] == '`' && s[n-1] == '`') || (s[0] == '"' && s[n-1] == '"') {
			s = s[1 : n-1]
		}
	}
	if strings.Contains(s, `\"`) {
		s = strings.ReplaceAll(s, `\"`, `"`)
	}
	return s
}
