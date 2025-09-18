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

func (a *LogAST) IsAggregateExpr() bool {
	switch a.Kind {
	case KindRangeAgg:
		return true
	case KindLogRange:
		return true // range over logs → rewrite
	case KindVectorAgg:
		if a.VectorAgg != nil {
			return a.VectorAgg.Left.IsAggregateExpr()
		}
		return false
	case KindBinOp:
		return a.BinOp != nil && (a.BinOp.LHS.IsAggregateExpr() || a.BinOp.RHS.IsAggregateExpr())
	default:
		return false
	}
}

type LabelFilter struct {
	Label string  `json:"label"`
	Op    MatchOp `json:"op"` // =, !=, =~, !~
	Value string  `json:"value"`

	// Where did this appear in the pipeline?
	AfterParser bool `json:"afterParser,omitempty"` // true if it appeared after some parser
	ParserIdx   *int `json:"parserIdx,omitempty"`   // index in LogSelector.Parsers (0-based), if AfterParser
}

type LabelFormatExpr struct {
	Out  string `json:"out"`            // target label/column name
	Tmpl string `json:"tmpl,omitempty"` // original template (normalized, optional for debug)
	SQL  string `json:"sql"`            // compiled DuckDB SQL expression
}

type ParserStage struct {
	Type         string            `json:"type"`                   // json|logfmt|regexp|label_format|keep_labels|...
	Params       map[string]string `json:"params"`                 // optional (e.g. regexp pattern)
	Filters      []LabelFilter     `json:"filters,omitempty"`      // label filters that follow this parser
	LabelFormats []LabelFormatExpr `json:"labelFormats,omitempty"` // ONLY for label_format
}

type LogSelector struct {
	Matchers     []LabelMatch  `json:"matchers"`
	LineFilters  []LineFilter  `json:"lineFilters,omitempty"`
	LabelFilters []LabelFilter `json:"labelFilters,omitempty"`
	Parsers      []ParserStage `json:"parsers,omitempty"` // json, logfmt, regexp, label_format, keep/drop, unwrap, etc.
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
	Param   *int     `json:"param,omitempty"` // e.g. 3 for topk(3, ...)
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
	MatchGt  MatchOp = ">"
	MatchGte MatchOp = ">="
	MatchLt  MatchOp = "<"
	MatchLte MatchOp = "<="
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

		addedUnwrap := false
		alreadyHasUnwrap := func() bool {
			for _, p := range ls.Parsers {
				if p.Type == "unwrap" {
					return true
				}
			}
			return false
		}

		// Prefer AST node when present.
		if v.Unwrap != nil && !alreadyHasUnwrap() {
			uw := cleanStage(v.Unwrap.String())
			if p, ok := parseUnwrapParams(uw); ok {
				ls.Parsers = append(ls.Parsers, ParserStage{Type: "unwrap", Params: p})
				addedUnwrap = true
			} else if p, ok := parseUnwrapInner(uw); ok {
				ls.Parsers = append(ls.Parsers, ParserStage{Type: "unwrap", Params: p})
				addedUnwrap = true
			}
		} else if !alreadyHasUnwrap() {
			// Fallback: older Loki versions don't populate v.Unwrap for bare forms.
			if p, ok := extractUnwrapFromString(v.String()); ok {
				ls.Parsers = append(ls.Parsers, ParserStage{Type: "unwrap", Params: p})
				addedUnwrap = true
			}
		}

		lr := LogRange{
			Selector: ls,
			Range:    promDur(v.Interval),
			Offset:   promDur(v.Offset),
			Unwrap:   addedUnwrap,
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
			norm := make([]string, 0, len(v.Grouping.Groups))
			for _, g := range v.Grouping.Groups {
				norm = append(norm, normalizeLabelName(g))
			}
			if v.Grouping.Without {
				ra.Without = norm
			} else {
				ra.By = norm
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
			Op:   firstToken(e.String()), // Loki has v.Operation; token keeps us resilient
			Left: left,
		}

		// Extract parameter for functions like topk(3, ...), bottomk(5, ...)
		if v.Params != 0 {
			va.Param = &v.Params
		}

		if v.Grouping != nil && len(v.Grouping.Groups) > 0 {
			norm := make([]string, 0, len(v.Grouping.Groups))
			for _, g := range v.Grouping.Groups {
				norm = append(norm, normalizeLabelName(g))
			}
			if v.Grouping.Without {
				va.Without = norm
			} else {
				va.By = norm
			}
		}
		return LogAST{Kind: KindVectorAgg, VectorAgg: &va, Raw: e.String()}, nil

	// <sample> <op> <sample>
	case *logql.BinOpExpr:
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
		val, err := v.Value()
		if err != nil {
			return LogAST{}, err
		}
		vec := Vector{Literal: &val}
		return LogAST{Kind: KindVector, Vector: &vec, Scalar: &val, Raw: e.String()}, nil

	case *logql.VectorExpr:
		val, err := v.Value()
		if err != nil {
			return LogAST{}, err
		}
		vec := Vector{Literal: &val}
		return LogAST{Kind: KindVector, Vector: &vec, Scalar: &val, Raw: e.String()}, nil

	// Pure selector (matchers, maybe with pipeline / stages).
	case logql.LogSelectorExpr:
		ls, err := buildSelector(v)
		if err != nil {
			return LogAST{}, err
		}
		return LogAST{Kind: KindLogSelector, LogSel: &ls, Raw: e.String()}, nil

	default:
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
		if lf.Left != nil {
			visit(*lf.Left)
		}
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

// Adds parser stages and label filters based on the *string* form of the selector pipeline.
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
					sqlExpr, err := buildLabelFormatExprTemplate(tmpl, func(col string) string { return quoteIdent(col) })
					if err != nil {
						return fmt.Errorf("label_format %s: %w", outName, err)
					}
					compiled = append(compiled, LabelFormatExpr{
						Out:  normalizeLabelName(outName),
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

	// Parse stages & label filters from the *string* pipeline (json/logfmt/regexp/etc.)
	if err := addParsersAndLabelFiltersFromString(sel.String(), &ls); err != nil {
		return LogSelector{}, err
	}

	sort.Slice(ls.Matchers, func(i, j int) bool { return ls.Matchers[i].Label < ls.Matchers[j].Label })
	return ls, nil
}

// splitPipelineStages splits the string form of a selector on top-level '|' after the matcher block.
func splitPipelineStages(selStr string) []string {
	if i := strings.IndexByte(selStr, '}'); i >= 0 {
		selStr = selStr[i+1:]
	}

	var out []string
	var buf []rune

	inDQ := false
	inBT := false
	esc := false
	paren := 0

	flush := func() {
		s := strings.TrimSpace(string(buf))
		if s != "" {
			out = append(out, strings.TrimSpace(strings.TrimPrefix(s, "|")))
		}
		buf = buf[:0]
	}

	for _, r := range selStr {
		if inDQ {
			buf = append(buf, r)
			if esc {
				esc = false
			} else if r == '\\' {
				esc = true
			} else if r == '"' {
				inDQ = false
			}
			continue
		}
		if inBT {
			buf = append(buf, r)
			if esc {
				esc = false
			} else if r == '\\' {
				esc = true
			} else if r == '`' {
				inBT = false
			}
			continue
		}

		switch r {
		case '"':
			inDQ = true
			buf = append(buf, r)
		case '`':
			inBT = true
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
	case "json":
		// support mappings: json a="x.y" b=`m.n` c=plain
		raw := parseLabelFormatParams(stage)
		params := make(map[string]string, len(raw))
		for k, v := range raw {
			params[k] = normalizeLabelFormatLiteral(v)
		}
		return "json", params, true

	case "logfmt":
		// support optional mappings after logfmt as well
		raw := parseLabelFormatParams(stage)
		params := make(map[string]string, len(raw))
		for k, v := range raw {
			params[k] = normalizeLabelFormatLiteral(v)
		}
		return "logfmt", params, true

		// in looksLikeParser(stage string)
	case "line_format":
		tmpl := ""
		if i := strings.IndexAny(stage, "`\""); i >= 0 {
			q := stage[i]
			if j := strings.LastIndexByte(stage, q); j > i {
				inner := stage[i : j+1]                   // includes quotes
				tmpl = normalizeLabelFormatLiteral(inner) // strips quotes/backticks, unescapes \" etc
			}
		}
		params := map[string]string{}
		if tmpl != "" {
			params["template"] = tmpl
		}
		return "line_format", params, true

	case "label_replace", "keep_labels", "drop_labels", "decolorize":
		return head[0], map[string]string{}, true

	case "regexp":
		params := map[string]string{}
		if i := strings.Index(stage, "\""); i >= 0 {
			if j := strings.LastIndex(stage, "\""); j > i {
				if pat, err := strconv.Unquote(stage[i : j+1]); err == nil {
					params["pattern"] = pat
				}
			}
		}
		return "regexp", params, true

	case "label_format", "label-format", "labelformat":
		return "label_format", parseLabelFormatParams(stage), true

	case "unwrap":
		if p, ok := parseUnwrapParams(stage); ok {
			return "unwrap", p, true
		}
		return "", nil, false

	default:
		return "", nil, false
	}
}

// parseUnwrapParams parses pipeline chunks like:
//
//	"unwrap payload_size"             → {"func":"identity","field":"payload_size"}
//	"unwrap bytes(payload_size)"      → {"func":"bytes","field":"payload_size"}
//	"unwrap duration(\"latency_ms\")" → {"func":"duration","field":"latency_ms"}
//	"unwrap bytes[5m]"                → {"func":"identity","field":"bytes"} (trims trailing window)
func parseUnwrapParams(stage string) (map[string]string, bool) {
	s := cleanStage(stage)
	if !strings.HasPrefix(s, "unwrap") {
		return nil, false
	}
	rest := strings.TrimSpace(s[len("unwrap"):])
	if rest == "" {
		return nil, false
	}

	isDur := strings.HasPrefix(rest, "duration")
	isBytes := strings.HasPrefix(rest, "bytes")
	hasParen := strings.Contains(rest, "(")

	// function form: unwrap duration(field) / unwrap bytes(field)
	if isDur || (isBytes && hasParen) {
		var fn string
		if isDur {
			fn = "duration"
			rest = strings.TrimSpace(rest[len("duration"):])
		} else {
			fn = "bytes"
			rest = strings.TrimSpace(rest[len("bytes"):])
		}
		if len(rest) < 2 || rest[0] != '(' || rest[len(rest)-1] != ')' {
			return nil, false
		}
		arg := strings.TrimSpace(rest[1 : len(rest)-1])
		arg = normalizeLabelFormatLiteral(arg)
		if arg == "" {
			return nil, false
		}
		return map[string]string{"func": fn, "field": arg}, true
	}

	// bare form: unwrap <identifier> (can appear as "unwrap bytes[5m]")
	tok := strings.TrimSpace(rest)
	// cut at first of '[', space, or ')'
	if i := strings.IndexAny(tok, "[ )"); i >= 0 {
		tok = tok[:i]
	}
	tok = normalizeLabelFormatLiteral(tok)
	if tok == "" {
		return nil, false
	}
	return map[string]string{"func": "identity", "field": tok}, true
}

// parseUnwrapInner parses what Loki returns from v.Unwrap.String():
//
//	"payload_size"         → {"func":"identity","field":"payload_size"}
//	"bytes(payload_size)"  → {"func":"bytes","field":"payload_size"}
//	"duration(latency_ms)" → {"func":"duration","field":"latency_ms"}
//	"unwrap bytes[5m]"     → {"func":"identity","field":"bytes"} (tolerant)
func parseUnwrapInner(s string) (map[string]string, bool) {
	s = cleanStage(s)
	if s == "" {
		return nil, false
	}

	// Tolerate an "unwrap" prefix
	if strings.HasPrefix(s, "unwrap") {
		s = strings.TrimSpace(s[len("unwrap"):])
	}

	// function form without "unwrap" prefix
	if strings.HasPrefix(s, "duration") || strings.HasPrefix(s, "bytes") {
		fn := "bytes"
		rest := s
		if strings.HasPrefix(s, "duration") {
			fn = "duration"
			rest = strings.TrimSpace(s[len("duration"):])
		} else if strings.HasPrefix(s, "bytes") {
			fn = "bytes"
			rest = strings.TrimSpace(s[len("bytes"):])
		}
		if len(rest) >= 2 && rest[0] == '(' && rest[len(rest)-1] == ')' {
			arg := strings.TrimSpace(rest[1 : len(rest)-1])
			arg = normalizeLabelFormatLiteral(arg)
			if arg == "" {
				return nil, false
			}
			return map[string]string{"func": fn, "field": arg}, true
		}
		// If no parens (e.g., "bytes[5m]"), fall through to bare handling below.
	}

	// bare identifier → default to identity; trim trailing range/paren/space
	field := normalizeLabelFormatLiteral(s)
	if i := strings.IndexAny(field, "[ )"); i >= 0 {
		field = strings.TrimSpace(field[:i])
	}
	if field == "" {
		return nil, false
	}
	return map[string]string{"func": "identity", "field": field}, true
}

// Robust fallback: slice the "| unwrap ..." chunk right out of the string.
func extractUnwrapFromString(s string) (map[string]string, bool) {
	// Prefer a pipeline head "| unwrap"
	if i := strings.Index(s, "| unwrap"); i >= 0 {
		chunk := strings.TrimSpace(s[i+1:]) // keep "unwrap ..."
		if j := strings.Index(chunk, "|"); j >= 0 {
			chunk = chunk[:j]
		}
		if p, ok := parseUnwrapParams(chunk); ok {
			return p, true
		}
		if p, ok := parseUnwrapInner(chunk); ok {
			return p, true
		}
	}
	// Fallback: search for " unwrap " even without a preceding pipe
	if i := strings.Index(s, " unwrap "); i >= 0 {
		chunk := strings.TrimSpace(s[i+1:])
		if j := strings.Index(chunk, "|"); j >= 0 {
			chunk = chunk[:j]
		}
		if p, ok := parseUnwrapParams(chunk); ok {
			return p, true
		}
		if p, ok := parseUnwrapInner(chunk); ok {
			return p, true
		}
	}
	return nil, false
}

// parseLabelFormatParams parses: label_format a=`tmpl` b="str" c=unquoted
// Supports multiple assignments separated by spaces and/or commas.
// Values may be backtick-quoted templates or double-quoted strings.
func parseLabelFormatParams(stage string) map[string]string {
	params := make(map[string]string)
	s := strings.TrimSpace(stage)

	// strip the head token (label_format / label-format / labelformat)
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
			if r == ',' || r == ' ' || r == '\t' {
				flushPair()
				continue
			}
			if r == '`' {
				inBacktick = true
				val.WriteRune(r)
				continue
			}
			if r == '"' {
				inQuote = true
				val.WriteRune(r)
				continue
			}
			val.WriteRune(r)

		default:
			if r == '=' && !haveKey {
				haveKey = true
				readingValue = true
				continue
			}
			if r == ',' {
				continue
			}
			if !(r == ' ' || r == '\t') || key.Len() > 0 {
				key.WriteRune(r)
			}
		}
	}
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
	for _, op := range []string{">=", "<=", ">", "<", "=~", "!~", "!=", "="} {
		if i := strings.Index(s, op); i >= 0 {
			lab := strings.TrimSpace(s[:i])
			val := strings.TrimSpace(s[i+len(op):])
			if lab == "" || val == "" {
				return LabelFilter{}, false
			}
			if len(val) >= 2 && val[0] == '"' && val[len(val)-1] == '"' {
				if u, err := strconv.Unquote(val); err == nil {
					val = u
				}
			}
			return LabelFilter{Label: normalizeLabelName(lab), Op: toMatchOpString(op), Value: val}, true
		}
	}
	return LabelFilter{}, false
}

func normalizeLabelName(label string) string {
	if strings.HasPrefix(label, "_cardinalhq_") {
		remainder := strings.TrimPrefix(label, "_cardinalhq_")
		return "_cardinalhq." + remainder
	}
	if strings.HasPrefix(label, "resource_") || strings.HasPrefix(label, "log_") {
		return strings.ReplaceAll(label, "_", ".")
	}
	return label
}

func toLabelMatches(ms []*labels.Matcher) []LabelMatch {
	out := make([]LabelMatch, 0, len(ms))
	for _, m := range ms {
		if m == nil {
			continue
		}
		if m.Name == "__name__" {
			continue
		}
		out = append(out, LabelMatch{Label: normalizeLabelName(m.Name), Op: toMatchOp(m.Type), Value: m.Value})
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
	case ">":
		return MatchGt
	case ">=":
		return MatchGte
	case "<":
		return MatchLt
	case "<=":
		return MatchLte
	default:
		return MatchEq
	}
}

// Normalize backticks / \" etc.
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

// cleanStage trims a leading pipe and surrounding spaces: "| foo" -> "foo".
func cleanStage(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "|") {
		s = strings.TrimSpace(s[1:])
	}
	return s
}
