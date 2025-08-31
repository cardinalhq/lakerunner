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
	"text/template"
	"text/template/parse"
	"time"
)

// ToWorkerSQL renders a left→right pipeline for this leaf as layered SQL.
// Upstream should replace {table} with the actual base relation (table/subquery).
// NOTE: Selector matchers (be.Matchers) are pushed as an early WHERE layer,
//
//	assuming per-row columns exist for those labels (or a view/CTE provides them).
func (be *LogLeaf) ToWorkerSQL(step time.Duration) string {
	const baseRel = "{table}"                 // replace upstream
	const bodyCol = "\"_cardinalhq.message\"" // quoted column for message text

	type layer struct {
		name string
		sql  string
	}

	var layers []layer
	mk := func(i int) string { return fmt.Sprintf("s%d", i) }

	layerIdx := 0
	push := func(selectList []string, from string, whereConds []string) {
		alias := mk(layerIdx)
		layerIdx++
		if len(selectList) == 0 {
			selectList = []string{"*"}
		}
		sl := strings.Join(selectList, ", ")
		var where string
		if len(whereConds) > 0 {
			where = " WHERE " + strings.Join(whereConds, " AND ")
		}
		sql := fmt.Sprintf("%s AS (\n  SELECT %s\n  FROM %s%s\n)", alias, sl, from, where)
		layers = append(layers, layer{name: alias, sql: sql})
	}

	// s0: base
	push(nil, baseRel, nil)

	// Apply selector matchers as an early WHERE layer (if any).
	if len(be.Matchers) > 0 {
		mLfs := make([]LabelFilter, 0, len(be.Matchers))
		for _, m := range be.Matchers {
			mLfs = append(mLfs, LabelFilter{Label: m.Label, Op: m.Op, Value: m.Value})
		}
		mWhere := buildLabelFilterWhere(mLfs, nil) // nil resolver => use quoteIdent(label)
		if len(mWhere) > 0 {
			push([]string{mk(layerIdx-1) + ".*"}, mk(layerIdx-1), mWhere)
		}
	}

	// Line filters (pre-parser) -> WHERE on current top
	lineWhere := buildLineFilterWhere(be.LineFilters, bodyCol)
	if len(lineWhere) > 0 {
		push([]string{mk(layerIdx-1) + ".*"}, mk(layerIdx-1), lineWhere)
	}

	// Track the current top alias
	top := func() string { return mk(layerIdx - 1) }

	// Collect label filters that aren't tied to a parser (or we can't tell):
	remainingLF := make([]LabelFilter, 0, len(be.LabelFilters))
	remainingLF = append(remainingLF, be.LabelFilters...)

	// -------- Pre-scan for labels that will be created by any later label_format stage --------
	futureCreated := make(map[string]struct{})
	for _, p := range be.Parsers {
		switch strings.ToLower(p.Type) {
		case "label_format", "label-format", "labelformat":
			for k := range p.Params {
				futureCreated[k] = struct{}{}
			}
		}
	}
	uniqLabels := func(lfs []LabelFilter) []string {
		set := map[string]struct{}{}
		for _, lf := range lfs {
			set[lf.Label] = struct{}{}
		}
		out := make([]string, 0, len(set))
		for k := range set {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	excludeFuture := func(keys []string) []string {
		out := make([]string, 0, len(keys))
		for _, k := range keys {
			if _, later := futureCreated[k]; !later {
				out = append(out, k)
			}
		}
		return out
	}
	mkSet := func(keys []string) map[string]struct{} {
		m := make(map[string]struct{}, len(keys))
		for _, k := range keys {
			m[k] = struct{}{}
		}
		return m
	}

	// Emit parser/transform layers left→right.
	for _, p := range be.Parsers {
		switch strings.ToLower(p.Type) {

		case "regexp":
			pat := p.Params["pattern"]
			names := regexCaptureNames(pat)
			// Projection: carry all columns + extracted captures.
			selects := []string{top() + ".*"}
			for i, name := range names {
				// DuckDB: group index is 1-based
				selects = append(selects,
					fmt.Sprintf("regexp_extract(%s, %s, %d) AS %s",
						bodyCol, sqlQuote(pat), i+1, quoteIdent(name)))
			}
			push(selects, top(), nil)

			// Any label filters targeting these extracted names? apply now.
			if len(names) > 0 {
				created := mkSet(names)
				now, later := partitionByNames(remainingLF, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now /* colResolver */, nil)
					push([]string{top() + ".*"}, top(), where)
				}
				remainingLF = later
			}

		case "json":
			// Project only the JSON keys we intend to filter on *now*, excluding those
			// that will be introduced later by label_format.
			need := uniqLabels(remainingLF)
			need = excludeFuture(need)

			selects := []string{top() + ".*"}
			for _, k := range need {
				path := jsonPathForKey(k)
				selects = append(selects, fmt.Sprintf("json_extract_string(%s, %s) AS %s", bodyCol, sqlQuote(path), quoteIdent(k)))
			}
			push(selects, top(), nil)

			// Apply only the filters that reference the columns we just created here.
			if len(need) > 0 {
				created := mkSet(need)
				now, later := partitionByNames(remainingLF, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now /* colResolver */, nil)
					push([]string{top() + ".*"}, top(), where)
				}
				remainingLF = later
			}

		case "logfmt":
			// For logfmt, synthesize columns for referenced labels using simple regex extracts,
			// excluding any labels that will be introduced later by label_format.
			need := uniqLabels(remainingLF)
			need = excludeFuture(need)

			selects := []string{top() + ".*"}
			for _, k := range need {
				// key=value with no spaces in value
				reKey := fmt.Sprintf(`(?:^|\s)%s=([^\s]+)`, regexp.QuoteMeta(k))
				selects = append(selects,
					fmt.Sprintf("regexp_extract(%s, %s, 1) AS %s", bodyCol, sqlQuote(reKey), quoteIdent(k)))
			}
			push(selects, top(), nil)

			// Apply only filters that reference the columns created in this stage.
			if len(need) > 0 {
				created := mkSet(need)
				now, later := partitionByNames(remainingLF, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now /* colResolver */, nil)
					push([]string{top() + ".*"}, top(), where)
				}
				remainingLF = later
			}

		case "label_format", "label-format", "labelformat":
			selects := []string{top() + ".*"}
			created := make(map[string]struct{})

			if len(p.LabelFormats) > 0 {
				for _, lf := range p.LabelFormats {
					selects = append(selects, fmt.Sprintf("(%s) AS %s", lf.SQL, quoteIdent(lf.Out)))
					created[lf.Out] = struct{}{}
				}
			} else {
				// Back-compat path (rare if parser is updated)
				keys := make([]string, 0, len(p.Params))
				for k := range p.Params {
					keys = append(keys, k)
				}
				sort.Strings(keys)
				for _, outName := range keys {
					tmpl := p.Params[outName]
					expr, err := buildLabelFormatExprTemplate(tmpl, func(s string) string { return quoteIdent(s) })
					if err != nil {
						expr = "''"
					}
					selects = append(selects, fmt.Sprintf("(%s) AS %s", expr, quoteIdent(outName)))
					created[outName] = struct{}{}
				}
			}

			push(selects, top(), nil)

			// Apply any label filters that reference newly created names
			now, later := partitionByNames(remainingLF, created)
			if len(now) > 0 {
				where := buildLabelFilterWhere(now, nil)
				push([]string{top() + ".*"}, top(), where)
			}
			remainingLF = later

		default:
			// Unknown parser: just pass-through (keeps layering predictable)
			push([]string{top() + ".*"}, top(), nil)
		}
	}

	// Any label filters we didn’t place yet? apply at the end.
	if len(remainingLF) > 0 {
		where := buildLabelFilterWhere(remainingLF /* colResolver */, nil)
		push([]string{top() + ".*"}, top(), where)
	}

	// Final SELECT
	var sb strings.Builder
	sb.WriteString("WITH\n")
	for i, l := range layers {
		sb.WriteString("  ")
		sb.WriteString(l.sql)
		if i != len(layers)-1 {
			sb.WriteString(",\n")
		} else {
			sb.WriteString("\n")
		}
	}
	sb.WriteString("SELECT * FROM ")
	sb.WriteString(mk(layerIdx - 1))
	sb.WriteString(";\n")
	return sb.String()
}

// ---------- helpers ----------

func sqlQuote(s string) string { // single-quote and escape for SQL literals
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

func quoteIdent(s string) string {
	// super conservative: allow [A-Za-z0-9_], otherwise double-quote
	if regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`).MatchString(s) {
		return s
	}
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func regexCaptureNames(pattern string) []string {
	// find all (?P<name>...) left→right
	re := regexp.MustCompile(`\(\?P<([A-Za-z_][A-Za-z0-9_]*)>`)
	m := re.FindAllStringSubmatchIndex(pattern, -1)
	if len(m) == 0 {
		return nil
	}
	names := make([]string, 0, len(m))
	for _, idx := range m {
		// submatch 2 is the name group
		names = append(names, pattern[idx[2]:idx[3]])
	}
	return names
}

func buildLineFilterWhere(lfs []LineFilter, bodyCol string) []string {
	var where []string
	for _, lf := range lfs {
		switch lf.Op {
		case LineContains:
			where = append(where, fmt.Sprintf("%s LIKE %s", bodyCol, sqlQuote("%"+lf.Match+"%")))
		case LineNotContains:
			where = append(where, fmt.Sprintf("NOT (%s LIKE %s)", bodyCol, sqlQuote("%"+lf.Match+"%")))
		case LineRegex:
			where = append(where, fmt.Sprintf("regexp_matches(%s, %s)", bodyCol, sqlQuote(lf.Match)))
		case LineNotRegex:
			where = append(where, fmt.Sprintf("NOT regexp_matches(%s, %s)", bodyCol, sqlQuote(lf.Match)))
		}
	}
	return where
}

func buildLabelFilterWhere(lfs []LabelFilter, colResolver func(string) string) []string {
	var where []string
	for _, lf := range lfs {
		col := lf.Label
		if colResolver != nil {
			col = colResolver(lf.Label)
		}
		switch lf.Op {
		case MatchEq:
			where = append(where, fmt.Sprintf("%s = %s", quoteIdent(col), sqlQuote(lf.Value)))
		case MatchNe:
			where = append(where, fmt.Sprintf("%s <> %s", quoteIdent(col), sqlQuote(lf.Value)))
		case MatchRe:
			where = append(where, fmt.Sprintf("regexp_matches(%s, %s)", quoteIdent(col), sqlQuote(lf.Value)))
		case MatchNre:
			where = append(where, fmt.Sprintf("NOT regexp_matches(%s, %s)", quoteIdent(col), sqlQuote(lf.Value)))
		}
	}
	return where
}

// Partition label filters by whether their Label is in "created" (new columns just projected).
func partitionByNames(lfs []LabelFilter, created map[string]struct{}) (now []LabelFilter, later []LabelFilter) {
	for _, lf := range lfs {
		if _, ok := created[lf.Label]; ok {
			now = append(now, lf)
		} else {
			later = append(later, lf)
		}
	}
	return now, later
}

var labelFormatTemplateFuncs = template.FuncMap{
	// boolean predicates (signatures match our intended DSL;
	// they won't run, but the parser needs them defined)
	"hasPrefix": func(prefix, s string) bool { return strings.HasPrefix(s, prefix) },
	"hasSuffix": func(suffix, s string) bool { return strings.HasSuffix(s, suffix) },
	"contains":  func(sub, s string) bool { return strings.Contains(s, sub) },
	"match":     func(re, s string) bool { return false }, // stub; parsing only
	"eq":        func(a, b string) bool { return a == b },
	"ne":        func(a, b string) bool { return a != b },

	// string transforms
	"lower": func(s string) string { return strings.ToLower(s) },
	"upper": func(s string) string { return strings.ToUpper(s) },
	"trim":  func(s string) string { return strings.TrimSpace(s) },
}

func buildLabelFormatExprTemplate(tmpl string, colResolver func(string) string) (string, error) {
	// strip wrapping backticks or quotes if present
	if n := len(tmpl); n >= 2 {
		if (tmpl[0] == '`' && tmpl[n-1] == '`') || (tmpl[0] == '"' && tmpl[n-1] == '"') {
			tmpl = tmpl[1 : n-1]
		}
	}
	if strings.Contains(tmpl, `\"`) {
		tmpl = strings.ReplaceAll(tmpl, `\"`, `"`)
	}

	t, err := template.New("lf").Funcs(labelFormatTemplateFuncs).Parse(tmpl)
	if err != nil {
		return "", fmt.Errorf("parse label_format template: %w", err)
	}
	if t.Tree == nil || t.Tree.Root == nil {
		return "''", nil
	}
	return genStringFromList(t.Tree.Root, colResolver)
}

// ---- Template → SQL (string) ----

func genStringFromList(list *parse.ListNode, colResolver func(string) string) (string, error) {
	if list == nil || len(list.Nodes) == 0 {
		return "''", nil
	}
	var parts []string
	for _, n := range list.Nodes {
		s, err := genStringFromNode(n, colResolver)
		if err != nil {
			return "", err
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, " || "), nil
}

func genStringFromNode(n parse.Node, colResolver func(string) string) (string, error) {
	switch x := n.(type) {
	case *parse.TextNode:
		if len(x.Text) == 0 {
			return "''", nil
		}
		return sqlQuote(string(x.Text)), nil

	case *parse.ActionNode: // {{ ... }}
		// In string context, only allow field refs or string-producing function calls.
		return genStringFromPipe(x.Pipe, colResolver)

	case *parse.IfNode:
		cond, err := genBoolFromPipe(x.Pipe, colResolver)
		if err != nil {
			return "", err
		}
		thenExpr, err := genStringFromList(x.List, colResolver)
		if err != nil {
			return "", err
		}
		elseExpr := "''"
		if x.ElseList != nil {
			elseExpr, err = genStringFromList(x.ElseList, colResolver)
			if err != nil {
				return "", err
			}
		}
		return fmt.Sprintf("(CASE WHEN %s THEN %s ELSE %s END)", cond, thenExpr, elseExpr), nil

	default:
		return "", fmt.Errorf("label_format: unsupported node %T", n)
	}
}

// ---- Pipe/Command helpers ----

func genStringFromPipe(p *parse.PipeNode, colResolver func(string) string) (string, error) {
	// Allow {{ .field }} or {{ lower .field }} or pipelines like {{ .field | lower }}
	sql, typ, err := evalPipe(p, colResolver)
	if err != nil {
		return "", err
	}
	// Cast/coalesce to VARCHAR in string context
	switch typ {
	case "string":
		return fmt.Sprintf("coalesce(cast(%s as varchar),'')", sql), nil
	case "bool":
		// if someone inlines a bool in a string context, convert to 'true'/'false'
		return fmt.Sprintf("(CASE WHEN %s THEN 'true' ELSE 'false' END)", sql), nil
	default:
		return fmt.Sprintf("cast(%s as varchar)", sql), nil
	}
}

func genBoolFromPipe(p *parse.PipeNode, colResolver func(string) string) (string, error) {
	sql, typ, err := evalPipe(p, colResolver)
	if err != nil {
		return "", err
	}
	if typ == "bool" {
		return sql, nil
	}
	// Truthiness for strings: non-empty → true
	if typ == "string" {
		return fmt.Sprintf("(coalesce(cast(%s as varchar),'') <> '')", sql), nil
	}
	return "", fmt.Errorf("label_format: condition must be boolean or string, got %s", typ)
}

// Evaluates a pipe and returns (sql, type) where type ∈ {"string","bool","other"}.
func evalPipe(p *parse.PipeNode, colResolver func(string) string) (string, string, error) {
	if p == nil || len(p.Cmds) == 0 {
		return "''", "string", nil
	}
	// Evaluate first command
	sql, typ, err := evalCommand(p.Cmds[0], nil, colResolver)
	if err != nil {
		return "", "", err
	}
	// Pipe the value into subsequent commands
	for i := 1; i < len(p.Cmds); i++ {
		sql, typ, err = evalCommand(p.Cmds[i], []arg{{sql, typ}}, colResolver)
		if err != nil {
			return "", "", err
		}
	}
	return sql, typ, nil
}

type arg struct {
	sql string
	typ string
}

func evalCommand(c *parse.CommandNode, prev []arg, colResolver func(string) string) (string, string, error) {
	if len(c.Args) == 0 {
		return "''", "string", nil
	}
	// First token decides: func call vs field/constant
	switch a := c.Args[0].(type) {
	case *parse.IdentifierNode:
		// function call
		fn := a.Ident
		var args []arg
		for _, raw := range c.Args[1:] {
			aa, err := evalArg(raw, colResolver)
			if err != nil {
				return "", "", err
			}
			args = append(args, aa)
		}
		// pipeline: append previous value as last arg
		args = append(args, prev...)
		return callFunc(fn, args)

	case *parse.FieldNode: // {{ .field }}
		name := strings.Join(a.Ident, ".")
		return colResolver(name), "string", nil

	case *parse.VariableNode: // {{ $x }} unsupported in this DSL
		return "", "", fmt.Errorf("label_format: variables not supported")

	case *parse.StringNode:
		// a.Quoted already contains quotes
		return a.Quoted, "string", nil

	default:
		return "", "", fmt.Errorf("label_format: unsupported command arg %T", a)
	}
}

func evalArg(n parse.Node, colResolver func(string) string) (arg, error) {
	switch x := n.(type) {
	case *parse.StringNode:
		// x.Quoted includes surrounding quotes
		return arg{sql: "'" + strings.ReplaceAll(x.String(), "\"", "") + "'", typ: "string"}, nil
	case *parse.NumberNode:
		return arg{sql: x.Text, typ: "other"}, nil
	case *parse.FieldNode:
		name := strings.Join(x.Ident, ".")
		return arg{sql: colResolver(name), typ: "string"}, nil
	case *parse.IdentifierNode:
		// bare identifier used as a string: treat as error to avoid ambiguity
		return arg{}, fmt.Errorf("label_format: bare identifier %q not allowed", x.Ident)
	default:
		return arg{}, fmt.Errorf("label_format: unsupported arg %T", n)
	}
}

// Map whitelisted template funcs to DuckDB SQL.
// Return (sql, type)
func callFunc(name string, args []arg) (string, string, error) {
	switch name {
	// boolean predicates
	case "hasPrefix":
		// hasPrefix "prefix" s   or   s | hasPrefix "prefix"
		if len(args) != 2 {
			return "", "", fmt.Errorf("hasPrefix needs 2 args")
		}
		// DuckDB: starts_with(s, prefix)
		return fmt.Sprintf("starts_with(%s, %s)", args[1].sql, args[0].sql), "bool", nil
	case "hasSuffix":
		if len(args) != 2 {
			return "", "", fmt.Errorf("hasSuffix needs 2 args")
		}
		return fmt.Sprintf("ends_with(%s, %s)", args[1].sql, args[0].sql), "bool", nil
	case "contains":
		if len(args) != 2 {
			return "", "", fmt.Errorf("contains needs 2 args")
		}
		// DuckDB has contains(haystack, needle) -> bool
		return fmt.Sprintf("contains(%s, %s)", args[1].sql, args[0].sql), "bool", nil
	case "match":
		if len(args) != 2 {
			return "", "", fmt.Errorf("match needs 2 args")
		}
		// Interpret last arg as haystack, first as regex:
		return fmt.Sprintf("regexp_matches(%s, %s)", args[1].sql, args[0].sql), "bool", nil

	case "eq":
		if len(args) != 2 {
			return "", "", fmt.Errorf("eq needs 2 args")
		}
		return fmt.Sprintf("(%s = %s)", args[0].sql, args[1].sql), "bool", nil
	case "ne":
		if len(args) != 2 {
			return "", "", fmt.Errorf("ne needs 2 args")
		}
		return fmt.Sprintf("(%s <> %s)", args[0].sql, args[1].sql), "bool", nil

	// string transforms (string→string)
	case "lower":
		if len(args) != 1 {
			return "", "", fmt.Errorf("lower needs 1 arg")
		}
		return fmt.Sprintf("lower(%s)", args[0].sql), "string", nil
	case "upper":
		if len(args) != 1 {
			return "", "", fmt.Errorf("upper needs 1 arg")
		}
		return fmt.Sprintf("upper(%s)", args[0].sql), "string", nil
	case "trim":
		if len(args) != 1 {
			return "", "", fmt.Errorf("trim needs 1 arg")
		}
		return fmt.Sprintf("trim(%s)", args[0].sql), "string", nil
	}

	return "", "", fmt.Errorf("label_format: unsupported func %q", name)
}

var simpleIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func jsonPathForKey(k string) string {
	if simpleIdentRe.MatchString(k) {
		return "$." + k
	}
	// Use quoted member: $."resource.service.name"
	esc := strings.ReplaceAll(k, `"`, `\"`)
	return `$."` + esc + `"`
}
