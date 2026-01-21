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
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"text/template"
	"text/template/parse"
)

// ToWorkerSQLWithLimit is like ToWorkerSQL but (for non-aggregated leaves) appends
func (be *LogLeaf) ToWorkerSQLWithLimit(limit int, order string, fields []string) string {
	return be.ToWorkerSQL(limit, order, fields)
}

func (be *LogLeaf) ToWorkerSQLForTagValues(tagName string) string {
	const baseRel = "{table}"       // replace upstream
	const bodyCol = `"log_message"` // quoted column for message text
	const tsCol = `"chq_timestamp"` // quoted column for event timestamp

	// Check if the tagName is created by any parser
	tagCreatedByParser := false
	for _, p := range be.Parsers {
		switch strings.ToLower(p.Type) {
		case "regexp":
			pat := p.Params["pattern"]
			names := regexCaptureNames(pat)
			if slices.Contains(names, tagName) {
				tagCreatedByParser = true
			}
		case "json":
			// For JSON, we need to check if the tagName matches any JSON path
			if _, exists := p.Params[tagName]; exists {
				tagCreatedByParser = true
			}
		case "logfmt":
			// For logfmt, check if the tagName is in the params
			if _, exists := p.Params[tagName]; exists {
				tagCreatedByParser = true
			}
		}
	}

	// If the tag is created by a parser, we need to build a more complex query
	if tagCreatedByParser {
		return be.buildTagValuesQueryWithParsers(tagName)
	}

	// For tags that exist in the base table, use a simpler query
	var whereConds []string

	// Add time range filter
	whereConds = append(whereConds, fmt.Sprintf("%s >= {start} AND %s <= {end}", tsCol, tsCol))

	// Apply selector matchers
	if len(be.Matchers) > 0 {
		mLfs := make([]LabelFilter, 0, len(be.Matchers))
		for _, m := range be.Matchers {
			mLfs = append(mLfs, LabelFilter{Label: m.Label, Op: m.Op, Value: m.Value})
		}
		mWhere := buildLabelFilterWhere(mLfs, nil) // nil resolver => use quoteIdent(label)
		whereConds = append(whereConds, mWhere...)
	}

	// Apply line filters
	lineWhere := buildLineFilterWhere(be.LineFilters, bodyCol)
	whereConds = append(whereConds, lineWhere...)

	// Apply label filters that don't depend on parsers
	remainingLF := make([]LabelFilter, 0, len(be.LabelFilters))
	for _, lf := range be.LabelFilters {
		if lf.Label != tagName {
			remainingLF = append(remainingLF, lf)
		}
	}
	if len(remainingLF) > 0 {
		lfWhere := buildLabelFilterWhere(remainingLF, nil)
		whereConds = append(whereConds, lfWhere...)
	}

	// Add filter to ensure the tag column exists and is not null
	whereConds = append(whereConds, fmt.Sprintf("%s IS NOT NULL", quoteIdent(tagName)))

	// Build the final SQL query
	var whereClause string
	if len(whereConds) > 0 {
		whereClause = " WHERE " + strings.Join(whereConds, " AND ")
	}

	sql := "SELECT DISTINCT " + quoteIdent(tagName) + " AS tag_value" +
		" FROM " + baseRel + whereClause +
		" ORDER BY tag_value ASC"

	return sql
}

// ToWorkerSQLForTagNames generates a DuckDB SQL query that returns distinct column names
// (tag names) that have at least one non-null value in rows matching the filter criteria.
// This is used for scoped tag discovery - finding which tags are relevant for a given filter.
func (be *LogLeaf) ToWorkerSQLForTagNames() string {
	const baseRel = "{table}"
	const bodyCol = `"log_message"`
	const tsCol = `"chq_timestamp"`

	// System columns to exclude from tag names - these are not user-facing tags
	// Note: We only exclude columns that are guaranteed to exist in all log tables.
	// The COLUMNS(*)::VARCHAR cast handles type conversion for any BIGINT columns.
	excludeCols := []string{
		"chq_timestamp",
		"chq_id",
		"chq_fingerprint",
		"log_message",
	}

	var whereConds []string

	// Add time range filter
	whereConds = append(whereConds, fmt.Sprintf("%s >= {start} AND %s <= {end}", tsCol, tsCol))

	// Apply selector matchers
	if len(be.Matchers) > 0 {
		mLfs := make([]LabelFilter, 0, len(be.Matchers))
		for _, m := range be.Matchers {
			mLfs = append(mLfs, LabelFilter{Label: m.Label, Op: m.Op, Value: m.Value})
		}
		mWhere := buildLabelFilterWhere(mLfs, nil)
		whereConds = append(whereConds, mWhere...)
	}

	// Apply line filters
	lineWhere := buildLineFilterWhere(be.LineFilters, bodyCol)
	whereConds = append(whereConds, lineWhere...)

	// Apply label filters
	if len(be.LabelFilters) > 0 {
		lfWhere := buildLabelFilterWhere(be.LabelFilters, nil)
		whereConds = append(whereConds, lfWhere...)
	}

	var whereClause string
	if len(whereConds) > 0 {
		whereClause = " WHERE " + strings.Join(whereConds, " AND ")
	}

	// Build EXCLUDE clause for system columns
	var excludeClause string
	if len(excludeCols) > 0 {
		quotedCols := make([]string, len(excludeCols))
		for i, col := range excludeCols {
			quotedCols[i] = quoteIdent(col)
		}
		excludeClause = " EXCLUDE (" + strings.Join(quotedCols, ", ") + ")"
	}

	// Use UNPIVOT to transform columns into rows, then get distinct non-null column names
	// The query:
	// 1. Casts all columns to VARCHAR (UNPIVOT requires homogeneous types)
	// 2. Filters rows based on matchers/line filters/label filters
	// 3. Unpivots all columns (except system columns) into name/value rows
	// 4. Filters out NULL values and empty strings
	// 5. Returns distinct column names as tag_value (to reuse tagValuesMapper)
	//
	// Note: We use a subquery with COLUMNS(*)::VARCHAR to cast all columns to VARCHAR
	// before UNPIVOT, avoiding "Cannot unpivot columns of types VARCHAR and BIGINT" errors.
	sql := "SELECT DISTINCT col_name AS tag_value FROM (" +
		"UNPIVOT (" +
		"SELECT *" + excludeClause + " FROM (" +
		"SELECT COLUMNS(*)::VARCHAR FROM " + baseRel + whereClause +
		")" +
		") ON COLUMNS(*) INTO NAME col_name VALUE col_value" +
		") WHERE col_value IS NOT NULL AND col_value != '' " +
		"ORDER BY tag_value ASC"

	return sql
}

// buildTagValuesQueryWithParsers builds a complex query when the tag is extracted by parsers
func (be *LogLeaf) buildTagValuesQueryWithParsers(tagName string) string {
	const baseRel = "{table}"         // replace upstream
	const bodyCol = "\"log_message\"" // quoted column for message text
	const tsCol = "\"chq_timestamp\"" // quoted column for event timestamp

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

	// ---- s0: minimal base projection ----
	need := map[string]struct{}{
		bodyCol:         {},
		tsCol:           {},
		"\"chq_id\"":    {},
		"\"log_level\"": {},
	}
	for _, m := range be.Matchers {
		need[quoteIdent(m.Label)] = struct{}{}
	}
	var s0Select []string
	for col := range need {
		s0Select = append(s0Select, col)
	}
	sort.Strings(s0Select)
	push(s0Select, baseRel, nil)

	// Sentinel layer so cache manager can splice segment filter via "AND true".
	push([]string{mk(layerIdx-1) + ".*"}, mk(layerIdx-1), []string{"1=1", "true"})

	// Track the current top alias
	top := func() string { return mk(layerIdx - 1) }

	// Apply selector matchers early (WHERE on current top).
	if len(be.Matchers) > 0 {
		mLfs := make([]LabelFilter, 0, len(be.Matchers))
		for _, m := range be.Matchers {
			mLfs = append(mLfs, LabelFilter{Label: m.Label, Op: m.Op, Value: m.Value})
		}
		mWhere := buildLabelFilterWhere(mLfs, nil) // nil resolver => use quoteIdent(label)
		if len(mWhere) > 0 {
			push([]string{top() + ".*"}, top(), mWhere)
		}
	}

	// Line filters (pre-parser) -> WHERE on current top
	lineWhere := buildLineFilterWhere(be.LineFilters, bodyCol)
	if len(lineWhere) > 0 {
		push([]string{top() + ".*"}, top(), lineWhere)
	}

	// Collect label filters that aren't tied to a parser (or we can't tell) yet.
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
			selects := []string{top() + ".*"}

			// Project each *named* capture directly by its position.
			for i, name := range names {
				if strings.HasPrefix(name, "__var_") {
					continue // skip unnamed captures
				}
				idx := i + 1 // regexp group positions are 1-based
				selects = append(selects,
					fmt.Sprintf("regexp_extract(%s, %s, %d) AS %s",
						bodyCol, sqlQuote(pat), idx, quoteIdent(name)))
			}
			push(selects, top(), nil)

			// Apply label filters that target these extracted names now.
			if len(names) > 0 {
				created := make(map[string]struct{})
				for _, name := range names {
					if strings.HasPrefix(name, "__var_") {
						continue
					}
					created[name] = struct{}{}
				}
				now, later := partitionByNames(remainingLF, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now, nil)
					push([]string{top() + ".*"}, top(), where)
				}
				remainingLF = later
			}

		case "json":
			needKeys := uniqLabels(remainingLF)
			needKeys = excludeFuture(needKeys)

			// Always include the tagName if it's in the parser params
			if _, exists := p.Params[tagName]; exists {
				found := false
				for _, k := range needKeys {
					if k == tagName {
						found = true
						break
					}
				}
				if !found {
					needKeys = append(needKeys, tagName)
				}
			}

			selects := []string{top() + ".*"}
			for _, k := range needKeys {
				path := jsonPathForKey(k)
				selects = append(selects, fmt.Sprintf("json_extract_string(%s, %s) AS %s", bodyCol, sqlQuote(path), quoteIdent(k)))
			}
			push(selects, top(), nil)

			if len(needKeys) > 0 {
				created := mkSet(needKeys)
				now, later := partitionByNames(remainingLF, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now, nil)
					push([]string{top() + ".*"}, top(), where)
				}
				remainingLF = later
			}

		case "logfmt":
			needKeys := uniqLabels(remainingLF)
			needKeys = excludeFuture(needKeys)

			// Always include the tagName if it's in the parser params
			if _, exists := p.Params[tagName]; exists {
				found := false
				for _, k := range needKeys {
					if k == tagName {
						found = true
						break
					}
				}
				if !found {
					needKeys = append(needKeys, tagName)
				}
			}

			selects := []string{top() + ".*"}
			for _, k := range needKeys {
				reKey := fmt.Sprintf(`(?:^|\s)%s=([^\s]+)`, regexp.QuoteMeta(k))
				selects = append(selects,
					fmt.Sprintf("regexp_extract(%s, %s, 1) AS %s", bodyCol, sqlQuote(reKey), quoteIdent(k)))
			}
			push(selects, top(), nil)

			if len(needKeys) > 0 {
				created := mkSet(needKeys)
				now, later := partitionByNames(remainingLF, created)
				if len(now) > 0 {
					where := buildLabelFilterWhere(now, nil)
					push([]string{top() + ".*"}, top(), where)
				}
				remainingLF = later
			}

		case "label_format", "label-format", "labelformat":
			selects := []string{top() + ".*"}
			for k, v := range p.Params {
				selects = append(selects, fmt.Sprintf("(%s) AS %s", v, quoteIdent(k)))
			}
			push(selects, top(), nil)

			// Apply label filters that target these new labels now.
			created := mkSet(keys(p.Params))
			now, later := partitionByNames(remainingLF, created)
			if len(now) > 0 {
				where := buildLabelFilterWhere(now, nil)
				push([]string{top() + ".*"}, top(), where)
			}
			remainingLF = later
		}
	}

	// Apply any remaining label filters
	if len(remainingLF) > 0 {
		where := buildLabelFilterWhere(remainingLF, nil)
		push([]string{top() + ".*"}, top(), where)
	}

	// Add time range filter
	timeWhere := []string{fmt.Sprintf("%s >= {start} AND %s <= {end}", tsCol, tsCol)}
	push([]string{top() + ".*"}, top(), timeWhere)

	// Add filter to ensure the tag column exists and is not null
	nullWhere := []string{fmt.Sprintf("%s IS NOT NULL", quoteIdent(tagName))}
	push([]string{top() + ".*"}, top(), nullWhere)

	// Build the final SQL query
	var sb strings.Builder
	sb.WriteString("WITH ")
	for i, layer := range layers {
		if i > 0 {
			sb.WriteString(",\n")
		}
		sb.WriteString(layer.sql)
	}

	sb.WriteString("\nSELECT DISTINCT ")
	sb.WriteString(quoteIdent(tagName))
	sb.WriteString(" AS tag_value FROM ")
	sb.WriteString(mk(layerIdx - 1))
	sb.WriteString(" ORDER BY tag_value ASC")

	return sb.String()
}

// Helper function to get keys from a map
func keys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
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

var numLitRE = regexp.MustCompile(`^\s*-?(?:\d+(?:\.\d*)?|\.\d+)\s*$`)

func asNumericSQL(lit string) string {
	// If it's a bare numeric, pass through unchanged.
	if numLitRE.MatchString(lit) {
		return strings.TrimSpace(lit)
	}
	// Fall back to casting a string literal → DOUBLE (safe + predictable).
	return fmt.Sprintf("try_cast(%s AS DOUBLE)", sqlQuote(lit))
}

func buildLabelFilterWhere(lfs []LabelFilter, colOverride map[string]string) []string {
	var out []string
	for _, lf := range lfs {
		col := quoteIdent(lf.Label)
		if colOverride != nil {
			if alt, ok := colOverride[lf.Label]; ok && alt != "" {
				col = alt
			}
		}

		switch lf.Op {
		case MatchEq:
			out = append(out, fmt.Sprintf("%s = %s", col, sqlQuote(lf.Value)))
		case MatchNe:
			out = append(out, fmt.Sprintf("%s <> %s", col, sqlQuote(lf.Value)))
		case MatchRe:
			out = append(out, fmt.Sprintf("regexp_matches(%s, %s)", col, sqlQuote(lf.Value)))
		case MatchNre:
			out = append(out, fmt.Sprintf("NOT regexp_matches(%s, %s)", col, sqlQuote(lf.Value)))

		case MatchGt, MatchLt, MatchGte, MatchLte:
			rhs := asNumericSQL(lf.Value)
			out = append(out, fmt.Sprintf("try_cast(%s AS DOUBLE) %s %s", col, string(lf.Op), rhs))

		default:
			// Fallback to equality to be conservative
			out = append(out, fmt.Sprintf("%s = %s", col, sqlQuote(lf.Value)))
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
	"len": func(s string) int {
		var arr []any
		err := json.Unmarshal([]byte(s), &arr)
		if err == nil {
			return len(arr)
		}
		return len(s)
	},
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
	if t.Tree == nil || t.Root == nil {
		return "''", nil
	}
	return genStringFromList(t.Root, colResolver)
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

func joinIndexKeys(keys []string) string {
	if len(keys) == 0 {
		return ""
	}
	// If you prefer flat single key only, just return keys[0]
	return strings.Join(keys, ".")
}

func evalCommand(c *parse.CommandNode, prev []arg, colResolver func(string) string) (string, string, error) {
	if len(c.Args) == 0 {
		return "''", "string", nil
	}

	switch a := c.Args[0].(type) {
	case *parse.IdentifierNode:
		fn := a.Ident

		if fn == "index" {
			if len(c.Args) < 3 {
				return "", "", fmt.Errorf("label_format: index requires root (.) and at least one string key")
			}
			// 1) First arg must be DotNode (.) or $ (if you want to allow it)
			switch root := c.Args[1].(type) {
			case *parse.DotNode:
				// ok
			case *parse.VariableNode:
				// allow only "$" if you want to accept it
				if len(root.Ident) != 1 || root.Ident[0] != "$" {
					return "", "", fmt.Errorf("label_format: index first arg must be . or $")
				}
			default:
				return "", "", fmt.Errorf("label_format: index first arg must be . or $")
			}

			// 2) Collect string keys
			var keys []string
			for _, n := range c.Args[2:] {
				sn, ok := n.(*parse.StringNode)
				if !ok {
					return "", "", fmt.Errorf("label_format: index keys must be string literals")
				}
				// sn.Text is the unquoted value already
				keys = append(keys, sn.Text)
			}
			if len(keys) == 0 {
				return "", "", fmt.Errorf("label_format: index requires at least one key")
			}

			// 3) Build the column name
			// For your case: "log.@OrderResult" => exactly that.
			// If you want nested support, joinIndexKeys(keys) → "a.b"
			colName := joinIndexKeys(keys)

			// 4) Return it as a string-typed SQL expression via the resolver
			return colResolver(colName), "string", nil
		}

		// Normal function call path (your existing behavior)
		var args []arg
		for _, raw := range c.Args[1:] {
			aa, err := evalArg(raw, colResolver)
			if err != nil {
				return "", "", err
			}
			args = append(args, aa)
		}
		args = append(args, prev...)
		return callFunc(fn, args)

	case *parse.FieldNode: // {{ .field }}
		name := strings.Join(a.Ident, ".")
		return colResolver(name), "string", nil

	case *parse.VariableNode:
		return "", "", fmt.Errorf("label_format: variables not supported")

	case *parse.StringNode:
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
		return fmt.Sprintf("contains(%s, %s)", args[1].sql, args[0].sql), "bool", nil
	case "match":
		if len(args) != 2 {
			return "", "", fmt.Errorf("match needs 2 args")
		}
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

	case "len":
		if len(args) != 1 {
			return "", "", fmt.Errorf("len needs 1 arg")
		}
		s := args[0].sql
		return fmt.Sprintf(`(CASE WHEN try_cast(%s AS JSON) IS NOT NULL THEN json_array_length(cast(%s AS JSON)) ELSE length(cast(%s AS VARCHAR)) END)`, s, s, s), "int64", nil
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

// duration strings like "250ms", "2s", "3m", "1.5h" -> seconds as DOUBLE
func unwrapDurationExpr(col string) string {
	num := fmt.Sprintf("try_cast(regexp_extract(%s, '([0-9]*\\.?[0-9]+)', 1) AS DOUBLE)", col)
	unit := fmt.Sprintf("coalesce(regexp_extract(%s, '(ns|us|µs|ms|s|m|h)', 1), 's')", col)
	return fmt.Sprintf(`CASE %s
		WHEN 'ns' THEN %s/1e9
		WHEN 'us' THEN %s/1e6
		WHEN 'µs' THEN %s/1e6
		WHEN 'ms' THEN %s/1e3
		WHEN 's'  THEN %s
		WHEN 'm'  THEN %s*60
		WHEN 'h'  THEN %s*3600
		ELSE try_cast(%s AS DOUBLE) END`,
		unit, num, num, num, num, num, num, num, col)
}

// size strings like "10KB", "3MiB", "512", etc. -> bytes as DOUBLE
func unwrapBytesExpr(col string) string {
	num := fmt.Sprintf("try_cast(regexp_extract(%s, '([0-9]*\\.?[0-9]+)', 1) AS DOUBLE)", col)
	unit := fmt.Sprintf("upper(coalesce(regexp_extract(%s, '(B|KB|MB|GB|TB|PB|EB|KIB|MIB|GIB|TIB|PIB|EIB)', 1), 'B'))", col)
	return fmt.Sprintf(`CASE %s
		WHEN 'B'   THEN %s
		WHEN 'KB'  THEN %s*1000
		WHEN 'MB'  THEN %s*1000*1000
		WHEN 'GB'  THEN %s*1000*1000*1000
		WHEN 'TB'  THEN %s*1000*1000*1000*1000
		WHEN 'PB'  THEN %s*1e15
		WHEN 'EB'  THEN %s*1e18
		WHEN 'KIB' THEN %s*1024
		WHEN 'MIB' THEN %s*1024*1024
		WHEN 'GIB' THEN %s*1024*1024*1024
		WHEN 'TIB' THEN %s*1024*1024*1024*1024
		WHEN 'PIB' THEN %s*1024*1024*1024*1024*1024
		WHEN 'EIB' THEN %s*1024*1024*1024*1024*1024*1024
		ELSE try_cast(%s AS DOUBLE) END`,
		unit, num, num, num, num, num, num, num, num, num, num, num, num, num, col)
}
