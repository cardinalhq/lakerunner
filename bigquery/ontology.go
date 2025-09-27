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

package bigquery

import (
	"fmt"
	"sort"
	"strings"
)

type Ontology struct {
	Facts      map[string]*Fact
	Dimensions map[string]*DimensionTable
	Templates  []QueryTemplate
}

type Fact struct {
	TableID       string
	Measures      []Measure
	TimeAttrs     []TimeAttr
	ReachableDims []DimRef
}

type DimensionTable struct {
	TableID    string
	Attributes []DimensionAttr // candidate attributes to group by
}

type Measure struct {
	TableID  string
	Column   string
	BQType   string
	Semantic string   // e.g., "amount", "count", "latency"
	Aggs     []string // allowed aggs: SUM, AVG, P50, P95, COUNT_DISTINCT, etc.
}

type DimensionAttr struct {
	TableID string
	Column  string
	BQType  string
	Role    string // e.g., "name", "category", "status", "id"
}

type TimeAttr struct {
	TableID string
	Column  string
	BQType  string
	Grains  []string // e.g., hour, day, week, month
}

type DimRef struct {
	TableID  string
	Attr     DimensionAttr
	JoinPath []EdgeRef // path from fact -> dim
}

type EdgeRef struct {
	From     string
	To       string
	ColPairs [][2]string // (from_col, to_col)
	Kind     EdgeKind
	Note     string
}

type QueryTemplate struct {
	FactTable string
	Measure   Measure
	DimCols   []DimensionAttr
	TimeAttr  *TimeAttr
	Grain     string // one of TimeAttr.Grains
	SQL       string // parameterized with @start_time, @end_time
}

// ---------- BuildOntology entrypoint ----------

// BuildOntology analyzes the BQGraph and the underlying columns to produce
// facts, dimensions, and concrete, joinable templates.
func BuildOntology(projectID string, g *BQGraph, sampleLimit int) (*Ontology, error) {

	if sampleLimit <= 0 {
		sampleLimit = 10
	}

	o := &Ontology{
		Facts:      map[string]*Fact{},
		Dimensions: map[string]*DimensionTable{},
	}

	// 1) Classify tables into facts vs dims and extract semantics
	for tableID, t := range g.Nodes {
		measures := detectMeasures(t)
		timeAttrs := detectTimeAttrs(t)
		if len(measures) > 0 {
			o.Facts[tableID] = &Fact{
				TableID:   tableID,
				Measures:  measures,
				TimeAttrs: timeAttrs,
			}
		} else {
			attrs := detectDimensionAttrs(t)
			o.Dimensions[tableID] = &DimensionTable{
				TableID:    tableID,
				Attributes: attrs,
			}
		}
	}

	// 2) For each fact, find reachable dimensions via graph paths (BFS over FK/heur)
	for _, f := range o.Facts {
		reachable := findReachableDims(g, o, f.TableID)
		f.ReachableDims = reachable
	}

	// 3) Generate joinable templates: (measure × subset of dims × a time grain)
	templates := []QueryTemplate{}
	for _, f := range o.Facts {
		// pick a default time attribute (first timestamp/date if present)
		var tattr *TimeAttr
		if len(f.TimeAttrs) > 0 {
			tattr = &f.TimeAttrs[0]
		}

		// choose a small number of best dims (you can expand/prioritize later)
		// heuristic: prefer dims with "name"/"category"/"status" roles
		cands := selectTopDims(f.ReachableDims, 6)

		// powerset could blow up; start with up to pairs/triads
		dimCombos := boundedDimCombos(cands, 0, 3) // 0..3 dims per template

		for _, m := range f.Measures {
			grains := []string{""}
			if tattr != nil && len(tattr.Grains) > 0 {
				grains = tattr.Grains
			}
			for _, grain := range grains {
				for _, dims := range dimCombos {
					sql := renderTemplateSQL(projectID, f.TableID, m, dims, tattr, grain, g)
					templates = append(templates, QueryTemplate{
						FactTable: f.TableID,
						Measure:   m,
						DimCols:   attrsOf(dims),
						TimeAttr:  tattr,
						Grain:     grain,
						SQL:       sql,
					})
				}
			}
		}
	}

	o.Templates = templates
	return o, nil
}

// ---------- Heuristics: detect measures, dims, time ----------

func detectMeasures(t *Table) []Measure {
	out := []Measure{}
	for col, typ := range t.Columns {
		up := strings.ToUpper(typ)
		if !isNumeric(up) {
			continue
		}
		if maybeIdentifier(col) { // skip ids
			continue
		}
		sem := measureSemantic(col)
		if sem == "" {
			continue
		}
		out = append(out, Measure{
			TableID:  t.ID,
			Column:   col,
			BQType:   typ,
			Semantic: sem,
			Aggs:     allowedAggs(sem, up),
		})
	}
	return out
}

func detectDimensionAttrs(t *Table) []DimensionAttr {
	attrs := []DimensionAttr{}
	for col, typ := range t.Columns {
		up := strings.ToUpper(typ)
		lc := strings.ToLower(col)
		// IDs are still useful as dims sometimes, but de-prioritize in selection
		if strings.Contains(up, "STRING") || strings.Contains(up, "BOOL") || strings.Contains(up, "BYTES") {
			role := dimRole(col)
			attrs = append(attrs, DimensionAttr{TableID: t.ID, Column: col, BQType: typ, Role: role})
		} else if lc == "id" || strings.HasSuffix(lc, "_id") {
			attrs = append(attrs, DimensionAttr{TableID: t.ID, Column: col, BQType: typ, Role: "id"})
		}
	}
	// stable order
	sort.Slice(attrs, func(i, j int) bool { return attrs[i].Column < attrs[j].Column })
	return attrs
}

func detectTimeAttrs(t *Table) []TimeAttr {
	out := []TimeAttr{}
	for col, typ := range t.Columns {
		up := strings.ToUpper(typ)
		lc := strings.ToLower(col)
		if strings.Contains(up, "TIMESTAMP") || strings.Contains(up, "DATETIME") || strings.Contains(up, "DATE") {
			grains := []string{"hour", "day", "week", "month"}
			// If DATE only, skip "hour"
			if strings.Contains(up, "DATE") && !strings.Contains(up, "TIME") {
				grains = []string{"day", "week", "month"}
			}
			// prefer event-time-ish names
			if lc == "event_time" || lc == "timestamp" || lc == "created_at" || lc == "occurred_at" {
				out = append([]TimeAttr{{TableID: t.ID, Column: col, BQType: typ, Grains: grains}}, out...)
			} else {
				out = append(out, TimeAttr{TableID: t.ID, Column: col, BQType: typ, Grains: grains})
			}
		}
	}
	return out
}

// ---------- Joinability & path finding ----------

func findReachableDims(g *BQGraph, o *Ontology, factID string) []DimRef {
	// BFS allowing edges both directions; accumulate first-attempt path to dimension tables
	type node struct {
		ID   string
		Path []EdgeRef
	}
	queue := []node{{ID: factID, Path: nil}}
	seen := map[string]bool{factID: true}
	refs := []DimRef{}

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		// consider out-edges
		for _, e := range g.Edges[cur.ID] {
			er := EdgeRef{From: e.From, To: e.To, ColPairs: e.Cols, Kind: e.Kind, Note: e.Note}
			next := e.To
			if !seen[next] {
				seen[next] = true
				p := append(append([]EdgeRef{}, cur.Path...), er)
				queue = append(queue, node{ID: next, Path: p})
				if dt, ok := o.Dimensions[next]; ok && len(dt.Attributes) > 0 {
					// pick a "display" attribute to use by default
					attr := chooseDisplayAttr(dt.Attributes)
					refs = append(refs, DimRef{TableID: next, Attr: attr, JoinPath: p})
				}
			}
		}
		// also consider reverse edges (To -> From)
		for _, outs := range g.Edges {
			for _, e := range outs {
				if e.To == cur.ID {
					// reverse
					revPairs := make([][2]string, len(e.Cols))
					for i, p := range e.Cols {
						revPairs[i] = [2]string{p[1], p[0]}
					}
					er := EdgeRef{From: e.To, To: e.From, ColPairs: revPairs, Kind: e.Kind, Note: "reverse:" + e.Note}
					next := e.From
					if !seen[next] {
						seen[next] = true
						p := append(append([]EdgeRef{}, cur.Path...), er)
						queue = append(queue, node{ID: next, Path: p})
						if dt, ok := o.Dimensions[next]; ok && len(dt.Attributes) > 0 {
							attr := chooseDisplayAttr(dt.Attributes)
							refs = append(refs, DimRef{TableID: next, Attr: attr, JoinPath: p})
						}
					}
				}
			}
		}
	}
	// dedupe by (tableID, attr.Column)
	keyed := map[string]DimRef{}
	for _, r := range refs {
		k := r.TableID + "::" + r.Attr.Column
		if _, ok := keyed[k]; !ok {
			keyed[k] = r
		}
	}
	out := make([]DimRef, 0, len(keyed))
	for _, r := range keyed {
		out = append(out, r)
	}
	// stable
	sort.Slice(out, func(i, j int) bool {
		if out[i].TableID == out[j].TableID {
			return out[i].Attr.Column < out[j].Attr.Column
		}
		return out[i].TableID < out[j].TableID
	})
	return out
}

// ---------- Template selection ----------

func selectTopDims(dims []DimRef, max int) []DimRef {
	// prioritize by role: name > category > status > id > others
	score := func(r string) int {
		switch r {
		case "name":
			return 4
		case "category":
			return 3
		case "status":
			return 2
		case "id":
			return 1
		default:
			return 0
		}
	}
	sort.Slice(dims, func(i, j int) bool {
		ri := score(dims[i].Attr.Role)
		rj := score(dims[j].Attr.Role)
		if ri == rj {
			if dims[i].TableID == dims[j].TableID {
				return dims[i].Attr.Column < dims[j].Attr.Column
			}
			return dims[i].TableID < dims[j].TableID
		}
		return ri > rj
	})
	if max > 0 && len(dims) > max {
		return dims[:max]
	}
	return dims
}

func boundedDimCombos(dims []DimRef, min, max int) [][]DimRef {
	if max > len(dims) {
		max = len(dims)
	}
	var out [][]DimRef
	// include empty set if min == 0
	if min == 0 {
		out = append(out, nil)
	}
	// combos of size 1..max
	var dfs func(start, k int, cur []DimRef)
	dfs = func(start, k int, cur []DimRef) {
		if k == 0 {
			cp := make([]DimRef, len(cur))
			copy(cp, cur)
			out = append(out, cp)
			return
		}
		for i := start; i <= len(dims)-k; i++ {
			cur = append(cur, dims[i])
			dfs(i+1, k-1, cur)
			cur = cur[:len(cur)-1]
		}
	}
	for size := 1; size <= max; size++ {
		dfs(0, size, nil)
	}
	return out
}

func attrsOf(drs []DimRef) []DimensionAttr {
	out := make([]DimensionAttr, 0, len(drs))
	for _, d := range drs {
		out = append(out, d.Attr)
	}
	return out
}

// ---------- SQL rendering ----------

func renderTemplateSQL(projectID, factID string, m Measure, dims []DimRef, tattr *TimeAttr, grain string, g *BQGraph) string {
	// Build FROM + JOINs from join paths
	type aliasInfo struct {
		Alias string
	}
	aliases := map[string]string{}
	order := []string{} // order of tables in the query
	nextAliasIdx := 0
	useAlias := func(tableID string) string {
		if a, ok := aliases[tableID]; ok {
			return a
		}
		alias := fmt.Sprintf("t%d", nextAliasIdx)
		nextAliasIdx++
		aliases[tableID] = alias
		order = append(order, tableID)
		return alias
	}

	// start with fact
	factAlias := useAlias(factID)
	joins := []string{}

	// collect all tables required by dimension join paths
	for _, d := range dims {
		prev := factID
		prevAlias := factAlias
		for _, e := range d.JoinPath {
			next := e.To
			nextAlias := useAlias(next)
			// build join condition referencing prevAlias and nextAlias
			conds := make([]string, 0, len(e.ColPairs))
			for _, p := range e.ColPairs {
				conds = append(conds, fmt.Sprintf("%s.%s = %s.%s", prevAlias, bqIdent(p[0]), nextAlias, bqIdent(p[1])))
			}
			join := fmt.Sprintf("LEFT JOIN `%s.%s` %s ON %s", projectID, next, nextAlias, strings.Join(conds, " AND "))
			joins = append(joins, join)
			prev = next
			prevAlias = nextAlias
		}
		_ = prev // not used further, but kept for clarity
	}

	// SELECT list
	selects := []string{}
	groupBys := []string{}
	// time bucket
	if tattr != nil && grain != "" {
		tf := timeTruncExpr(fmt.Sprintf("%s.%s", factAlias, bqIdent(tattr.Column)), tattr.BQType, grain)
		selects = append(selects, fmt.Sprintf("%s AS period", tf))
		groupBys = append(groupBys, "period")
	}
	// dims
	for _, d := range dims {
		a := aliases[d.TableID]
		col := fmt.Sprintf("%s.%s", a, bqIdent(d.Attr.Column))
		selects = append(selects, fmt.Sprintf("%s AS %s", col, safeAlias(d.TableID+"_"+d.Attr.Column)))
		groupBys = append(groupBys, fmt.Sprintf("%d", len(selects))) // group by ordinal
	}
	// measure (take first allowed agg)
	agg := "SUM"
	if len(m.Aggs) > 0 {
		agg = m.Aggs[0]
	}
	measureExpr := aggregateExpr(fmt.Sprintf("%s.%s", factAlias, bqIdent(m.Column)), agg)
	selects = append(selects, fmt.Sprintf("%s AS %s_%s", measureExpr, strings.ToLower(agg), safeAlias(m.Column)))

	// FROM line
	from := fmt.Sprintf("FROM `%s.%s` %s", projectID, factID, factAlias)

	// WHERE time range
	where := ""
	if tattr != nil {
		col := fmt.Sprintf("%s.%s", factAlias, bqIdent(tattr.Column))
		if strings.Contains(strings.ToUpper(tattr.BQType), "DATE") && !strings.Contains(strings.ToUpper(tattr.BQType), "TIME") {
			where = fmt.Sprintf("WHERE %s BETWEEN @start_time AND @end_time", col)
		} else {
			where = fmt.Sprintf("WHERE %s BETWEEN @start_time AND @end_time", col)
		}
	}

	// GROUP BY (if any dims/time)
	group := ""
	if len(groupBys) > 0 {
		group = "GROUP BY " + strings.Join(groupBys, ", ")
	}

	sql := "SELECT " + strings.Join(selects, ",\n       ") + "\n" +
		from + "\n" +
		strings.Join(joins, "\n") + "\n" +
		where + "\n" +
		group
	return sql
}

func timeTruncExpr(qualifiedCol, bqType, grain string) string {
	gr := strings.ToUpper(grain)
	if strings.Contains(strings.ToUpper(bqType), "DATE") && !strings.Contains(strings.ToUpper(bqType), "TIME") {
		// BigQuery: DATE_TRUNC(date_expression, DATE_PART)
		switch gr {
		case "WEEK":
			return fmt.Sprintf("DATE_TRUNC(%s, WEEK)", qualifiedCol)
		case "MONTH":
			return fmt.Sprintf("DATE_TRUNC(%s, MONTH)", qualifiedCol)
		default: // DAY
			return fmt.Sprintf("DATE_TRUNC(%s, DAY)", qualifiedCol)
		}
	}
	// TIMESTAMP/DATETIME
	switch gr {
	case "HOUR":
		return fmt.Sprintf("TIMESTAMP_TRUNC(%s, HOUR)", qualifiedCol)
	case "WEEK":
		return fmt.Sprintf("TIMESTAMP_TRUNC(%s, WEEK)", qualifiedCol)
	case "MONTH":
		return fmt.Sprintf("TIMESTAMP_TRUNC(%s, MONTH)", qualifiedCol)
	default: // DAY
		return fmt.Sprintf("TIMESTAMP_TRUNC(%s, DAY)", qualifiedCol)
	}
}

func aggregateExpr(qualifiedCol, agg string) string {
	switch strings.ToUpper(agg) {
	case "SUM":
		return "SUM(" + qualifiedCol + ")"
	case "AVG", "MEAN":
		return "AVG(" + qualifiedCol + ")"
	case "COUNT":
		return "COUNT(" + qualifiedCol + ")"
	case "COUNT_DISTINCT":
		return "COUNT(DISTINCT " + qualifiedCol + ")"
	case "P50":
		return "APPROX_QUANTILES(" + qualifiedCol + ", 2)[OFFSET(1)]"
	case "P95":
		return "APPROX_QUANTILES(" + qualifiedCol + ", 100)[OFFSET(95)]"
	case "P99":
		return "APPROX_QUANTILES(" + qualifiedCol + ", 100)[OFFSET(99)]"
	default:
		return "SUM(" + qualifiedCol + ")"
	}
}

func safeAlias(s string) string {
	s = strings.ReplaceAll(s, ".", "_")
	s = strings.ReplaceAll(s, "-", "_")
	s = strings.ReplaceAll(s, "/", "_")
	return strings.ToLower(s)
}

// ---------- Heuristic helpers ----------

func isNumeric(upType string) bool {
	return strings.Contains(upType, "INT") ||
		strings.Contains(upType, "NUMERIC") ||
		strings.Contains(upType, "BIGNUMERIC") ||
		strings.Contains(upType, "FLOAT") ||
		strings.Contains(upType, "DOUBLE")
}

func measureSemantic(col string) string {
	l := strings.ToLower(col)
	switch {
	case strings.Contains(l, "revenue"), strings.Contains(l, "amount"), strings.Contains(l, "price"), strings.Contains(l, "cost"), strings.Contains(l, "total"):
		return "amount"
	case strings.Contains(l, "count"):
		return "count"
	case strings.Contains(l, "duration"), strings.Contains(l, "latency"), strings.HasSuffix(l, "_ms"), strings.HasSuffix(l, "_sec"), strings.HasSuffix(l, "_s"):
		return "latency"
	default:
		// you can widen this over time or use a scorer
		return ""
	}
}

func allowedAggs(semantic, upType string) []string {
	switch semantic {
	case "amount":
		return []string{"SUM"}
	case "count":
		return []string{"SUM", "COUNT"} // supports pre-counted field or row count
	case "latency":
		// avoid SUM; prefer distribution-oriented stats
		return []string{"P95", "P50", "AVG"}
	default:
		// fallback based on type
		if strings.Contains(upType, "INT") || strings.Contains(upType, "NUMERIC") {
			return []string{"SUM", "AVG"}
		}
		return []string{"AVG"}
	}
}

func dimRole(col string) string {
	l := strings.ToLower(col)
	switch {
	case l == "name" || strings.HasSuffix(l, "_name"):
		return "name"
	case strings.Contains(l, "category") || strings.HasSuffix(l, "_type"):
		return "category"
	case strings.Contains(l, "status") || strings.HasSuffix(l, "_state"):
		return "status"
	case l == "id" || strings.HasSuffix(l, "_id"):
		return "id"
	default:
		return "attr"
	}
}

func chooseDisplayAttr(attrs []DimensionAttr) DimensionAttr {
	best := attrs[0]
	score := func(a DimensionAttr) int {
		switch a.Role {
		case "name":
			return 4
		case "category":
			return 3
		case "status":
			return 2
		case "id":
			return 1
		default:
			return 0
		}
	}
	for _, a := range attrs {
		if score(a) > score(best) {
			best = a
		}
	}
	return best
}

// ---------- Optional: tiny helper to preview a few templates in logs ----------

func (o *Ontology) Summarize(n int) string {
	if n <= 0 || n > len(o.Templates) {
		n = len(o.Templates)
	}
	lines := make([]string, 0, n)
	for i := 0; i < n; i++ {
		t := o.Templates[i]
		dnames := []string{}
		for _, d := range t.DimCols {
			dnames = append(dnames, d.TableID+"."+d.Column)
		}
		timeStr := "(no time)"
		if t.TimeAttr != nil && t.Grain != "" {
			timeStr = fmt.Sprintf("%s @ %s", t.TimeAttr.Column, t.Grain)
		}
		lines = append(lines, fmt.Sprintf("[%s] %s(%s) by [%s] %s",
			t.FactTable, strings.ToLower(t.Measure.Aggs[0]), t.Measure.Column, strings.Join(dnames, ", "), timeStr))
	}
	return strings.Join(lines, "\n")
}
