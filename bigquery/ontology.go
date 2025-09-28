// Copyright (C) 2025 CardinalHQ, Inc
// SPDX-License-Identifier: AGPL-3.0-only

package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/openai/openai-go"
	"github.com/openai/openai-go/option"
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
	Measure   Measure // still present for backward compat (LLM templates may set a benign default)
	DimCols   []DimensionAttr
	TimeAttr  *TimeAttr
	Grain     string // one of TimeAttr.Grains
	SQL       string // fully rendered SQL (can be non-aggregate for LLM templates)
}

// ---------- BuildOntology entrypoint ----------

// BuildOntology analyzes the BQGraph and the underlying columns to produce facts, dimensions,
// and concrete, joinable templates. It now also asks an LLM to propose per-table and cross-table
// queries based on schemas and discovered join keys.
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

	// 3) Generate traditional measure × dims × time templates
	var templates []QueryTemplate
	for _, f := range o.Facts {
		var tattr *TimeAttr
		if len(f.TimeAttrs) > 0 {
			tattr = &f.TimeAttrs[0]
		}
		cands := selectTopDims(f.ReachableDims, 6)
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

	// 4) LLM-driven templates (entity/lookup and cross-table join queries)
	// These are additive and dataset-agnostic: driven purely by schema + join keys.
	llm := newOntologyLLMFromEnv()
	if llm != nil {
		ctx := context.Background()

		// 4a) Per-table queries
		perTable := llmGeneratePerTableQueries(ctx, llm, projectID, g)
		templates = append(templates, perTable...)

		// 4b) Cross-table join queries (pairs of tables connected by edges)
		cross := llmGenerateJoinQueries(ctx, llm, projectID, g)
		templates = append(templates, cross...)
	} else {
		slog.Info("ontology LLM generation skipped (no OPENAI_API_KEY)")
	}

	// 5) Deduplicate templates by normalized SQL
	o.Templates = dedupeTemplates(templates)
	return o, nil
}

// ---------- LLM helpers for ontology ----------

type ontologyLLM struct {
	client  openai.Client
	model   openai.ChatModel
	timeout time.Duration
}

func newOntologyLLMFromEnv() *ontologyLLM {
	apiKey := os.Getenv("OPENAI_API_KEY")
	if strings.TrimSpace(apiKey) == "" {
		return nil
	}
	model := os.Getenv("OPENAI_GPT_MODEL")
	if model == "" {
		model = "gpt-5-thinking"
	}
	toSecs := 300
	if v := os.Getenv("LLM_ONTOLOGY_TIMEOUT_SECS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			toSecs = n
		}
	}
	return &ontologyLLM{
		client:  openai.NewClient(option.WithAPIKey(apiKey)),
		model:   model,
		timeout: time.Duration(toSecs) * time.Second,
	}
}

type llmQuery struct {
	Title       string `json:"title"`
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

type llmQueryList struct {
	Queries []llmQuery `json:"queries"`
}

func (l *ontologyLLM) askForQueries(ctx context.Context, sys, usr string, maxItems int) ([]llmQuery, error) {
	if l.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, l.timeout)
		defer cancel()
	}

	// Strict JSON schema: required must include every key in properties.
	respSchema := openai.ResponseFormatJSONSchemaJSONSchemaParam{
		Name:   "ontology_queries",
		Strict: openai.Bool(true),
		Schema: map[string]any{
			"type":                 "object",
			"additionalProperties": false,
			"properties": map[string]any{
				"queries": map[string]any{
					"type":     "array",
					"minItems": 1,
					"maxItems": maxItems,
					"items": map[string]any{
						"type":                 "object",
						"additionalProperties": false,
						"properties": map[string]any{
							"title":       map[string]any{"type": "string"},
							"description": map[string]any{"type": "string"},
							"sql":         map[string]any{"type": "string"},
						},
						"required": []string{"title", "description", "sql"},
					},
				},
			},
			"required": []string{"queries"},
		},
	}

	resp, err := l.client.Chat.Completions.New(ctx, openai.ChatCompletionNewParams{
		Model: l.model,
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(sys),
			openai.UserMessage(usr),
		},
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONSchema: &openai.ResponseFormatJSONSchemaParam{JSONSchema: respSchema},
		},
		Temperature: openai.Float(1.0),
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Choices) == 0 {
		return nil, fmt.Errorf("no choices returned")
	}

	var out llmQueryList
	if err := json.Unmarshal([]byte(resp.Choices[0].Message.Content), &out); err != nil {
		// If schema unmarshal ever fails, return nothing rather than crashing the pipeline.
		return nil, err
	}
	return out.Queries, nil
}

func llmGeneratePerTableQueries(ctx context.Context, llm *ontologyLLM, projectID string, g *BQGraph) []QueryTemplate {
	var out []QueryTemplate

	for tableID, t := range g.Nodes {
		// Skip purely technical tables (no attributes and no measures)
		attrs := detectDimensionAttrs(t)
		meas := detectMeasures(t)
		if len(attrs) == 0 && len(meas) == 0 {
			continue
		}

		// Build a compact schema card for the table + immediate join keys to neighbors.
		tableCard := schemaCard(projectID, tableID, t)
		joinNotes := joinKeyNotes(projectID, tableID, g)

		sys := "You are an expert BigQuery SQL assistant. You will propose high-signal queries for analysts."
		user := strings.Builder{}
		user.WriteString("Given the table schema and its known join keys, propose 3–6 useful queries.\n")
		user.WriteString("Mix of lookup/exploratory (no aggregation) and simple KPI questions.\n")
		user.WriteString("Rules:\n")
		user.WriteString("- Use **BigQuery Standard SQL**.\n")
		user.WriteString("- Always fully-qualify tables like `")
		user.WriteString(projectID)
		user.WriteString(".dataset.table` and backtick identifiers.\n")
		user.WriteString("- Prefer meaningful SELECT lists (e.g., names, types, dates) for lookups.\n")
		user.WriteString("- For KPI queries, simple aggregates are OK (e.g., COUNT rows, MIN/MAX dates).\n")
		user.WriteString("- Do **not** invent columns. Only use what’s shown.\n")
		user.WriteString("- Avoid parameters; write runnable examples (e.g., top-N, group-bys).\n\n")
		user.WriteString("TABLE SCHEMA:\n")
		user.WriteString(tableCard)
		user.WriteString("\n")
		if joinNotes != "" {
			user.WriteString("JOIN KEYS:\n")
			user.WriteString(joinNotes)
			user.WriteString("\n")
		}

		ideas, err := llm.askForQueries(ctx, sys, user.String(), 6)
		if err != nil {
			slog.Warn("llm per-table queries failed", "table", tableID, "error", err)
			continue
		}
		anchorMeasure := chooseAnchorMeasureForTable(t) // benign default for downstream title gen

		for _, q := range ideas {
			sql := strings.TrimSpace(q.SQL)
			if sql == "" {
				continue
			}
			out = append(out, QueryTemplate{
				FactTable: tableID,
				Measure:   anchorMeasure,
				DimCols:   nil,
				TimeAttr:  nil,
				Grain:     "",
				SQL:       sql,
			})
		}
	}
	return out
}

func llmGenerateJoinQueries(ctx context.Context, llm *ontologyLLM, projectID string, g *BQGraph) []QueryTemplate {
	var out []QueryTemplate

	// Build candidate pairs for which we have at least one edge (in either direction).
	type pair struct{ A, B string }
	seen := map[string]bool{}
	var pairs []pair

	for from, edges := range g.Edges {
		for _, e := range edges {
			key := pairKey(from, e.To)
			if !seen[key] {
				seen[key] = true
				pairs = append(pairs, pair{A: from, B: e.To})
			}
		}
	}

	for _, p := range pairs {
		tA := g.Nodes[p.A]
		tB := g.Nodes[p.B]
		if tA == nil || tB == nil {
			continue
		}

		sys := "You are an expert BigQuery SQL assistant. You will propose high-value join queries."
		user := strings.Builder{}
		user.WriteString("Given two joinable tables, propose 3–6 useful JOIN queries across them.\n")
		user.WriteString("Cover: lookups (show attributes from both sides) and simple KPIs (counts, sums if numeric).\n")
		user.WriteString("Rules:\n")
		user.WriteString("- Use **BigQuery Standard SQL**.\n")
		user.WriteString("- Always fully-qualify tables like `")
		user.WriteString(projectID)
		user.WriteString(".dataset.table` and backtick identifiers.\n")
		user.WriteString("- Use only provided join keys; do **not** guess joins.\n")
		user.WriteString("- Avoid parameters; write runnable examples (e.g., top-N).\n")
		user.WriteString("- Do **not** invent columns. Only use what’s shown.\n\n")

		user.WriteString("TABLE A:\n")
		user.WriteString(schemaCard(projectID, p.A, tA))
		user.WriteString("\nTABLE B:\n")
		user.WriteString(schemaCard(projectID, p.B, tB))
		user.WriteString("\nJOIN KEYS (A ↔ B):\n")
		user.WriteString(joinKeyPairs(projectID, p.A, p.B, g))
		user.WriteString("\n")

		ideas, err := llm.askForQueries(ctx, sys, user.String(), 6)
		if err != nil {
			slog.Warn("llm join queries failed", "pair", p, "error", err)
			continue
		}

		// Pick a benign measure from A if possible; else from B; else COUNT(*)
		anchor := chooseAnchorMeasureForTable(tA)
		if anchor.Column == "" {
			anchor = chooseAnchorMeasureForTable(tB)
		}
		if anchor.Column == "" {
			anchor = Measure{TableID: p.A, Column: "id", BQType: "INT64", Semantic: "count", Aggs: []string{"COUNT"}}
		}

		for _, q := range ideas {
			sql := strings.TrimSpace(q.SQL)
			if sql == "" {
				continue
			}
			out = append(out, QueryTemplate{
				FactTable: p.A, // anchor on A; SQL stands on its own
				Measure:   anchor,
				DimCols:   nil,
				TimeAttr:  nil,
				Grain:     "",
				SQL:       sql,
			})
		}
	}
	return out
}

// ---------- Heuristics: detect measures, dims, time ----------

func detectMeasures(t *Table) []Measure {
	var out []Measure
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
			// keep conservative; widen over time
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
	var attrs []DimensionAttr
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
	var out []TimeAttr
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

func findReachableDims(g *BQGraph, o *Ontology, factID string) []DimRef {
	type node struct {
		ID   string
		Path []EdgeRef
	}
	queue := []node{{ID: factID, Path: nil}}
	seen := map[string]bool{factID: true}
	var refs []DimRef

	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]

		// out-edges
		for _, e := range g.Edges[cur.ID] {
			er := EdgeRef{From: e.From, To: e.To, ColPairs: e.Cols, Kind: e.Kind, Note: e.Note}
			next := e.To
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
		// reverse edges
		for _, outs := range g.Edges {
			for _, e := range outs {
				if e.To == cur.ID {
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
	sort.Slice(out, func(i, j int) bool {
		if out[i].TableID == out[j].TableID {
			return out[i].Attr.Column < out[j].Attr.Column
		}
		return out[i].TableID < out[j].TableID
	})
	return out
}

// ---------- Template selection helpers (unchanged) ----------

func selectTopDims(dims []DimRef, max int) []DimRef {
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
	if min == 0 {
		out = append(out, nil)
	}
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

// ---------- SQL rendering for classic measure templates (unchanged) ----------

func renderTemplateSQL(projectID, factID string, m Measure, dims []DimRef, tattr *TimeAttr, grain string, g *BQGraph) string {
	aliases := map[string]string{}
	var order []string
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

	factAlias := useAlias(factID)
	var joins []string

	for _, d := range dims {
		prevAlias := factAlias
		for _, e := range d.JoinPath {
			next := e.To
			nextAlias := useAlias(next)

			// build join condition referencing prevAlias and nextAlias
			conditions := make([]string, 0, len(e.ColPairs))
			for _, p := range e.ColPairs {
				conditions = append(conditions,
					fmt.Sprintf("%s.%s = %s.%s", prevAlias, bqIdent(p[0]), nextAlias, bqIdent(p[1])),
				)
			}
			join := fmt.Sprintf("LEFT JOIN `%s.%s` %s ON %s",
				projectID, next, nextAlias, strings.Join(conditions, " AND "),
			)
			joins = append(joins, join)

			// advance the left side for multi-hop paths
			prevAlias = nextAlias
		}
	}

	var selects []string
	var groupBys []string

	if tattr != nil && grain != "" {
		tf := timeTruncExpr(fmt.Sprintf("%s.%s", factAlias, bqIdent(tattr.Column)), tattr.BQType, grain)
		selects = append(selects, fmt.Sprintf("%s AS period", tf))
		groupBys = append(groupBys, "period")
	}

	for _, d := range dims {
		a := aliases[d.TableID]
		col := fmt.Sprintf("%s.%s", a, bqIdent(d.Attr.Column))
		selects = append(selects, fmt.Sprintf("%s AS %s", col, safeAlias(d.TableID+"_"+d.Attr.Column)))
		groupBys = append(groupBys, fmt.Sprintf("%d", len(selects)))
	}

	agg := "SUM"
	if len(m.Aggs) > 0 {
		agg = m.Aggs[0]
	}
	measureExpr := aggregateExpr(fmt.Sprintf("%s.%s", factAlias, bqIdent(m.Column)), agg)
	selects = append(selects, fmt.Sprintf("%s AS %s_%s", measureExpr, strings.ToLower(agg), safeAlias(m.Column)))

	from := fmt.Sprintf("FROM `%s.%s` %s", projectID, factID, factAlias)

	where := ""
	if tattr != nil {
		col := fmt.Sprintf("%s.%s", factAlias, bqIdent(tattr.Column))
		where = fmt.Sprintf("WHERE %s BETWEEN @start_time AND @end_time", col)
	}

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
		switch gr {
		case "WEEK":
			return fmt.Sprintf("DATE_TRUNC(%s, WEEK)", qualifiedCol)
		case "MONTH":
			return fmt.Sprintf("DATE_TRUNC(%s, MONTH)", qualifiedCol)
		default:
			return fmt.Sprintf("DATE_TRUNC(%s, DAY)", qualifiedCol)
		}
	}
	switch gr {
	case "HOUR":
		return fmt.Sprintf("TIMESTAMP_TRUNC(%s, HOUR)", qualifiedCol)
	case "WEEK":
		return fmt.Sprintf("TIMESTAMP_TRUNC(%s, WEEK)", qualifiedCol)
	case "MONTH":
		return fmt.Sprintf("TIMESTAMP_TRUNC(%s, MONTH)", qualifiedCol)
	default:
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
	case strings.Contains(l, "revenue"), strings.Contains(l, "amount"), strings.Contains(l, "price"), strings.Contains(l, "cost"), strings.Contains(l, "total"), strings.Contains(l, "value"):
		return "amount"
	case strings.Contains(l, "count"), strings.HasPrefix(l, "n_"), strings.HasSuffix(l, "_count"):
		return "count"
	case strings.Contains(l, "duration"), strings.Contains(l, "latency"), strings.HasSuffix(l, "_ms"), strings.HasSuffix(l, "_sec"), strings.HasSuffix(l, "_s"):
		return "latency"
	default:
		return ""
	}
}

func allowedAggs(semantic, upType string) []string {
	switch semantic {
	case "amount":
		return []string{"SUM"}
	case "count":
		return []string{"SUM", "COUNT"}
	case "latency":
		return []string{"P95", "P50", "AVG"}
	default:
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
	case strings.Contains(l, "category") || strings.HasSuffix(l, "_type") || strings.HasSuffix(l, "_class"):
		return "category"
	case strings.Contains(l, "status") || strings.HasSuffix(l, "_state"):
		return "status"
	case l == "id" || strings.HasSuffix(l, "_id") || strings.Contains(l, "key"):
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

// ---------- Utility: schema/joins → prompt text ----------

func schemaCard(projectID, tableID string, t *Table) string {
	var cols []string
	for c, typ := range t.Columns {
		cols = append(cols, fmt.Sprintf("%s %s", c, strings.ToUpper(typ)))
	}
	sort.Strings(cols)
	return fmt.Sprintf("- %s: columns=[%s]", tableID, strings.Join(cols, ", "))
}

func joinKeyNotes(projectID, tableID string, g *BQGraph) string {
	var lines []string
	// outgoing
	for _, e := range g.Edges[tableID] {
		for _, p := range e.Cols {
			lines = append(lines, fmt.Sprintf("%s.%s = %s.%s", tableID, p[0], e.To, p[1]))
		}
	}
	// incoming
	for from, edges := range g.Edges {
		for _, e := range edges {
			if e.To == tableID {
				for _, p := range e.Cols {
					lines = append(lines, fmt.Sprintf("%s.%s = %s.%s", from, p[0], tableID, p[1]))
				}
			}
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func joinKeyPairs(projectID, a, b string, g *BQGraph) string {
	var lines []string
	// a -> b
	for _, e := range g.Edges[a] {
		if e.To != b {
			continue
		}
		for _, p := range e.Cols {
			lines = append(lines, fmt.Sprintf("%s.%s = %s.%s", a, p[0], b, p[1]))
		}
	}
	// b -> a
	for _, e := range g.Edges[b] {
		if e.To != a {
			continue
		}
		for _, p := range e.Cols {
			lines = append(lines, fmt.Sprintf("%s.%s = %s.%s", b, p[0], a, p[1]))
		}
	}
	if len(lines) == 0 {
		return "(none)"
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func chooseAnchorMeasureForTable(t *Table) Measure {
	meas := detectMeasures(t)
	if len(meas) > 0 {
		return meas[0]
	}
	// benign default to keep downstream title-gen happy
	// prefer an id-like column if present
	var idCol string
	for c := range t.Columns {
		if maybeIdentifier(c) {
			idCol = c
			break
		}
	}
	if idCol == "" {
		// just pick a column to COUNT
		for c := range t.Columns {
			idCol = c
			break
		}
	}
	return Measure{TableID: t.ID, Column: idCol, BQType: "INT64", Semantic: "count", Aggs: []string{"COUNT"}}
}

func pairKey(a, b string) string {
	if a < b {
		return a + "||" + b
	}
	return b + "||" + a
}

func dedupeTemplates(in []QueryTemplate) []QueryTemplate {
	seen := map[string]bool{}
	var out []QueryTemplate
	for _, t := range in {
		key := strings.TrimSpace(strings.ToLower(t.SQL))
		if key == "" {
			continue
		}
		if !seen[key] {
			seen[key] = true
			out = append(out, t)
		}
	}
	return out
}

// ---------- Optional: preview helper ----------

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
		agg := ""
		if len(t.Measure.Aggs) > 0 {
			agg = strings.ToLower(t.Measure.Aggs[0])
		}
		lines = append(lines, fmt.Sprintf("[%s] %s(%s) by [%s] %s",
			t.FactTable, agg, t.Measure.Column, strings.Join(dnames, ", "), timeStr))
	}
	return strings.Join(lines, "\n")
}
