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

package queryapi

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/cardinalhq/lakerunner/logql"
)

// StageCheck summarizes one validation step.
type StageCheck struct {
	Name           string
	SQL            string
	RowCount       int
	OK             bool
	Error          error
	FieldsExpected []string
	MissingFields  []string
}

func stageWiseValidation(
	db *sql.DB,
	table string,
	leaf logql.LogLeaf,
	startMillis, endMillis int64,
	limit int,
	order string,
	extraOrderBy []string,
) ([]StageCheck, error) {

	var out []StageCheck
	ctx := context.Background()

	// If there are parsers and *every* line filter has ParserIdx == nil,
	// assume they belong after the last parser (so they see the rewritten body).
	//leaf = fallbackAnnotateLineFilters(leaf)

	run := func(lf logql.LogLeaf, name string, expect []string) StageCheck {
		sc := StageCheck{Name: name, FieldsExpected: append([]string(nil), expect...)}
		sqlText := lf.ToWorkerSQL(limit, order, extraOrderBy)
		sqlText = replacePlaceholders(sqlText, table, startMillis, endMillis)
		sc.SQL = sqlText

		rows, err := queryAllRows(db, sqlText)
		if err != nil {
			sc.Error = fmt.Errorf("execute: %w", err)
			sc.OK = false
			return sc
		}
		sc.RowCount = len(rows)
		sc.OK = sc.RowCount > 0

		if len(expect) > 0 {
			missing := missingOrEmptyFields(rows, expect)
			if len(missing) > 0 {
				sc.MissingFields = missing
				sc.OK = false
			}
		}
		_ = ctx
		return sc
	}

	// Stage 0: matchers only
	stage0 := leaf
	stage0.LineFilters = nil
	stage0.LabelFilters = nil
	stage0.Parsers = nil
	out = append(out, run(stage0, "matchers", nil))
	if !out[len(out)-1].OK {
		// If the raw selector doesn't return rows, nothing else will.
		return out, nil
	}

	// Independent label filters (ParserIdx == nil)
	var independent []logql.LabelFilter
	for _, lf := range leaf.LabelFilters {
		if lf.ParserIdx == nil {
			independent = append(independent, lf)
		}
	}

	// Stage 1: matchers + independent label filters + PRE line filters (ParserIdx == nil)
	stage1 := leaf
	stage1.Parsers = nil
	stage1.LabelFilters = independent
	stage1.LineFilters = lineFiltersUpTo(leaf.LineFilters, -1) // only pre-parser line filters
	out = append(out, run(stage1, "matchers+independent+pre_line_filters", nil))

	// IMPORTANT: do *not* early-return after Stage 1; we want to see parser stages even if Stage 1 had 0 rows.

	// Stage 2..: cumulative parsers; include label/line filters whose ParserIdx <= i
	var cumulative []logql.ParserStage
	for i, p := range leaf.Parsers {
		cumulative = append(cumulative, p)

		si := leaf
		si.Parsers = append([]logql.ParserStage(nil), cumulative...)

		// label filters up to parser i (ParserIdx <= i) + independent ones
		var upto []logql.LabelFilter
		for _, lf := range leaf.LabelFilters {
			if lf.ParserIdx != nil && *lf.ParserIdx <= i {
				upto = append(upto, lf)
			}
		}
		si.LabelFilters = append(upto, independent...)

		// line filters up to parser i (pre were applied in Stage 1; here we add those with ParserIdx <= i)
		si.LineFilters = lineFiltersUpTo(leaf.LineFilters, i)

		expect := createdByParser(p)
		name := fmt.Sprintf("parser[%d]: %s", i, strings.ToLower(p.Type))
		out = append(out, run(si, name, expect))

		// Stop at the first failing parser stage; later ones can't succeed.
		if !out[len(out)-1].OK {
			return out, nil
		}
	}

	return out, nil
}

// lineFiltersUpTo returns pre-parser filters when upto < 0,
// and any filters attached to a parser index <= upto otherwise.
func lineFiltersUpTo(all []logql.LineFilter, upto int) []logql.LineFilter {
	if len(all) == 0 {
		return nil
	}
	var out []logql.LineFilter
	for _, lf := range all {
		if lf.ParserIdx == nil {
			if upto < 0 {
				out = append(out, lf)
			}
			continue
		}
		if *lf.ParserIdx <= upto {
			out = append(out, lf)
		}
	}
	return out
}

// createdByParser returns the set of columns we expect a parser to create.
func createdByParser(p logql.ParserStage) []string {
	switch strings.ToLower(p.Type) {
	case "line_format", "line-format", "lineformat":
		return []string{"_cardinalhq.message"}
	case "regexp":
		pat := p.Params["pattern"]
		names := captureNamesFromRegexp(pat)
		dst := names[:0]
		for _, n := range names {
			if strings.HasPrefix(n, "__var_") {
				continue
			}
			dst = append(dst, n)
		}
		return dedupeSorted(dst)
	case "label_format", "label-format", "labelformat":
		if len(p.LabelFormats) > 0 {
			outs := make([]string, 0, len(p.LabelFormats))
			for _, lf := range p.LabelFormats {
				outs = append(outs, lf.Out)
			}
			return dedupeSorted(outs)
		}
		outs := make([]string, 0, len(p.Params))
		for k := range p.Params {
			outs = append(outs, k)
		}
		return dedupeSorted(outs)
	case "json":
		if len(p.Params) == 0 {
			return nil
		}
		outs := make([]string, 0, len(p.Params))
		for k := range p.Params {
			outs = append(outs, k)
		}
		return dedupeSorted(outs)
	case "logfmt":
		if len(p.Params) == 0 {
			return nil
		}
		outs := make([]string, 0, len(p.Params))
		for k := range p.Params {
			outs = append(outs, k)
		}
		return dedupeSorted(outs)
	case "unwrap":
		return []string{"__unwrap_value"}
	default:
		return nil
	}
}

var reNamedGroup = regexp.MustCompile(`\(\?P<([A-Za-z_][A-Za-z0-9_]*)>`)

func captureNamesFromRegexp(pat string) []string {
	if pat == "" {
		return nil
	}
	m := reNamedGroup.FindAllStringSubmatch(pat, -1)
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	seen := map[string]struct{}{}
	for _, g := range m {
		if len(g) < 2 {
			continue
		}
		k := g[1]
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
}

func dedupeSorted(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	for _, s := range in {
		if s == "" {
			continue
		}
		seen[s] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func missingOrEmptyFields(rows []rowstruct, expect []string) []string {
	if len(expect) == 0 {
		return nil
	}
	var missing []string
	for _, col := range expect {
		foundNonEmpty := false
		for _, r := range rows {
			v, ok := r[col]
			if !ok || v == nil {
				continue
			}
			if isEffectivelyEmpty(v) {
				continue
			}
			foundNonEmpty = true
			break
		}
		if !foundNonEmpty {
			missing = append(missing, col)
		}
	}
	return missing
}

func isEffectivelyEmpty(v any) bool {
	switch x := v.(type) {
	case string:
		return strings.TrimSpace(x) == ""
	case []byte:
		return len(x) == 0
	default:
		return false
	}
}
