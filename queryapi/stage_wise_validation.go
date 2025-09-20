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
	Name           string   // human readable, e.g. "matchers", "matchers+independent", "parser[2]: regexp"
	SQL            string   // fully-resolved SQL (placeholders replaced)
	RowCount       int      // number of rows returned
	OK             bool     // did this stage pass?
	Error          error    // query error, if any
	FieldsExpected []string // fields that this stage expected to be created/present
	MissingFields  []string // subset of FieldsExpected not present/non-empty in any row
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

	// Helper to run a leaf and count rows
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
		// Base OK depends on "any rows?"
		sc.OK = sc.RowCount > 0

		// If we have expected fields (from the most-recent parser),
		// they must be present and non-empty in at least one returned row.
		if sc.OK && len(expect) > 0 {
			missing := missingOrEmptyFields(rows, expect)
			if len(missing) > 0 {
				sc.MissingFields = missing
				sc.OK = false
			}
		}
		_ = ctx // reserved for future per-stage queries
		return sc
	}

	// --- Stage 0: matchers only ---
	stage0 := leaf
	stage0.LineFilters = nil
	stage0.LabelFilters = nil
	stage0.Parsers = nil
	out = append(out, run(stage0, "matchers", nil))

	// If stage 0 fails (no rows), stop early.
	if !out[len(out)-1].OK {
		return out, nil
	}

	// --- independent label filters: those that were NOT attached to a parser ---
	var independent []logql.LabelFilter
	for _, lf := range leaf.LabelFilters {
		if !lf.AfterParser {
			independent = append(independent, lf)
		}
	}

	// --- Stage 1: matchers + independent label filters + ALL line filters ---
	stage1 := leaf
	stage1.Parsers = nil
	stage1.LabelFilters = independent
	out = append(out, run(stage1, "matchers+independent+line_filters", nil))

	if !out[len(out)-1].OK {
		return out, nil
	}

	// --- Stage 2..: add parsers one by one; attach their label filters as the columns appear ---
	var cumulativeParsers []logql.ParserStage
	for i, p := range leaf.Parsers {
		cumulativeParsers = append(cumulativeParsers, p)

		si := leaf
		si.Parsers = append([]logql.ParserStage(nil), cumulativeParsers...)
		// label filters attached to parsers up to i
		var upto []logql.LabelFilter
		for _, lf := range leaf.LabelFilters {
			if lf.AfterParser && lf.ParserIdx != nil && *lf.ParserIdx <= i {
				upto = append(upto, lf)
			}
		}
		// plus the already-known independent ones
		upto = append(upto, independent...)
		si.LabelFilters = upto

		// fields this parser is expected to *create* at this step
		expect := createdByParser(p)

		name := fmt.Sprintf("parser[%d]: %s", i, strings.ToLower(p.Type))
		out = append(out, run(si, name, expect))

		if !out[len(out)-1].OK {
			// stop at first failure in parser chain
			return out, nil
		}
	}

	return out, nil
}

// createdByParser returns the set of columns we expect a parser to create.
func createdByParser(p logql.ParserStage) []string {
	typ := strings.ToLower(p.Type)
	switch typ {
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
		// fall back to keys of Params
		outs := make([]string, 0, len(p.Params))
		for k := range p.Params {
			outs = append(outs, k)
		}
		return dedupeSorted(outs)
	case "json":
		// Only the explicit mappings are statically knowable.
		if len(p.Params) == 0 {
			return nil
		}
		outs := make([]string, 0, len(p.Params))
		for k := range p.Params {
			outs = append(outs, k)
		}
		return dedupeSorted(outs)
	case "logfmt":
		// Same as json: we only know the explicit mappings.
		if len(p.Params) == 0 {
			return nil
		}
		outs := make([]string, 0, len(p.Params))
		for k := range p.Params {
			outs = append(outs, k)
		}
		return dedupeSorted(outs)
	case "unwrap":
		// We create a numeric value for window ops
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
