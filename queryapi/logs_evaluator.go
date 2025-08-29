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
	"fmt"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/google/uuid"
	"slices"
	"time"

	"github.com/google/codesearch/index"
	"github.com/google/codesearch/regexp"
)

func (q *QuerierService) EvaluateLogsQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs, endTs int64,
	queryPlan logql.LQueryPlan,
) (<-chan map[string]promql.EvalResult, error) {
	//workers, err := q.workerDiscovery.GetAllWorkers()
	//if err != nil {
	//	slog.Error("failed to get all workers", "err", err)
	//	return nil, fmt.Errorf("failed to get all workers: %w", err)
	//}
	//
	//out := make(chan map[string]promql.EvalResult, 1024)
	//
	//go func() {
	//	defer close(out)
	//
	//	for _, leaf := range queryPlan.Leaves {
	//
	//	}
	//}
	return nil, nil
}

type TQFp struct {
	tq           *index.Query
	fingerprints []int64
}

type SegmentLookupFunc func(context.Context, lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error)

func (q *QuerierService) lookupLogsSegments(
	ctx context.Context,
	dih DateIntHours,
	leaf logql.LogLeaf,
	startTs, endTs int64,
	stepDuration time.Duration,
	orgUUID uuid.UUID,
	lookupFunc SegmentLookupFunc,
) ([]SegmentInfo, error) {

	var allSegments []SegmentInfo

	coarseFPs := make(map[int64]struct{})
	var acc *TQFp

	// Process leaf Matchers
	for _, lm := range leaf.Matchers {
		label := lm.Label
		val := lm.Value

		if !slices.Contains(buffet.DimensionsToIndex, label) {
			fp := buffet.ComputeFingerprint(label, buffet.ExistsRegex)
			coarseFPs[fp] = struct{}{}
			continue
		}

		switch lm.Op {
		case logql.MatchEq, logql.MatchRe:
			tq, fps := buildLabelTrigram(label, val)
			if len(fps) == 0 && tq == nil {
				coarseFPs[buffet.ComputeFingerprint(label, buffet.ExistsRegex)] = struct{}{}
				continue
			}
			acc = andAccumulate(acc, tq, fps)

		case logql.MatchNe, logql.MatchNre:
			coarseFPs[buffet.ComputeFingerprint(label, buffet.ExistsRegex)] = struct{}{}
		}
	}

	const bodyField = "_cardinalhq.message"

	// Process Line Filters
	for _, lf := range leaf.LineFilters {
		switch lf.Op {
		case logql.LineContains:
			pat := ".*" + lf.Match + ".*"
			tq, fps := buildLabelTrigram(bodyField, pat)
			acc = andAccumulate(acc, tq, fps)

		case logql.LineRegex:
			tq, fps := buildLabelTrigram(bodyField, lf.Match)
			acc = andAccumulate(acc, tq, fps)

		case logql.LineNotContains, logql.LineNotRegex:
			coarseFPs[buffet.ComputeFingerprint(bodyField, buffet.ExistsRegex)] = struct{}{}
		}
	}

	// Process Label Filters (that are not part of Parsers)
	for _, lf := range leaf.LabelFilters {
		if lf.ParserIdx != nil {
			continue
		}
		switch lf.Op {
		case logql.MatchEq, logql.MatchRe:
			tq, fps := buildLabelTrigram(lf.Label, lf.Value)
			if len(fps) == 0 && tq == nil {
				coarseFPs[buffet.ComputeFingerprint(lf.Label, buffet.ExistsRegex)] = struct{}{}
				continue
			}
			acc = andAccumulate(acc, tq, fps)

		case logql.MatchNe, logql.MatchNre:
			coarseFPs[buffet.ComputeFingerprint(lf.Label, buffet.ExistsRegex)] = struct{}{}
		}
	}

	fpSet := make(map[int64]struct{})
	for fp := range coarseFPs {
		fpSet[fp] = struct{}{}
	}
	if acc != nil {
		for _, fp := range acc.fingerprints {
			fpSet[fp] = struct{}{}
		}
	}
	if len(fpSet) == 0 {
		return nil, nil
	}

	fpList := make([]int64, 0, len(fpSet))
	for fp := range fpSet {
		fpList = append(fpList, fp)
	}
	slices.Sort(fpList)

	rows, err := lookupFunc(ctx, lrdb.ListLogSegmentsForQueryParams{
		OrganizationID: orgUUID,
		Dateint:        int32(dih.DateInt),
		Fingerprints:   fpList,
		S:              startTs,
		E:              endTs,
	})
	if err != nil {
		return nil, fmt.Errorf("ListLogSegmentsForQuery: %w", err)
	}

	fpToSegments := make(map[int64][]SegmentInfo)
	segUnique := make(map[string]SegmentInfo)

	for _, row := range rows {
		endHour := zeroFilledHour(time.UnixMilli(row.EndTs).UTC().Hour())
		seg := SegmentInfo{
			DateInt:        dih.DateInt,
			Hour:           endHour,
			SegmentID:      row.SegmentID,
			StartTs:        row.StartTs,
			EndTs:          row.EndTs,
			Dataset:        "logs",
			OrganizationID: orgUUID,
			InstanceNum:    row.InstanceNum,
			Frequency:      stepDuration.Milliseconds(),
		}
		allSegments = append(allSegments)
		key := fmt.Sprintf("%d:%d:%d", row.StartTs, row.EndTs, seg.SegmentID)
		segUnique[key] = seg

		fpToSegments[row.Fingerprint] = append(fpToSegments[row.Fingerprint], seg)
	}

	if acc == nil || acc.tq == nil {
		allSegments = make([]SegmentInfo, 0, len(segUnique))
		for _, s := range segUnique {
			allSegments = append(allSegments, s)
		}
		return allSegments, nil
	}

	finalSet := resolveByTrigram(acc.tq, fpToSegments)
	allSegments = make([]SegmentInfo, 0, len(finalSet))
	for s := range finalSet {
		allSegments = append(allSegments, s)
	}
	return allSegments, nil
}

// buildLabelTrigram compiles a regex pattern to a trigram query and returns
// the query node plus the computed fingerprints for that label’s trigrams.
func buildLabelTrigram(label, pattern string) (*index.Query, []int64) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return nil, nil
	}
	tq := index.RegexpQuery(re.Syntax)

	var fps []int64
	if len(tq.Trigram) > 0 {
		fps = make([]int64, 0, len(tq.Trigram))
		for _, tri := range tq.Trigram {
			fps = append(fps, buffet.ComputeFingerprint(label, tri))
		}
		return tq, dedupeInt64(fps)
	}

	// If it’s a degenerate (match-all) query and there’s no Sub, no narrowing
	if len(tq.Sub) == 0 {
		return tq, nil
	}

	// Recursively collect fingerprints from children where possible
	// (for OR/AND we’ll combine at the planner level; here we only emit leaf fps)
	var collect func(q *index.Query)
	collect = func(q *index.Query) {
		if q == nil {
			return
		}
		if len(q.Trigram) > 0 {
			for _, tri := range q.Trigram {
				fps = append(fps, buffet.ComputeFingerprint(label, tri))
			}
		}
		for _, ch := range q.Sub {
			collect(ch)
		}
	}
	collect(tq)
	return tq, dedupeInt64(fps)
}

// andAccumulate merges a new (tq,fps) into the running accumulator with AND semantics.
func andAccumulate(acc *TQFp, tq *index.Query, fps []int64) *TQFp {
	if tq == nil && len(fps) == 0 {
		return acc
	}
	if acc == nil {
		return &TQFp{tq: tq, fingerprints: dedupeInt64(fps)}
	}
	// Intersect fingerprints
	acc.fingerprints = intersectInt64(acc.fingerprints, fps)

	// Compose query: AND(existing, incoming)
	if acc.tq == nil {
		acc.tq = tq
		return acc
	}
	if tq == nil {
		return acc
	}
	acc.tq = &index.Query{
		Op:  index.QAnd,
		Sub: []*index.Query{acc.tq, tq},
	}
	return acc
}

func dedupeInt64(in []int64) []int64 {
	if len(in) <= 1 {
		// nil or single already unique
		return in
	}
	m := make(map[int64]struct{}, len(in))
	for _, v := range in {
		m[v] = struct{}{}
	}
	out := make([]int64, 0, len(m))
	for v := range m {
		out = append(out, v)
	}
	slices.Sort(out)
	return out
}

func intersectInt64(a, b []int64) []int64 {
	if len(a) == 0 || len(b) == 0 {
		return nil
	}
	// Use the smaller as the set
	if len(b) < len(a) {
		a, b = b, a
	}
	set := make(map[int64]struct{}, len(a))
	for _, v := range a {
		set[v] = struct{}{}
	}
	var out []int64
	for _, v := range b {
		if _, ok := set[v]; ok {
			out = append(out, v)
		}
	}
	return dedupeInt64(out)
}

// resolveByTrigram applies the trigram query tree to fp->segments and returns the final set.
func resolveByTrigram(qry *index.Query, fpToSegs map[int64][]SegmentInfo) map[SegmentInfo]struct{} {
	if qry == nil {
		return flattenAll(fpToSegs)
	}

	switch qry.Op {
	case index.QAll:
		return flattenAll(fpToSegs)
	case index.QNone:
		return map[SegmentInfo]struct{}{}
	case index.QAnd:
		if len(qry.Sub) == 0 {
			// AND leaf: intersect all leaf trigrams present
			return intersectSetsByTrigrams(qry.Trigram, fpToSegs)
		}
		sets := make([]map[SegmentInfo]struct{}, 0, len(qry.Sub))
		for _, ch := range qry.Sub {
			sets = append(sets, resolveByTrigram(ch, fpToSegs))
		}
		return intersectSets(sets...)
	case index.QOr:
		// OR over children
		if len(qry.Sub) == 0 {
			return unionSetsByTrigrams(qry.Trigram, fpToSegs)
		}
		out := make(map[SegmentInfo]struct{})
		for _, ch := range qry.Sub {
			for s := range resolveByTrigram(ch, fpToSegs) {
				out[s] = struct{}{}
			}
		}
		return out
	default:
		// Unknown op – fall back to all
		return flattenAll(fpToSegs)
	}
}

func flattenAll(fpToSegs map[int64][]SegmentInfo) map[SegmentInfo]struct{} {
	out := make(map[SegmentInfo]struct{})
	for _, segs := range fpToSegs {
		for _, s := range segs {
			out[s] = struct{}{}
		}
	}
	return out
}

func intersectSets(sets ...map[SegmentInfo]struct{}) map[SegmentInfo]struct{} {
	if len(sets) == 0 {
		return map[SegmentInfo]struct{}{}
	}
	// Start from the smallest
	smallIdx := 0
	for i := range sets {
		if len(sets[i]) < len(sets[smallIdx]) {
			smallIdx = i
		}
	}
	base := sets[smallIdx]
	out := make(map[SegmentInfo]struct{})
	for s := range base {
		ok := true
		for i := range sets {
			if i == smallIdx {
				continue
			}
			if _, has := sets[i][s]; !has {
				ok = false
				break
			}
		}
		if ok {
			out[s] = struct{}{}
		}
	}
	return out
}

func intersectSetsByTrigrams(tris []string, fpToSegs map[int64][]SegmentInfo) map[SegmentInfo]struct{} {
	if len(tris) == 0 {
		return flattenAll(fpToSegs)
	}
	sets := make([]map[SegmentInfo]struct{}, 0, len(tris))
	for _, tri := range tris {
		fp := buffet.ComputeFingerprint("message", tri) // field here doesn’t matter for set algebra; pick body
		segs := fpToSegs[fp]
		set := make(map[SegmentInfo]struct{}, len(segs))
		for _, s := range segs {
			set[s] = struct{}{}
		}
		sets = append(sets, set)
	}
	return intersectSets(sets...)
}

func unionSetsByTrigrams(tris []string, fpToSegs map[int64][]SegmentInfo) map[SegmentInfo]struct{} {
	out := make(map[SegmentInfo]struct{})
	for _, tri := range tris {
		fp := buffet.ComputeFingerprint("message", tri)
		for _, s := range fpToSegs[fp] {
			out[s] = struct{}{}
		}
	}
	return out
}
