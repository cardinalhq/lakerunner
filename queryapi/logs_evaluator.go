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

type TrigramQuery struct {
	Op        index.QueryOp
	Trigram   []string
	Sub       []*TrigramQuery
	fieldName string
}

type SegmentLookupFunc func(context.Context, lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error)

const bodyField = "_cardinalhq.message"

func (q *QuerierService) lookupLogsSegments(
	ctx context.Context,
	dih DateIntHours,
	leaf logql.LogLeaf,
	startTs, endTs int64,
	stepDuration time.Duration,
	orgUUID uuid.UUID,
	lookupFunc SegmentLookupFunc,
) ([]SegmentInfo, error) {
	query := &TrigramQuery{
		Op: index.QAll,
	}

	fpsToFetch := make(map[int64]struct{})

	// Process leaf Matchers
	for _, lm := range leaf.Matchers {
		label := lm.Label
		val := lm.Value

		if !slices.Contains(buffet.DimensionsToIndex, label) {
			fpsToFetch[buffet.ComputeFingerprint(label, buffet.ExistsRegex)] = struct{}{}
			query = &TrigramQuery{
				Op:  index.QAnd,
				Sub: []*TrigramQuery{query, toExistsTrigramQuery(label)},
			}
			continue
		}

		switch lm.Op {
		case logql.MatchEq, logql.MatchRe:
			query = q.andQuery(label, val, fpsToFetch, query)

		case logql.MatchNe, logql.MatchNre:
			fpsToFetch[buffet.ComputeFingerprint(label, buffet.ExistsRegex)] = struct{}{}
			query = &TrigramQuery{
				Op:  index.QAnd,
				Sub: []*TrigramQuery{query, toExistsTrigramQuery(label)},
			}
		}
	}

	// Process Line Filters (these are always on the body/message field)
	for _, lf := range leaf.LineFilters {
		switch lf.Op {
		case logql.LineContains:
			pat := ".*" + lf.Match + ".*"
			query = q.andQuery(bodyField, pat, fpsToFetch, query)

		case logql.LineRegex:
			query = q.andQuery(bodyField, lf.Match, fpsToFetch, query)

		default:
		}
	}

	// Process Label Filters (that are not part of Parsers)
	for _, lf := range leaf.LabelFilters {
		if lf.ParserIdx != nil {
			continue
		}

		if !slices.Contains(buffet.DimensionsToIndex, lf.Label) {
			fpsToFetch[buffet.ComputeFingerprint(lf.Label, buffet.ExistsRegex)] = struct{}{}
			query = &TrigramQuery{
				Op:  index.QAnd,
				Sub: []*TrigramQuery{query, toExistsTrigramQuery(lf.Label)},
			}
			continue
		}
		switch lf.Op {
		case logql.MatchEq, logql.MatchRe:
			query = q.andQuery(lf.Label, lf.Value, fpsToFetch, query)

		case logql.MatchNe, logql.MatchNre:
			fpsToFetch[buffet.ComputeFingerprint(lf.Label, buffet.ExistsRegex)] = struct{}{}
			query = &TrigramQuery{
				Op:  index.QAnd,
				Sub: []*TrigramQuery{query, toExistsTrigramQuery(lf.Label)},
			}
		}
	}

	fpList := make([]int64, 0, len(fpsToFetch))
	for fp := range fpsToFetch {
		fpList = append(fpList, fp)
	}

	rows, err := lookupFunc(ctx, lrdb.ListLogSegmentsForQueryParams{
		OrganizationID: orgUUID,
		Dateint:        int32(dih.DateInt),
		Fingerprints:   fpList,
		S:              startTs,
		E:              endTs,
	})
	if err != nil {
		return nil, fmt.Errorf("list log segments for query: %w", err)
	}
	fpToSegments := make(map[int64][]SegmentInfo)

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
		fpToSegments[row.Fingerprint] = append(fpToSegments[row.Fingerprint], seg)
	}

	return allSegments, nil
}

func (q *QuerierService) andQuery(label string, val string, fpsToFetch map[int64]struct{}, query *TrigramQuery) *TrigramQuery {
	lt, fps := buildLabelTrigram(label, val)
	for _, fp := range fps {
		fpsToFetch[fp] = struct{}{}
	}
	tq := &TrigramQuery{
		Op:        lt.Op,
		fieldName: label,
		Trigram:   lt.Trigram,
		Sub:       make([]*TrigramQuery, 0, len(lt.Sub)),
	}
	for _, sub := range lt.Sub {
		tq.Sub = append(tq.Sub, &TrigramQuery{
			Op:        sub.Op,
			fieldName: label,
			Trigram:   sub.Trigram,
		})
	}

	query = &TrigramQuery{
		Op:  index.QAnd,
		Sub: []*TrigramQuery{query, tq},
	}
	return query
}

func toExistsTrigramQuery(label string) *TrigramQuery {
	trigramQuery := &TrigramQuery{
		Op:        index.QAnd,
		fieldName: label,
		Trigram:   make([]string, 0),
	}
	trigramQuery.Trigram = append(trigramQuery.Trigram, buffet.ExistsRegex)
	return trigramQuery
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
