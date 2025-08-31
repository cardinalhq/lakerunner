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
	"encoding/json"
	"fmt"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"
	"github.com/google/uuid"
	"log/slog"
	"slices"
	"time"

	"github.com/google/codesearch/index"
	"github.com/google/codesearch/regexp"
)

const (
	DefaultLogStep = 10 * time.Second
)

func (q *QuerierService) EvaluateLogsQuery(
	ctx context.Context,
	orgID uuid.UUID,
	startTs, endTs int64,
	reverse bool,
	limit int,
	queryPlan logql.LQueryPlan,
) (<-chan promql.Timestamped, error) {
	workers, err := q.workerDiscovery.GetAllWorkers()
	if err != nil {
		slog.Error("failed to get all workers", "err", err)
		return nil, fmt.Errorf("failed to get all workers: %w", err)
	}
	stepDuration := StepForQueryDuration(startTs, endTs)

	out := make(chan promql.Timestamped, 1024)

	go func() {
		defer close(out)

		// Partition by dateInt hours for storage listing.
		dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC)

		for _, leaf := range queryPlan.Leaves {
			for _, dih := range dateIntHours {
				segments, err := q.lookupLogsSegments(ctx, dih, leaf, startTs, endTs, DefaultLogStep, orgID, q.mdb.ListLogSegmentsForQuery)
				slog.Info("lookupLogsSegments", "dih", dih, "leaf", leaf, "found", len(segments))
				if err != nil {
					slog.Error("failed to lookup log segments", "err", err, "dih", dih, "leaf", leaf)
					return
				}
				if len(segments) == 0 {
					continue
				}
				// Form time-contiguous batches sized for the number of workers.
				groups := ComputeReplayBatchesWithWorkers(segments, DefaultLogStep, startTs, endTs, len(workers), true)
				for _, group := range groups {
					select {
					case <-ctx.Done():
						return
					default:
					}

					slog.Info("Pushing down segments", "groupSize", len(group.Segments))

					// Collect all segment IDs for worker assignment
					segmentIDs := make([]int64, 0, len(group.Segments))
					segmentMap := make(map[int64][]SegmentInfo)
					for _, segment := range group.Segments {
						segmentIDs = append(segmentIDs, segment.SegmentID)
						segmentMap[segment.SegmentID] = append(segmentMap[segment.SegmentID], segment)
					}

					// Get worker assignments for all segments
					mappings, err := q.workerDiscovery.GetWorkersForSegments(orgID, segmentIDs)
					if err != nil {
						slog.Error("failed to get worker assignments", "err", err)
						continue
					}

					// Group segments by assigned worker
					workerGroups := make(map[Worker][]SegmentInfo)
					for _, mapping := range mappings {
						segmentList := segmentMap[mapping.SegmentID]
						workerGroups[mapping.Worker] = append(workerGroups[mapping.Worker], segmentList...)
					}

					var groupLeafChans []<-chan promql.Timestamped
					for worker, workerSegments := range workerGroups {
						req := PushDownRequest{
							OrganizationID: orgID,
							LogLeaf:        &leaf,
							StartTs:        group.StartTs,
							EndTs:          group.EndTs,
							Segments:       workerSegments,
							Step:           stepDuration,
							Limit:          limit,
							Reverse:        reverse,
						}
						ch, err := q.logsPushDown(ctx, worker, req)
						if err != nil {
							slog.Error("pushdown failed", "worker", worker, "err", err)
							continue
						}
						groupLeafChans = append(groupLeafChans, ch)
					}
					// No channels for this group — skip.
					if len(groupLeafChans) == 0 {
						continue
					}

					// Merge this group's worker streams by timestamp
					mergedGroup := promql.MergeSorted(ctx, 1024, reverse, limit, groupLeafChans...)

					// Forward group results into final output stream as they arrive.
					for {
						select {
						case <-ctx.Done():
							return
						case res, ok := <-mergedGroup:
							if !ok {
								// Group done, proceed to next group.
								goto nextGroup
							}
							out <- res
						}
					}
				nextGroup:
				}
			}
		}
	}()
	return out, nil
}

func (q *QuerierService) logsPushDown(
	ctx context.Context,
	worker Worker,
	request PushDownRequest,
) (<-chan promql.Timestamped, error) {
	return PushDownStream[promql.Timestamped](ctx, worker, request,
		func(typ string, data json.RawMessage) (promql.Timestamped, bool, error) {
			var zero promql.Timestamped
			if typ != "result" {
				return zero, false, nil
			}
			var si promql.Exemplar
			if err := json.Unmarshal(data, &si); err != nil {
				return zero, false, err
			}
			return si, true, nil
		})
}

type SegmentLookupFunc func(context.Context, lrdb.ListLogSegmentsForQueryParams) ([]lrdb.ListLogSegmentsForQueryRow, error)

const bodyField = "_cardinalhq.message"

type TrigramQuery struct {
	Op        index.QueryOp
	Trigram   []string
	Sub       []*TrigramQuery
	fieldName string
}

func (q *QuerierService) lookupLogsSegments(
	ctx context.Context,
	dih DateIntHours,
	leaf logql.LogLeaf,
	startTs, endTs int64,
	stepDuration time.Duration,
	orgUUID uuid.UUID,
	lookupFunc SegmentLookupFunc,
) ([]SegmentInfo, error) {
	root := &TrigramQuery{Op: index.QAll}
	fpsToFetch := make(map[int64]struct{})

	for _, lm := range leaf.Matchers {
		label, val := lm.Label, lm.Value
		if !slices.Contains(buffet.DimensionsToIndex, label) {
			addExistsNode(label, fpsToFetch, &root)
			continue
		}
		switch lm.Op {
		case logql.MatchEq, logql.MatchRe:
			addAndNodeFromPattern(label, val, fpsToFetch, &root)
		default:
			addExistsNode(label, fpsToFetch, &root)
		}
	}

	for _, lf := range leaf.LineFilters {
		switch lf.Op {
		case logql.LineContains:
			addAndNodeFromPattern(bodyField, ".*"+lf.Match+".*", fpsToFetch, &root)
		case logql.LineRegex:
			addAndNodeFromPattern(bodyField, lf.Match, fpsToFetch, &root)
		default:
			addExistsNode(bodyField, fpsToFetch, &root)
		}
	}

	for _, lf := range leaf.LabelFilters {
		if lf.ParserIdx != nil {
			continue
		}
		label, val := lf.Label, lf.Value
		if !slices.Contains(buffet.DimensionsToIndex, label) {
			addExistsNode(label, fpsToFetch, &root)
			continue
		}
		switch lf.Op {
		case logql.MatchEq, logql.MatchRe:
			addAndNodeFromPattern(label, val, fpsToFetch, &root)
		default:
			addExistsNode(label, fpsToFetch, &root)
		}
	}

	slog.Info("lookupLogsSegments", "dih", dih, "startTs", startTs, "endTs", endTs, "fps", len(fpsToFetch))
	if len(fpsToFetch) == 0 {
		addExistsNode(bodyField, fpsToFetch, &root)
	}

	// 2) Fetch candidate segments for the UNION of all fingerprints.
	fpList := make([]int64, 0, len(fpsToFetch))
	for fp := range fpsToFetch {
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
		return nil, fmt.Errorf("list log segments for query: %w", err)
	}

	if len(rows) == 0 {
		slog.Info("lookupLogsSegments: no segments found", "dih", dih, "startTs", startTs, "endTs", endTs, slog.Any("fps", fpsToFetch))
	}

	fpToSegments := make(map[int64][]SegmentInfo, len(rows))
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

	finalSet := computeSegmentSet(root, fpToSegments)

	out := make([]SegmentInfo, 0, len(finalSet))
	for s := range finalSet {
		out = append(out, s)
	}
	return out, nil
}

func addAndNodeFromPattern(label, pattern string, fps map[int64]struct{}, root **TrigramQuery) {
	lt, fpsList := buildLabelTrigram(label, pattern) // lt: *index.Query
	for _, fp := range fpsList {
		fps[fp] = struct{}{}
	}
	tq := fromIndexQuery(label, lt)
	*root = &TrigramQuery{Op: index.QAnd, Sub: []*TrigramQuery{*root, tq}}
}

func addExistsNode(label string, fps map[int64]struct{}, root **TrigramQuery) {
	fp := buffet.ComputeFingerprint(label, buffet.ExistsRegex)
	fps[fp] = struct{}{}
	tq := &TrigramQuery{
		Op:        index.QAnd,
		fieldName: label,
		Trigram:   []string{buffet.ExistsRegex},
	}
	slog.Info("Adding exists node", "label", label, "fp", fp)
	*root = &TrigramQuery{Op: index.QAnd, Sub: []*TrigramQuery{*root, tq}}
}

func fromIndexQuery(label string, iq *index.Query) *TrigramQuery {
	if iq == nil {
		return &TrigramQuery{Op: index.QAll, fieldName: label} // match all
	}
	node := &TrigramQuery{
		Op:        iq.Op,
		fieldName: label,
		Trigram:   append([]string(nil), iq.Trigram...),
		Sub:       make([]*TrigramQuery, 0, len(iq.Sub)),
	}
	for _, ch := range iq.Sub {
		node.Sub = append(node.Sub, fromIndexQuery(label, ch))
	}
	return node
}

func computeSegmentSet(q *TrigramQuery, fpToSegs map[int64][]SegmentInfo) map[SegmentInfo]struct{} {
	if q == nil {
		return flattenAll(fpToSegs)
	}
	if len(q.Sub) > 0 {
		switch q.Op {
		case index.QAll:
			return flattenAll(fpToSegs)
		case index.QNone:
			return map[SegmentInfo]struct{}{}
		case index.QAnd:
			sets := make([]map[SegmentInfo]struct{}, 0, len(q.Sub))
			for _, ch := range q.Sub {
				sets = append(sets, computeSegmentSet(ch, fpToSegs))
			}
			return intersectSets(sets...)
		case index.QOr:
			out := make(map[SegmentInfo]struct{})
			for _, ch := range q.Sub {
				for s := range computeSegmentSet(ch, fpToSegs) {
					out[s] = struct{}{}
				}
			}
			return out
		default:
			return flattenAll(fpToSegs)
		}
	}

	switch q.Op {
	case index.QAll:
		return flattenAll(fpToSegs)
	case index.QNone:
		return map[SegmentInfo]struct{}{}
	case index.QAnd:
		// Intersect sets of segments for all leaf trigrams
		if len(q.Trigram) == 0 {
			return flattenAll(fpToSegs)
		}
		sets := make([]map[SegmentInfo]struct{}, 0, len(q.Trigram))
		for _, tri := range q.Trigram {
			fp := buffet.ComputeFingerprint(q.fieldName, tri)
			set := make(map[SegmentInfo]struct{})
			for _, s := range fpToSegs[fp] {
				set[s] = struct{}{}
			}
			sets = append(sets, set)
		}
		return intersectSets(sets...)
	case index.QOr:
		out := make(map[SegmentInfo]struct{})
		for _, tri := range q.Trigram {
			fp := buffet.ComputeFingerprint(q.fieldName, tri)
			for _, s := range fpToSegs[fp] {
				out[s] = struct{}{}
			}
		}
		return out
	default:
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
	switch len(sets) {
	case 0:
		return map[SegmentInfo]struct{}{}
	case 1:
		// clone
		out := make(map[SegmentInfo]struct{}, len(sets[0]))
		for s := range sets[0] {
			out[s] = struct{}{}
		}
		return out
	}
	// start from smallest
	minIdx := 0
	for i := 1; i < len(sets); i++ {
		if len(sets[i]) < len(sets[minIdx]) {
			minIdx = i
		}
	}
	base := sets[minIdx]
	out := make(map[SegmentInfo]struct{})
	for s := range base {
		ok := true
		for i := 0; i < len(sets); i++ {
			if i == minIdx {
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
