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
	"log/slog"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/logql"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/lakerunner/promql"

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
	fields []string,
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

		// If limit <= 0, treat as unlimited.
		unlimited := limit <= 0

		// Cancellation for the entire pushdown tree.
		ctxAll, cancelAll := context.WithCancel(ctx)
		defer cancelAll()

		emitted := 0

		// Partition by dateInt hours for storage listing.
		dateIntHours := dateIntHoursRange(startTs, endTs, time.UTC, reverse)

	outer:
		for _, leaf := range queryPlan.Leaves {
			for _, dih := range dateIntHours {
				segments, err := q.lookupLogsSegments(ctxAll, dih, leaf, startTs, endTs, orgID, q.mdb.ListLogSegmentsForQuery)
				if err != nil {
					slog.Error("failed to lookup log segments", "err", err, "dih", dih, "leaf", leaf)
					return
				}
				if len(segments) == 0 {
					continue
				}

				groups := ComputeReplayBatchesWithWorkers(segments, DefaultLogStep, startTs, endTs, len(workers), reverse)
				for _, group := range groups {
					select {
					case <-ctxAll.Done():
						return
					default:
					}

					// Respect global limit across groups
					remaining := math.MaxInt // large default
					if !unlimited {
						remaining = limit - emitted
						if remaining <= 0 {
							cancelAll()
							break outer
						}
					}

					slog.Info("Pushing down segments", "groupSize", len(group.Segments), "remaining", remaining)

					// Collect all segment IDs for worker assignment
					segmentIDs := make([]int64, 0, len(group.Segments))
					segmentMap := make(map[int64][]SegmentInfo)
					for _, segment := range group.Segments {
						segmentIDs = append(segmentIDs, segment.SegmentID)
						segmentMap[segment.SegmentID] = append(segmentMap[segment.SegmentID], segment)
					}

					// Get worker assignments
					mappings, err := q.workerDiscovery.GetWorkersForSegments(orgID, segmentIDs)
					if err != nil {
						slog.Error("failed to get worker assignments", "err", err)
						continue
					}

					workerGroups := make(map[Worker][]SegmentInfo)
					for _, mapping := range mappings {
						segmentList := segmentMap[mapping.SegmentID]
						workerGroups[mapping.Worker] = append(workerGroups[mapping.Worker], segmentList...)
					}

					var groupLeafChans []<-chan promql.Timestamped
					for worker, workerSegments := range workerGroups {
						reqLimit := 0
						if !unlimited {
							reqLimit = remaining
						}
						req := PushDownRequest{
							OrganizationID: orgID,
							LogLeaf:        &leaf,
							StartTs:        group.StartTs,
							EndTs:          group.EndTs,
							Segments:       workerSegments,
							Step:           stepDuration,
							Limit:          reqLimit,
							Reverse:        reverse,
							Fields:         fields,
						}
						ch, err := q.logsPushDown(ctxAll, worker, req)
						if err != nil {
							slog.Error("pushdown failed", "worker", worker, "err", err)
							continue
						}
						groupLeafChans = append(groupLeafChans, ch)
					}
					if len(groupLeafChans) == 0 {
						continue
					}

					// Merge this group's worker streams by timestamp
					mergeLimit := 0 // unlimited for the merge by default
					if !unlimited {
						mergeLimit = remaining
					}
					mergedGroup := promql.MergeSorted(ctxAll, 1024, reverse, mergeLimit, groupLeafChans...)

					// Forward results (and stop globally when limit hit)
					for {
						select {
						case <-ctxAll.Done():
							return
						case res, ok := <-mergedGroup:
							if !ok {
								// Group done
								goto nextGroup
							}
							out <- res
							if !unlimited {
								emitted++
								if emitted >= limit {
									cancelAll() // stop all remaining work
									break outer
								}
							}
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
	return PushDownStream(ctx, worker, request,
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

const bodyField = "log_message"

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
	orgUUID uuid.UUID,
	lookupFunc SegmentLookupFunc,
) ([]SegmentInfo, error) {
	root := &TrigramQuery{Op: index.QAll}
	fpsToFetch := make(map[int64]struct{})

	for _, lm := range leaf.Matchers {
		label, val := lm.Label, lm.Value
		if !slices.Contains(dimensionsToIndex, label) {
			addExistsNode(label, fpsToFetch, &root)
			continue
		}
		switch lm.Op {
		case logql.MatchEq:
			// For full-value dimensions with exact match, use the full value fingerprint
			if slices.Contains(fullValueDimensions, label) {
				addFullValueNode(label, val, fpsToFetch, &root)
			} else {
				addAndNodeFromPattern(label, val, fpsToFetch, &root)
			}
		case logql.MatchRe:
			// For full-value dimensions, check if this is a simple alternation (e.g., from 'in' operator)
			// If so, convert to OR of exact matches instead of falling back to exists check
			if slices.Contains(fullValueDimensions, label) {
				if values, ok := tryExtractExactAlternates(val); ok && len(values) > 0 {
					// Create OR of exact value matches
					addOrNodeFromValues(label, values, fpsToFetch, &root)
				} else {
					// Fall back to exists check for complex regex patterns
					addExistsNode(label, fpsToFetch, &root)
				}
			} else {
				// For non-full-value dimensions, use trigram matching
				addAndNodeFromPattern(label, val, fpsToFetch, &root)
			}
		default:
			addExistsNode(label, fpsToFetch, &root)
		}
	}

	for _, lf := range leaf.LabelFilters {
		if lf.ParserIdx != nil {
			continue
		}
		label, val := lf.Label, lf.Value
		if !slices.Contains(dimensionsToIndex, label) {
			addExistsNode(label, fpsToFetch, &root)
			continue
		}
		switch lf.Op {
		case logql.MatchEq:
			// For full-value dimensions with exact match, use the full value fingerprint
			if slices.Contains(fullValueDimensions, label) {
				addFullValueNode(label, val, fpsToFetch, &root)
			} else {
				addAndNodeFromPattern(label, val, fpsToFetch, &root)
			}
		case logql.MatchRe:
			// For full-value dimensions, check if this is a simple alternation (e.g., from 'in' operator)
			// If so, convert to OR of exact matches instead of falling back to exists check
			if slices.Contains(fullValueDimensions, label) {
				if values, ok := tryExtractExactAlternates(val); ok && len(values) > 0 {
					// Create OR of exact value matches
					addOrNodeFromValues(label, values, fpsToFetch, &root)
				} else {
					// Fall back to exists check for complex regex patterns
					addExistsNode(label, fpsToFetch, &root)
				}
			} else {
				// For non-full-value dimensions, use trigram matching
				addAndNodeFromPattern(label, val, fpsToFetch, &root)
			}
		default:
			addExistsNode(label, fpsToFetch, &root)
		}
	}

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

	fpToSegments := make(map[int64][]SegmentInfo, len(rows))
	for _, row := range rows {
		startHour := zeroFilledHour(time.UnixMilli(row.StartTs).UTC().Hour())
		seg := SegmentInfo{
			DateInt:        dih.DateInt,
			Hour:           startHour,
			SegmentID:      row.SegmentID,
			StartTs:        row.StartTs,
			EndTs:          row.EndTs,
			OrganizationID: orgUUID,
			InstanceNum:    row.InstanceNum,
			Frequency:      10000,
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
	fp := computeFingerprint(label, existsRegex)
	fps[fp] = struct{}{}
	tq := &TrigramQuery{
		Op:        index.QAnd,
		fieldName: label,
		Trigram:   []string{existsRegex},
	}
	*root = &TrigramQuery{Op: index.QAnd, Sub: []*TrigramQuery{*root, tq}}
}

func addFullValueNode(label, value string, fps map[int64]struct{}, root **TrigramQuery) {
	// For full-value dimensions, we index both the exists fingerprint and the exact value
	// Add both fingerprints to the query
	existsFp := computeFingerprint(label, existsRegex)
	valueFp := computeFingerprint(label, value)
	fps[existsFp] = struct{}{}
	fps[valueFp] = struct{}{}

	// Create a query node that requires both fingerprints (AND)
	tq := &TrigramQuery{
		Op:        index.QAnd,
		fieldName: label,
		Trigram:   []string{existsRegex, value},
	}
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
			fp := computeFingerprint(q.fieldName, tri)
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
			fp := computeFingerprint(q.fieldName, tri)
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
			fps = append(fps, computeFingerprint(label, tri))
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
				fps = append(fps, computeFingerprint(label, tri))
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

// addOrNodeFromValues creates an OR query node for multiple exact values
// This is used for optimizing regex patterns that are really just alternations (e.g., from 'in' operator)
func addOrNodeFromValues(label string, values []string, fps map[int64]struct{}, root **TrigramQuery) {
	if len(values) == 0 {
		return
	}

	if len(values) == 1 {
		// Single value, just use exact match
		addFullValueNode(label, values[0], fps, root)
		return
	}

	// Multiple values - create OR of exact matches
	var orSubs []*TrigramQuery
	for _, val := range values {
		existsFp := computeFingerprint(label, existsRegex)
		valueFp := computeFingerprint(label, val)
		fps[existsFp] = struct{}{}
		fps[valueFp] = struct{}{}

		orSubs = append(orSubs, &TrigramQuery{
			Op:        index.QAnd,
			fieldName: label,
			Trigram:   []string{existsRegex, val},
		})
	}

	orNode := &TrigramQuery{
		Op:  index.QOr,
		Sub: orSubs,
	}

	*root = &TrigramQuery{Op: index.QAnd, Sub: []*TrigramQuery{*root, orNode}}
}

// tryExtractExactAlternates attempts to extract exact values from a simple alternation pattern
// like ^(val1|val2|val3)$ by string parsing. Returns the unescaped values and true if successful.
// Returns false for patterns with wildcards or other complex regex features.
func tryExtractExactAlternates(pattern string) ([]string, bool) {
	// Check if it matches the basic structure: ^(...)$
	if !strings.HasPrefix(pattern, "^(") || !strings.HasSuffix(pattern, ")$") {
		// Also try without parens: ^val$
		if strings.HasPrefix(pattern, "^") && strings.HasSuffix(pattern, "$") {
			inner := strings.TrimPrefix(pattern, "^")
			inner = strings.TrimSuffix(inner, "$")
			// Make sure it's just a literal (no regex metacharacters except escaped ones)
			if isSimpleLiteral(inner) {
				return []string{unescapeRegex(inner)}, true
			}
		}
		return nil, false
	}

	// Extract the content between ^( and )$
	inner := strings.TrimPrefix(pattern, "^(")
	inner = strings.TrimSuffix(inner, ")$")

	// Split by | at the top level (not inside nested parens/brackets)
	parts := splitTopLevelPipe(inner)
	if len(parts) == 0 {
		return nil, false
	}

	// Check each part is a simple literal (only escaped metacharacters allowed)
	var values []string
	for _, part := range parts {
		if !isSimpleLiteral(part) {
			return nil, false
		}
		values = append(values, unescapeRegex(part))
	}

	return values, true
}

// splitTopLevelPipe splits on | but not inside nested ()[]{}
func splitTopLevelPipe(s string) []string {
	var parts []string
	var current strings.Builder
	depth := 0
	escaped := false

	for i := 0; i < len(s); i++ {
		c := s[i]

		if escaped {
			current.WriteByte(c)
			escaped = false
			continue
		}

		if c == '\\' {
			current.WriteByte(c)
			escaped = true
			continue
		}

		if c == '(' || c == '[' || c == '{' {
			depth++
			current.WriteByte(c)
			continue
		}

		if c == ')' || c == ']' || c == '}' {
			depth--
			current.WriteByte(c)
			continue
		}

		if c == '|' && depth == 0 {
			parts = append(parts, current.String())
			current.Reset()
			continue
		}

		current.WriteByte(c)
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// isSimpleLiteral checks if a string is just literals and escaped metacharacters.
// Returns false if it contains unescaped regex metacharacters that change meaning.
func isSimpleLiteral(s string) bool {
	escaped := false
	for i := 0; i < len(s); i++ {
		c := s[i]

		if escaped {
			escaped = false
			continue
		}

		if c == '\\' {
			escaped = true
			continue
		}

		// Check for unescaped regex metacharacters that aren't just literals
		if strings.ContainsRune(".+*?()[]{}^$|", rune(c)) {
			return false
		}
	}

	return true
}

// unescapeRegex unescapes common regex escapes like \. to .
func unescapeRegex(s string) string {
	var result strings.Builder
	escaped := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if escaped {
			result.WriteByte(c)
			escaped = false
		} else if c == '\\' {
			escaped = true
		} else {
			result.WriteByte(c)
		}
	}
	return result.String()
}
