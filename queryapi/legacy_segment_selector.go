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
	"log/slog"
	"regexp"
	"slices"

	"github.com/google/codesearch/index"
	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/lrdb"
)

// SelectSegmentsFromLegacyFilter selects segments based on a legacy filter tree.
// It bypasses LogQL and converts the filter directly to trigram queries.
func SelectSegmentsFromLegacyFilter(
	ctx context.Context,
	dih DateIntHours,
	filter QueryClause,
	startTs, endTs int64,
	orgID uuid.UUID,
	lookupFunc SegmentLookupFunc,
) ([]SegmentInfo, error) {
	root := &TrigramQuery{Op: index.QAll}
	fpsToFetch := make(map[int64]struct{})

	// Build trigram query from filter tree
	filterToTrigramQuery(filter, fpsToFetch, &root)

	// If no fingerprints collected, add exists check for body field
	if len(fpsToFetch) == 0 {
		addExistsNode(bodyField, fpsToFetch, &root)
	}

	// Fetch candidate segments from database
	fpList := make([]int64, 0, len(fpsToFetch))
	for fp := range fpsToFetch {
		fpList = append(fpList, fp)
	}
	slices.Sort(fpList)

	slog.Info("Legacy segment selector: looking up fingerprints",
		"count", len(fpList),
		"fingerprints", fpList,
		"dateint", dih.DateInt,
		"org_id", orgID)

	rows, err := lookupFunc(ctx, lrdb.ListLogSegmentsForQueryParams{
		OrganizationID: orgID,
		Dateint:        int32(dih.DateInt),
		Fingerprints:   fpList,
		S:              startTs,
		E:              endTs,
	})
	if err != nil {
		return nil, fmt.Errorf("list log segments for query: %w", err)
	}

	slog.Info("Legacy segment selector: database returned segments",
		"total_rows", len(rows))

	// Group segments by fingerprint
	fpToSegments := make(map[int64][]SegmentInfo, len(rows))
	for _, row := range rows {
		seg := SegmentInfo{
			DateInt:        dih.DateInt,
			Hour:           zeroFilledHour(int(row.StartTs / 1000 / 3600 % 24)),
			SegmentID:      row.SegmentID,
			StartTs:        row.StartTs,
			EndTs:          row.EndTs,
			OrganizationID: orgID,
			InstanceNum:    row.InstanceNum,
			Frequency:      10000,
		}
		fpToSegments[row.Fingerprint] = append(fpToSegments[row.Fingerprint], seg)
	}

	// Log segments grouped by fingerprint
	for fp, segs := range fpToSegments {
		segIDs := make([]int64, len(segs))
		for i, s := range segs {
			segIDs[i] = s.SegmentID
		}
		slog.Info("Legacy segment selector: fingerprint matches",
			"fingerprint", fp,
			"segment_count", len(segs),
			"segment_ids", segIDs)
	}

	// Compute final set based on trigram query logic
	finalSet := computeSegmentSet(root, fpToSegments)

	finalSegIDs := make([]int64, 0, len(finalSet))
	for s := range finalSet {
		finalSegIDs = append(finalSegIDs, s.SegmentID)
	}
	slices.Sort(finalSegIDs)

	slog.Info("Legacy segment selector: final segments after trigram query logic",
		"final_count", len(finalSet),
		"segment_ids", finalSegIDs)

	out := make([]SegmentInfo, 0, len(finalSet))
	for s := range finalSet {
		out = append(out, s)
	}
	return out, nil
}

// filterToTrigramQuery recursively converts a legacy filter tree to trigram queries.
func filterToTrigramQuery(clause QueryClause, fps map[int64]struct{}, root **TrigramQuery) {
	if clause == nil {
		return
	}

	switch c := clause.(type) {
	case Filter:
		// Normalize label name (dots → underscores, _cardinalhq.* → chq_*)
		label := normalizeLabelName(c.K)

		// Check if this dimension is indexed
		if !slices.Contains(dimensionsToIndex, label) {
			addExistsNode(label, fps, root)
			return
		}

		switch c.Op {
		case "eq":
			if len(c.V) == 0 {
				return
			}
			// For full-value dimensions with exact match, use the full value fingerprint
			if slices.Contains(fullValueDimensions, label) {
				addFullValueNode(label, c.V[0], fps, root)
			} else {
				// For non-full-value dimensions, use trigram matching
				addAndNodeFromPattern(label, regexp.QuoteMeta(c.V[0]), fps, root)
			}

		case "in":
			if len(c.V) == 0 {
				return
			}
			// For full-value dimensions, create OR of exact matches
			if slices.Contains(fullValueDimensions, label) {
				addOrNodeFromValues(label, c.V, fps, root)
			} else {
				// For non-full-value dimensions, create OR of trigram patterns
				var orSubs []*TrigramQuery
				for _, val := range c.V {
					pattern := regexp.QuoteMeta(val)
					lt, fpsList := buildLabelTrigram(label, pattern)
					for _, fp := range fpsList {
						fps[fp] = struct{}{}
					}
					tq := fromIndexQuery(label, lt)
					orSubs = append(orSubs, tq)
				}
				if len(orSubs) > 0 {
					*root = &TrigramQuery{
						Op:  index.QAnd,
						Sub: []*TrigramQuery{*root, {Op: index.QOr, Sub: orSubs}},
					}
				}
			}

		case "contains":
			if len(c.V) == 0 {
				return
			}
			if slices.Contains(dimensionsToIndex, label) {
				pattern := ".*" + regexp.QuoteMeta(c.V[0]) + ".*"
				addAndNodeFromPattern(label, pattern, fps, root)
			} else {
				addExistsNode(label, fps, root)
			}

		case "regex":
			if len(c.V) == 0 {
				return
			}
			// For full-value dimensions, fall back to exists check
			if slices.Contains(fullValueDimensions, label) {
				addExistsNode(label, fps, root)
			} else {
				// Use the regex pattern directly
				addAndNodeFromPattern(label, c.V[0], fps, root)
			}

		case "gt", "gte", "lt", "lte":
			// Comparison operators can't be optimized with trigrams
			// Fall back to exists check (filter will be applied in SQL)
			addExistsNode(label, fps, root)

		default:
			// Unknown operator, fall back to exists check
			addExistsNode(label, fps, root)
		}

	case BinaryClause:
		if len(c.Clauses) == 0 {
			return
		}

		op := c.Op
		switch op {
		case "and":
			// For AND: combine all sub-clauses with AND
			for _, subClause := range c.Clauses {
				filterToTrigramQuery(subClause, fps, root)
			}
		case "or":
			// For OR: create OR node with all sub-clauses
			var orSubs []*TrigramQuery
			for _, subClause := range c.Clauses {
				// Create a fresh root for each OR branch
				branchRoot := &TrigramQuery{Op: index.QAll}
				filterToTrigramQuery(subClause, fps, &branchRoot)
				orSubs = append(orSubs, branchRoot)
			}
			if len(orSubs) > 0 {
				*root = &TrigramQuery{
					Op:  index.QAnd,
					Sub: []*TrigramQuery{*root, {Op: index.QOr, Sub: orSubs}},
				}
			}
		}
	}
}
