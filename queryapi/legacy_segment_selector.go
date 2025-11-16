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
// Returns the matching segments and the fpToSegments map for filter pruning.
func SelectSegmentsFromLegacyFilter(
	ctx context.Context,
	dih DateIntHours,
	filter QueryClause,
	startTs, endTs int64,
	orgID uuid.UUID,
	lookupFunc SegmentLookupFunc,
) ([]SegmentInfo, map[int64][]SegmentInfo, error) {
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
		return nil, nil, fmt.Errorf("list log segments for query: %w", err)
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
		slog.Info("Legacy segment selector: fingerprint matches",
			"fingerprint", fp,
			"segment_count", len(segs))
	}

	// Compute final set based on trigram query logic
	finalSet := computeSegmentSet(root, fpToSegments)

	finalSegIDs := make([]int64, 0, len(finalSet))
	for s := range finalSet {
		finalSegIDs = append(finalSegIDs, s.SegmentID)
	}
	slices.Sort(finalSegIDs)

	slog.Info("Legacy segment selector: final segments after trigram query logic",
		"final_count", len(finalSet))

	out := make([]SegmentInfo, 0, len(finalSet))
	for s := range finalSet {
		out = append(out, s)
	}
	return out, fpToSegments, nil
}

// buildTrigramQueryForClause builds a standalone trigram query for a single clause.
// This is used for OR branches where each branch needs to be independent.
// Returns nil if the clause produces no query.
func buildTrigramQueryForClause(clause QueryClause, fps map[int64]struct{}) *TrigramQuery {
	if clause == nil {
		return nil
	}

	switch c := clause.(type) {
	case Filter:
		label := normalizeLabelName(c.K)

		// Check if this dimension is indexed
		if !slices.Contains(dimensionsToIndex, label) {
			fp := computeFingerprint(label, existsRegex)
			fps[fp] = struct{}{}
			return &TrigramQuery{
				Op:        index.QAnd,
				fieldName: label,
				Trigram:   []string{existsRegex},
			}
		}

		switch c.Op {
		case "eq":
			if len(c.V) == 0 {
				return nil
			}
			if slices.Contains(fullValueDimensions, label) {
				existsFp := computeFingerprint(label, existsRegex)
				valueFp := computeFingerprint(label, c.V[0])
				fps[existsFp] = struct{}{}
				fps[valueFp] = struct{}{}
				return &TrigramQuery{
					Op:        index.QAnd,
					fieldName: label,
					Trigram:   []string{existsRegex, c.V[0]},
				}
			} else {
				pattern := regexp.QuoteMeta(c.V[0])
				lt, fpsList := buildLabelTrigram(label, pattern)
				for _, fp := range fpsList {
					fps[fp] = struct{}{}
				}
				return fromIndexQuery(label, lt)
			}

		case "in":
			if len(c.V) == 0 {
				return nil
			}
			if slices.Contains(fullValueDimensions, label) {
				if len(c.V) == 1 {
					existsFp := computeFingerprint(label, existsRegex)
					valueFp := computeFingerprint(label, c.V[0])
					fps[existsFp] = struct{}{}
					fps[valueFp] = struct{}{}
					return &TrigramQuery{
						Op:        index.QAnd,
						fieldName: label,
						Trigram:   []string{existsRegex, c.V[0]},
					}
				}
				var orSubs []*TrigramQuery
				for _, val := range c.V {
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
				return &TrigramQuery{Op: index.QOr, Sub: orSubs}
			} else {
				var orSubs []*TrigramQuery
				for _, val := range c.V {
					pattern := regexp.QuoteMeta(val)
					lt, fpsList := buildLabelTrigram(label, pattern)
					for _, fp := range fpsList {
						fps[fp] = struct{}{}
					}
					orSubs = append(orSubs, fromIndexQuery(label, lt))
				}
				if len(orSubs) == 1 {
					return orSubs[0]
				}
				return &TrigramQuery{Op: index.QOr, Sub: orSubs}
			}

		case "contains":
			if len(c.V) == 0 {
				return nil
			}
			if slices.Contains(fullValueDimensions, label) {
				fp := computeFingerprint(label, existsRegex)
				fps[fp] = struct{}{}
				return &TrigramQuery{
					Op:        index.QAnd,
					fieldName: label,
					Trigram:   []string{existsRegex},
				}
			} else {
				pattern := ".*" + regexp.QuoteMeta(c.V[0]) + ".*"
				lt, fpsList := buildLabelTrigram(label, pattern)
				for _, fp := range fpsList {
					fps[fp] = struct{}{}
				}
				return fromIndexQuery(label, lt)
			}

		case "regex":
			if len(c.V) == 0 {
				return nil
			}
			if slices.Contains(fullValueDimensions, label) {
				fp := computeFingerprint(label, existsRegex)
				fps[fp] = struct{}{}
				return &TrigramQuery{
					Op:        index.QAnd,
					fieldName: label,
					Trigram:   []string{existsRegex},
				}
			} else {
				lt, fpsList := buildLabelTrigram(label, c.V[0])
				for _, fp := range fpsList {
					fps[fp] = struct{}{}
				}
				return fromIndexQuery(label, lt)
			}

		case "gt", "gte", "lt", "lte":
			fp := computeFingerprint(label, existsRegex)
			fps[fp] = struct{}{}
			return &TrigramQuery{
				Op:        index.QAnd,
				fieldName: label,
				Trigram:   []string{existsRegex},
			}

		default:
			fp := computeFingerprint(label, existsRegex)
			fps[fp] = struct{}{}
			return &TrigramQuery{
				Op:        index.QAnd,
				fieldName: label,
				Trigram:   []string{existsRegex},
			}
		}

	case BinaryClause:
		if len(c.Clauses) == 0 {
			return nil
		}

		if c.Op == "and" {
			// For AND: build each sub-clause and combine with AND
			var andSubs []*TrigramQuery
			for _, subClause := range c.Clauses {
				branch := buildTrigramQueryForClause(subClause, fps)
				if branch != nil {
					andSubs = append(andSubs, branch)
				}
			}
			if len(andSubs) == 0 {
				return nil
			}
			if len(andSubs) == 1 {
				return andSubs[0]
			}
			return &TrigramQuery{Op: index.QAnd, Sub: andSubs}
		} else if c.Op == "or" {
			// For OR: build each sub-clause and combine with OR
			var orSubs []*TrigramQuery
			for _, subClause := range c.Clauses {
				branch := buildTrigramQueryForClause(subClause, fps)
				if branch != nil {
					orSubs = append(orSubs, branch)
				}
			}
			if len(orSubs) == 0 {
				return nil
			}
			if len(orSubs) == 1 {
				return orSubs[0]
			}
			return &TrigramQuery{Op: index.QOr, Sub: orSubs}
		}
	}

	return nil
}

// filterToTrigramQuery recursively converts a legacy filter tree to trigram queries.
// It ANDs the clause's query with the existing root.
func filterToTrigramQuery(clause QueryClause, fps map[int64]struct{}, root **TrigramQuery) {
	branch := buildTrigramQueryForClause(clause, fps)
	if branch != nil {
		*root = &TrigramQuery{
			Op:  index.QAnd,
			Sub: []*TrigramQuery{*root, branch},
		}
	}
}

// PruneFilterForMissingFields removes filter branches that reference fields whose
// "exists" fingerprint is not present in the returned segments.
// This prevents SQL errors when querying non-existent columns in OR clauses.
func PruneFilterForMissingFields(filter QueryClause, fpToSegments map[int64][]SegmentInfo) QueryClause {
	if filter == nil {
		return nil
	}

	switch c := filter.(type) {
	case Filter:
		// Normalize the label name
		label := normalizeLabelName(c.K)

		// For non-indexed fields, check if the exists fingerprint is present
		if !slices.Contains(dimensionsToIndex, label) {
			existsFp := computeFingerprint(label, existsRegex)
			// If the exists fingerprint is not in fpToSegments, this field doesn't exist in any segment
			if _, exists := fpToSegments[existsFp]; !exists {
				slog.Info("Pruning filter for non-existent field",
					"field", c.K,
					"normalized", label,
					"exists_fp", existsFp)
				return nil // Prune this filter
			}
		}
		// Field exists or is indexed, keep the filter
		return c

	case BinaryClause:
		if len(c.Clauses) == 0 {
			return nil
		}

		// Recursively prune sub-clauses
		var prunedClauses []QueryClause
		for _, subClause := range c.Clauses {
			pruned := PruneFilterForMissingFields(subClause, fpToSegments)
			if pruned != nil {
				prunedClauses = append(prunedClauses, pruned)
			}
		}

		// Handle different operators
		if c.Op == "or" {
			// For OR: if all branches pruned, return nil; if only one left, return it directly
			if len(prunedClauses) == 0 {
				return nil
			}
			if len(prunedClauses) == 1 {
				return prunedClauses[0]
			}
			return BinaryClause{Op: "or", Clauses: prunedClauses}
		} else if c.Op == "and" {
			// For AND: if ANY branch was pruned, the entire AND fails
			// A pruned branch means a non-existent field was referenced, so nothing can match
			if len(prunedClauses) < len(c.Clauses) {
				slog.Info("AND clause has pruned branch - entire AND fails",
					"original_count", len(c.Clauses),
					"pruned_count", len(prunedClauses))
				return nil
			}
			// All branches survived pruning
			if len(prunedClauses) == 0 {
				return nil
			}
			if len(prunedClauses) == 1 {
				return prunedClauses[0]
			}
			return BinaryClause{Op: "and", Clauses: prunedClauses}
		}

		return BinaryClause{Op: c.Op, Clauses: prunedClauses}
	}

	return filter
}
