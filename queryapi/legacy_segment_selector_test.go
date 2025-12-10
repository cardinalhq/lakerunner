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
	"testing"

	"github.com/google/codesearch/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFilterToTrigramQuery_SimpleContains(t *testing.T) {
	// Test a simple contains query on an indexed field
	filter := Filter{
		K:  "resource.file.type",
		V:  []string{"cloudxcommand"},
		Op: "contains",
	}

	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})

	filterToTrigramQuery(filter, fps, &root)

	// Should collect fingerprints for the trigrams
	assert.NotEmpty(t, fps, "Should collect fingerprints for trigrams")

	// Root should be modified to include the trigram query
	assert.NotNil(t, root)
	assert.Equal(t, index.QAnd, root.Op, "Root should be AND operation")
}

func TestFilterToTrigramQuery_NonIndexedFieldContains(t *testing.T) {
	// Test contains query on a non-indexed field (should use exists check)
	filter := Filter{
		K:  "log.source", // This field is NOT in dimensionsToIndex
		V:  []string{"cloudxcommand"},
		Op: "contains",
	}

	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})

	filterToTrigramQuery(filter, fps, &root)

	// Should collect fingerprint for exists check
	assert.NotEmpty(t, fps, "Should collect fingerprint for exists check")
	assert.Len(t, fps, 1, "Should have exactly one fingerprint for exists check")

	// Verify it's the exists fingerprint
	expectedFp := computeFingerprint("log_source", existsRegex)
	assert.Contains(t, fps, expectedFp, "Should have exists fingerprint")
}

func TestFilterToTrigramQuery_OrClause(t *testing.T) {
	// Test OR clause with two contains conditions
	filter := BinaryClause{
		Op: "or",
		Clauses: []QueryClause{
			Filter{
				K:  "resource.file.type",
				V:  []string{"cloudxcommand"},
				Op: "contains",
			},
			Filter{
				K:  "log.source",
				V:  []string{"cloudxcommand"},
				Op: "contains",
			},
		},
	}

	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})

	filterToTrigramQuery(filter, fps, &root)

	// Should collect fingerprints from both branches
	assert.NotEmpty(t, fps, "Should collect fingerprints from both OR branches")

	// Should have fingerprints for:
	// 1. Trigrams from resource_file_type
	// 2. Exists check for log_source
	t.Logf("Collected %d fingerprints", len(fps))
	for fp := range fps {
		t.Logf("Fingerprint: %d", fp)
	}

	// Root should be modified to include OR logic
	assert.NotNil(t, root)
	assert.Equal(t, index.QAnd, root.Op, "Root should be AND operation")
	require.Len(t, root.Sub, 2, "Root should have 2 sub-queries (QAll and OR)")

	// Second sub should be the OR node
	orNode := root.Sub[1]
	assert.Equal(t, index.QOr, orNode.Op, "Second sub should be OR operation")
	assert.Len(t, orNode.Sub, 2, "OR node should have 2 branches")
}

func TestFilterToTrigramQuery_AndClause(t *testing.T) {
	// Test AND clause with two conditions
	filter := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "resource.bucket.name",
				V:  []string{"test-bucket"},
				Op: "eq",
			},
			Filter{
				K:  "resource.file.type",
				V:  []string{"cloudxcommand"},
				Op: "contains",
			},
		},
	}

	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})

	filterToTrigramQuery(filter, fps, &root)

	// Should collect fingerprints from both conditions
	assert.NotEmpty(t, fps, "Should collect fingerprints from both AND conditions")
	t.Logf("Collected %d fingerprints for AND query", len(fps))

	// Root should combine both conditions with AND
	assert.NotNil(t, root)
}

func TestFilterToTrigramQuery_ComplexOrWithAndBranches(t *testing.T) {
	// Test the real-world scenario: AND of (bucket, file, OR(file_type, log_source))
	filter := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "resource.bucket.name",
				V:  []string{"test-bucket"},
				Op: "eq",
			},
			Filter{
				K:  "resource.file",
				V:  []string{"test-file_controller"},
				Op: "in",
			},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{
						K:  "resource.file.type",
						V:  []string{"cloudxcommand"},
						Op: "contains",
					},
					Filter{
						K:  "log.source",
						V:  []string{"cloudxcommand"},
						Op: "contains",
					},
				},
			},
		},
	}

	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})

	filterToTrigramQuery(filter, fps, &root)

	// Should collect fingerprints from all branches
	assert.NotEmpty(t, fps, "Should collect fingerprints from all branches")
	t.Logf("Collected %d fingerprints for complex query", len(fps))

	// Log all collected fingerprints for debugging
	for fp := range fps {
		t.Logf("Fingerprint: %d", fp)
	}

	// Verify we have fingerprints for:
	// 1. resource_bucket_name (full-value)
	// 2. resource_file (full-value)
	// 3. resource_file_type trigrams
	// 4. log_source exists
	assert.GreaterOrEqual(t, len(fps), 4, "Should have at least 4 fingerprints")
}

func TestFilterToTrigramQuery_IndexedVsNonIndexedFields(t *testing.T) {
	tests := []struct {
		name          string
		fieldName     string
		isIndexed     bool
		isFullValue   bool // full-value dimensions only get 1 fingerprint (no trigrams)
		expectedFpMin int  // Minimum expected fingerprints
		expectedFpMax int  // Maximum expected fingerprints (0 = no max)
	}{
		{
			name:          "indexed_field_resource_service_name_full_value",
			fieldName:     "resource.service.name",
			isIndexed:     true,
			isFullValue:   true,
			expectedFpMin: 1, // Full-value dimension: just the exists fingerprint for contains
			expectedFpMax: 1,
		},
		{
			name:          "non_indexed_field_log_source",
			fieldName:     "log.source",
			isIndexed:     false,
			isFullValue:   false,
			expectedFpMin: 1, // Just the exists fingerprint
			expectedFpMax: 1,
		},
		{
			name:          "indexed_field_log_level",
			fieldName:     "log.level",
			isIndexed:     true,
			isFullValue:   false,
			expectedFpMin: 2, // Trigrams from "test": "tes" and "est"
			expectedFpMax: 0, // No max
		},
		{
			name:          "non_indexed_field_resource_file_type",
			fieldName:     "resource.file.type",
			isIndexed:     false,
			isFullValue:   false,
			expectedFpMin: 1, // Not in dimensionsToIndex, uses exists check
			expectedFpMax: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := Filter{
				K:  tt.fieldName,
				V:  []string{"test"},
				Op: "contains",
			}

			root := &TrigramQuery{Op: index.QAll}
			fps := make(map[int64]struct{})

			filterToTrigramQuery(filter, fps, &root)

			assert.GreaterOrEqual(t, len(fps), tt.expectedFpMin,
				"Should generate at least %d fingerprints", tt.expectedFpMin)
			if tt.expectedFpMax > 0 {
				assert.LessOrEqual(t, len(fps), tt.expectedFpMax,
					"Should generate at most %d fingerprints", tt.expectedFpMax)
			}
		})
	}
}

func TestFilterToTrigramQuery_OrWithOnlyNonIndexedFields(t *testing.T) {
	// Test OR clause where NONE of the fields are indexed
	// This tests the edge case where both branches just do exists checks
	filter := BinaryClause{
		Op: "or",
		Clauses: []QueryClause{
			Filter{
				K:  "custom_field1", // Not indexed
				V:  []string{"value1"},
				Op: "contains",
			},
			Filter{
				K:  "custom_field2", // Not indexed
				V:  []string{"value2"},
				Op: "contains",
			},
		},
	}

	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})

	filterToTrigramQuery(filter, fps, &root)

	// Should have 2 fingerprints (one exists check for each field)
	assert.Equal(t, 2, len(fps), "Should have 2 exists fingerprints")

	// Root should have OR structure
	require.NotNil(t, root)
	require.Len(t, root.Sub, 2, "Root should have 2 sub-queries")
	orNode := root.Sub[1]
	assert.Equal(t, index.QOr, orNode.Op, "Second sub should be OR")
	assert.Len(t, orNode.Sub, 2, "OR should have 2 branches")
}

// Helper to visualize tree structure for debugging
func printTrigramTree(t *testing.T, q *TrigramQuery, indent string) {
	if q == nil {
		return
	}
	opName := "UNKNOWN"
	switch q.Op {
	case index.QAll:
		opName = "QAll"
	case index.QAnd:
		opName = "QAnd"
	case index.QOr:
		opName = "QOr"
	}
	t.Logf("%s%s (field=%s, trigrams=%v)", indent, opName, q.fieldName, q.Trigram)
	for _, sub := range q.Sub {
		printTrigramTree(t, sub, indent+"  ")
	}
}

func TestFilterToTrigramQuery_NestedStructureValidation(t *testing.T) {
	// Test: AND of (A, OR of (B, C))

	filter := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "log.level",
				V:  []string{"error"},
				Op: "eq",
			},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{
						K:  "resource.service.name",
						V:  []string{"api"},
						Op: "contains",
					},
					Filter{
						K:  "resource.k8s.namespace.name",
						V:  []string{"prod"},
						Op: "contains",
					},
				},
			},
		},
	}

	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})

	filterToTrigramQuery(filter, fps, &root)

	t.Logf("Collected %d fingerprints", len(fps))
	t.Log("Tree structure:")
	printTrigramTree(t, root, "")

	// Verify structure
	require.NotNil(t, root)
	assert.Equal(t, index.QAnd, root.Op, "Root should be QAnd")
	require.GreaterOrEqual(t, len(root.Sub), 2, "Root should have at least 2 children")

	// Walk the tree to find the OR node
	var findOr func(q *TrigramQuery) *TrigramQuery
	findOr = func(q *TrigramQuery) *TrigramQuery {
		if q == nil {
			return nil
		}
		if q.Op == index.QOr {
			return q
		}
		for _, sub := range q.Sub {
			if result := findOr(sub); result != nil {
				return result
			}
		}
		return nil
	}

	orNode := findOr(root)
	require.NotNil(t, orNode, "Should find OR node in tree")
	assert.Equal(t, index.QOr, orNode.Op, "Found node should be OR")
	assert.Len(t, orNode.Sub, 2, "OR should have exactly 2 branches")

	// Each OR branch will be QAnd (wrapping QAll + condition)
	for i, branch := range orNode.Sub {
		assert.NotNil(t, branch, "OR branch %d should not be nil", i)
		// Branch gets wrapped in QAnd when condition is added
		assert.Equal(t, index.QAnd, branch.Op, "OR branch %d should be QAnd after processing", i)
	}
}

func TestFilterToTrigramQuery_OrOfAnds(t *testing.T) {
	// Test: OR of (AND(A,B), AND(C,D))
	// Expected: OR node with two branches, each branch is an AND of conditions

	filter := BinaryClause{
		Op: "or",
		Clauses: []QueryClause{
			BinaryClause{
				Op: "and",
				Clauses: []QueryClause{
					Filter{K: "log.level", V: []string{"error"}, Op: "eq"},
					Filter{K: "resource.service.name", V: []string{"api"}, Op: "contains"},
				},
			},
			BinaryClause{
				Op: "and",
				Clauses: []QueryClause{
					Filter{K: "log.level", V: []string{"warn"}, Op: "eq"},
					Filter{K: "resource.service.name", V: []string{"web"}, Op: "contains"},
				},
			},
		},
	}

	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})

	filterToTrigramQuery(filter, fps, &root)

	t.Logf("Collected %d fingerprints", len(fps))
	t.Log("Tree structure:")
	printTrigramTree(t, root, "")

	// Find the OR node
	var findOr func(q *TrigramQuery) *TrigramQuery
	findOr = func(q *TrigramQuery) *TrigramQuery {
		if q == nil {
			return nil
		}
		if q.Op == index.QOr {
			return q
		}
		for _, sub := range q.Sub {
			if result := findOr(sub); result != nil {
				return result
			}
		}
		return nil
	}

	orNode := findOr(root)
	require.NotNil(t, orNode, "Should find OR node")
	assert.Len(t, orNode.Sub, 2, "OR should have 2 branches")

	// Each branch will be QAnd after processing the AND clause
	for i, branch := range orNode.Sub {
		assert.NotNil(t, branch, "Branch %d should not be nil", i)
		assert.Equal(t, index.QAnd, branch.Op, "Branch %d should be QAnd after processing", i)
	}

	// Should collect fingerprints from all 4 filters
	assert.GreaterOrEqual(t, len(fps), 4, "Should have fingerprints from all 4 filters")
}

func TestFilterToTrigramQuery_DeepNesting(t *testing.T) {
	// Test: AND(A, OR(B, AND(C, D)))
	// This tests 3 levels of nesting

	filter := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{K: "log.level", V: []string{"error"}, Op: "eq"},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{K: "resource.service.name", V: []string{"api"}, Op: "contains"},
					BinaryClause{
						Op: "and",
						Clauses: []QueryClause{
							Filter{K: "resource.k8s.namespace.name", V: []string{"prod"}, Op: "contains"},
							Filter{K: "log.source", V: []string{"app"}, Op: "contains"},
						},
					},
				},
			},
		},
	}

	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})

	filterToTrigramQuery(filter, fps, &root)

	t.Logf("Collected %d fingerprints", len(fps))

	// Verify we collected fingerprints from all levels
	// log.level (eq) = 1 fp
	// resource.service.name (contains, indexed) = 1+ fps
	// resource.k8s.namespace.name (contains, indexed) = 1+ fps
	// log.source (contains, non-indexed) = 1 fp
	assert.GreaterOrEqual(t, len(fps), 4, "Should collect fingerprints from all nested levels")

	// Verify OR node exists
	var findOr func(q *TrigramQuery) *TrigramQuery
	findOr = func(q *TrigramQuery) *TrigramQuery {
		if q == nil {
			return nil
		}
		if q.Op == index.QOr {
			return q
		}
		for _, sub := range q.Sub {
			if result := findOr(sub); result != nil {
				return result
			}
		}
		return nil
	}

	orNode := findOr(root)
	require.NotNil(t, orNode, "Should find OR node in deeply nested structure")
	assert.Len(t, orNode.Sub, 2, "OR should have 2 branches")
}

func TestFilterToTrigramQuery_WithSegmentLookupSimulation(t *testing.T) {
	// Test the full flow: Query → Fingerprints → Segment Lookup → Final Segments
	// Query: OR(AND(log.level="error", resource.service.name contains "api"),
	//           AND(log.level="warn", resource.service.name contains "web"))
	//
	// Note: resource_service_name is a full-value dimension, so "contains" queries
	// only use the exists fingerprint (trigram search is not supported for full-value dims).
	// This means the segment filtering is less precise at the fingerprint level,
	// and the actual "contains" matching happens at query time.

	filter := BinaryClause{
		Op: "or",
		Clauses: []QueryClause{
			BinaryClause{
				Op: "and",
				Clauses: []QueryClause{
					Filter{K: "log.level", V: []string{"error"}, Op: "eq"},
					Filter{K: "resource.service.name", V: []string{"api"}, Op: "contains"},
				},
			},
			BinaryClause{
				Op: "and",
				Clauses: []QueryClause{
					Filter{K: "log.level", V: []string{"warn"}, Op: "eq"},
					Filter{K: "resource.service.name", V: []string{"web"}, Op: "contains"},
				},
			},
		},
	}

	// Step 1: Generate fingerprints and trigram query tree
	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})
	filterToTrigramQuery(filter, fps, &root)

	t.Logf("Generated %d fingerprints from query", len(fps))
	t.Log("Trigram query tree:")
	printTrigramTree(t, root, "")

	// Step 2: Compute expected fingerprints manually
	// For "error" - trigrams: "err", "rro", "ror"
	fpError1 := computeFingerprint("log_level", "err")
	fpError2 := computeFingerprint("log_level", "rro")
	fpError3 := computeFingerprint("log_level", "ror")

	// For "warn" - trigrams: "war", "arn"
	fpWarn1 := computeFingerprint("log_level", "war")
	fpWarn2 := computeFingerprint("log_level", "arn")

	// For resource_service_name "contains" - uses exists fingerprint since it's a full-value dimension
	fpServiceNameExists := computeFingerprint("resource_service_name", existsRegex)

	t.Logf("Expected fingerprints:")
	t.Logf("  error: %d, %d, %d", fpError1, fpError2, fpError3)
	t.Logf("  warn: %d, %d", fpWarn1, fpWarn2)
	t.Logf("  service_name exists: %d", fpServiceNameExists)

	// Step 3: Create mock segment data simulating database contents
	seg1 := SegmentInfo{SegmentID: 1, DateInt: 20250101, Hour: "00"}
	seg2 := SegmentInfo{SegmentID: 2, DateInt: 20250101, Hour: "00"}
	seg3 := SegmentInfo{SegmentID: 3, DateInt: 20250101, Hour: "00"}
	seg4 := SegmentInfo{SegmentID: 4, DateInt: 20250101, Hour: "00"}
	seg5 := SegmentInfo{SegmentID: 5, DateInt: 20250101, Hour: "00"}

	fpToSegments := make(map[int64][]SegmentInfo)

	// seg1: Has "error" AND service_name exists → should match first OR branch
	fpToSegments[fpError1] = append(fpToSegments[fpError1], seg1)
	fpToSegments[fpError2] = append(fpToSegments[fpError2], seg1)
	fpToSegments[fpError3] = append(fpToSegments[fpError3], seg1)
	fpToSegments[fpServiceNameExists] = append(fpToSegments[fpServiceNameExists], seg1)

	// seg2: Has "warn" AND service_name exists → should match second OR branch
	fpToSegments[fpWarn1] = append(fpToSegments[fpWarn1], seg2)
	fpToSegments[fpWarn2] = append(fpToSegments[fpWarn2], seg2)
	fpToSegments[fpServiceNameExists] = append(fpToSegments[fpServiceNameExists], seg2)

	// seg3: Has "error" but NO service_name → should NOT match (AND fails)
	fpToSegments[fpError1] = append(fpToSegments[fpError1], seg3)
	fpToSegments[fpError2] = append(fpToSegments[fpError2], seg3)
	fpToSegments[fpError3] = append(fpToSegments[fpError3], seg3)

	// seg4: Has both "error" AND "warn" AND service_name exists → should match both branches
	fpToSegments[fpError1] = append(fpToSegments[fpError1], seg4)
	fpToSegments[fpError2] = append(fpToSegments[fpError2], seg4)
	fpToSegments[fpError3] = append(fpToSegments[fpError3], seg4)
	fpToSegments[fpWarn1] = append(fpToSegments[fpWarn1], seg4)
	fpToSegments[fpWarn2] = append(fpToSegments[fpWarn2], seg4)
	fpToSegments[fpServiceNameExists] = append(fpToSegments[fpServiceNameExists], seg4)

	// seg5: Has "warn" but NO service_name → should NOT match (AND fails)
	fpToSegments[fpWarn1] = append(fpToSegments[fpWarn1], seg5)
	fpToSegments[fpWarn2] = append(fpToSegments[fpWarn2], seg5)

	// Step 4: Compute final segment set using the trigram query logic
	finalSegments := computeSegmentSet(root, fpToSegments)

	t.Logf("Final segments returned: %d", len(finalSegments))
	for seg := range finalSegments {
		t.Logf("  - segment %d", seg.SegmentID)
	}

	// Step 5: Verify results
	// Should match: seg1 (error+service_name), seg2 (warn+service_name), seg4 (both)
	// Should NOT match: seg3 (error but no service_name), seg5 (warn but no service_name)
	assert.Len(t, finalSegments, 3, "Should return exactly 3 segments")
	assert.Contains(t, finalSegments, seg1, "segment 1 should match (error AND service_name exists)")
	assert.Contains(t, finalSegments, seg2, "segment 2 should match (warn AND service_name exists)")
	assert.Contains(t, finalSegments, seg4, "segment 4 should match (both branches)")
	assert.NotContains(t, finalSegments, seg3, "segment 3 should NOT match (error but no service_name)")
	assert.NotContains(t, finalSegments, seg5, "segment 5 should NOT match (warn but no service_name)")
}

func TestFilterToTrigramQuery_ComplexOrSegmentSelection(t *testing.T) {
	// Test realistic scenario from the original issue:
	// Query files where (resource.file.type contains "cloudxcommand" OR log.source contains "cloudxcommand")
	// Since resource.file.type is NOT indexed, it should use exists check
	// Since log.source is NOT indexed, it should also use exists check

	filter := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{K: "resource.bucket.name", V: []string{"test-bucket"}, Op: "eq"},
			Filter{K: "resource.file", V: []string{"test-file_controller"}, Op: "in"},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{K: "resource.file.type", V: []string{"cloudxcommand"}, Op: "contains"},
					Filter{K: "log.source", V: []string{"cloudxcommand"}, Op: "contains"},
				},
			},
		},
	}

	// Step 1: Generate fingerprints
	root := &TrigramQuery{Op: index.QAll}
	fps := make(map[int64]struct{})
	filterToTrigramQuery(filter, fps, &root)

	t.Logf("Generated %d fingerprints", len(fps))
	t.Log("Trigram query tree:")
	printTrigramTree(t, root, "")

	// Step 2: Compute expected fingerprints
	// For full-value dimensions (bucket, file), we need both exists and actual value
	fpBucketExists := computeFingerprint("resource_bucket_name", existsRegex)
	fpBucketValue := computeFingerprint("resource_bucket_name", "test-bucket")
	fpFileExists := computeFingerprint("resource_file", existsRegex)
	fpFileValue := computeFingerprint("resource_file", "test-file_controller")
	fpFileTypeExists := computeFingerprint("resource_file_type", existsRegex)
	fpLogSourceExists := computeFingerprint("log_source", existsRegex)

	t.Logf("Expected fingerprints:")
	t.Logf("  bucket exists: %d, value: %d", fpBucketExists, fpBucketValue)
	t.Logf("  file exists: %d, value: %d", fpFileExists, fpFileValue)
	t.Logf("  file_type (exists): %d", fpFileTypeExists)
	t.Logf("  log_source (exists): %d", fpLogSourceExists)

	// Step 3: Create mock segments
	seg1 := SegmentInfo{SegmentID: 1, DateInt: 20250101, Hour: "00"}
	seg2 := SegmentInfo{SegmentID: 2, DateInt: 20250101, Hour: "00"}
	seg3 := SegmentInfo{SegmentID: 3, DateInt: 20250101, Hour: "00"}

	fpToSegments := make(map[int64][]SegmentInfo)

	// seg1: Has bucket, file, and file_type field exists → should match
	fpToSegments[fpBucketExists] = append(fpToSegments[fpBucketExists], seg1)
	fpToSegments[fpBucketValue] = append(fpToSegments[fpBucketValue], seg1)
	fpToSegments[fpFileExists] = append(fpToSegments[fpFileExists], seg1)
	fpToSegments[fpFileValue] = append(fpToSegments[fpFileValue], seg1)
	fpToSegments[fpFileTypeExists] = append(fpToSegments[fpFileTypeExists], seg1)

	// seg2: Has bucket, file, and log_source field exists → should match
	fpToSegments[fpBucketExists] = append(fpToSegments[fpBucketExists], seg2)
	fpToSegments[fpBucketValue] = append(fpToSegments[fpBucketValue], seg2)
	fpToSegments[fpFileExists] = append(fpToSegments[fpFileExists], seg2)
	fpToSegments[fpFileValue] = append(fpToSegments[fpFileValue], seg2)
	fpToSegments[fpLogSourceExists] = append(fpToSegments[fpLogSourceExists], seg2)

	// seg3: Has bucket and file, but neither file_type nor log_source exists → should NOT match
	fpToSegments[fpBucketExists] = append(fpToSegments[fpBucketExists], seg3)
	fpToSegments[fpBucketValue] = append(fpToSegments[fpBucketValue], seg3)
	fpToSegments[fpFileExists] = append(fpToSegments[fpFileExists], seg3)
	fpToSegments[fpFileValue] = append(fpToSegments[fpFileValue], seg3)

	// Step 4: Compute final segments
	finalSegments := computeSegmentSet(root, fpToSegments)

	t.Logf("Final segments returned: %d", len(finalSegments))
	for seg := range finalSegments {
		t.Logf("  - segment %d", seg.SegmentID)
	}

	// Step 5: Verify results
	// Should match: seg1 (has file_type field), seg2 (has log_source field)
	// Should NOT match: seg3 (has neither field)
	assert.Len(t, finalSegments, 2, "Should return exactly 2 segments")
	assert.Contains(t, finalSegments, seg1, "segment 1 should match (has file_type field)")
	assert.Contains(t, finalSegments, seg2, "segment 2 should match (has log_source field)")
	assert.NotContains(t, finalSegments, seg3, "segment 3 should NOT match (missing both OR fields)")
}
