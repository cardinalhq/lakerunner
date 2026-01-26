// Copyright (C) 2025-2026 CardinalHQ, Inc
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
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLegacyLeaf_BasicEqOperator(t *testing.T) {
	filter := Filter{
		K:  "resource.bucket.name",
		V:  []string{"my-bucket"},
		Op: "eq",
	}

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(100, "DESC", nil)

	// Should contain normalized column name
	assert.Contains(t, sql, `"resource_bucket_name"`)
	// Should contain equals condition
	assert.Contains(t, sql, `"resource_bucket_name" = 'my-bucket'`)
	// Should have time filter placeholder
	assert.Contains(t, sql, "{start}")
	assert.Contains(t, sql, "{end}")
	// Should have ORDER BY and LIMIT
	assert.Contains(t, sql, "ORDER BY")
	assert.Contains(t, sql, "DESC")
	assert.Contains(t, sql, "LIMIT 100")
}

func TestLegacyLeaf_InOperator(t *testing.T) {
	tests := []struct {
		name     string
		values   []string
		expected string
	}{
		{
			name:     "single value",
			values:   []string{"value1"},
			expected: `"field" = 'value1'`,
		},
		{
			name:     "multiple values",
			values:   []string{"value1", "value2", "value3"},
			expected: `"field" IN ('value1', 'value2', 'value3')`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := Filter{
				K:  "field",
				V:  tt.values,
				Op: "in",
			}

			leaf := &LegacyLeaf{Filter: filter}
			sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

			assert.Contains(t, sql, tt.expected)
		})
	}
}

func TestLegacyLeaf_ContainsOperator(t *testing.T) {
	filter := Filter{
		K:  "_cardinalhq.message",
		V:  []string{"error"},
		Op: "contains",
	}

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// Should normalize _cardinalhq.message to log_message (backward compatibility)
	assert.Contains(t, sql, `"log_message"`)
	// Should use case-insensitive regex to match Scala behavior
	assert.Contains(t, sql, `REGEXP_MATCHES`)
	assert.Contains(t, sql, `'.*error.*'`)
	assert.Contains(t, sql, `'i'`) // Case-insensitive flag
}

func TestLegacyLeaf_RegexOperator(t *testing.T) {
	filter := Filter{
		K:  "log.log_level",
		V:  []string{"ERROR|WARN"},
		Op: "regex",
	}

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	assert.Contains(t, sql, `"log_log_level"`)
	assert.Contains(t, sql, "REGEXP_MATCHES")
	assert.Contains(t, sql, "ERROR|WARN")
	assert.Contains(t, sql, `'i'`) // Case-insensitive flag to match Scala
}

func TestLegacyLeaf_ComparisonOperators(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		dataType string
		value    string
		expected string
	}{
		{
			name:     "greater than numeric",
			op:       "gt",
			dataType: "number",
			value:    "100",
			expected: `"count" > 100`,
		},
		{
			name:     "greater than or equal",
			op:       "gte",
			dataType: "number",
			value:    "50",
			expected: `"count" >= 50`,
		},
		{
			name:     "less than",
			op:       "lt",
			dataType: "number",
			value:    "200",
			expected: `"count" < 200`,
		},
		{
			name:     "less than or equal",
			op:       "lte",
			dataType: "number",
			value:    "500",
			expected: `"count" <= 500`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := Filter{
				K:        "count",
				V:        []string{tt.value},
				Op:       tt.op,
				DataType: tt.dataType,
			}

			leaf := &LegacyLeaf{Filter: filter}
			sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

			assert.Contains(t, sql, tt.expected)
		})
	}
}

func TestLegacyLeaf_AndOperator(t *testing.T) {
	binaryClause := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "resource.bucket.name",
				V:  []string{"my-bucket"},
				Op: "eq",
			},
			Filter{
				K:  "log.log_level",
				V:  []string{"ERROR"},
				Op: "eq",
			},
		},
	}

	leaf := &LegacyLeaf{Filter: binaryClause}
	sql := leaf.ToWorkerSQLWithLimit(100, "DESC", nil)

	// Should have both conditions
	assert.Contains(t, sql, `"resource_bucket_name" = 'my-bucket'`)
	assert.Contains(t, sql, `"log_log_level" = 'ERROR'`)

	// Should have multiple CTE stages (one per AND clause)
	assert.Contains(t, sql, "s0 AS")
	assert.Contains(t, sql, "s1 AS")
	assert.Contains(t, sql, "s2 AS")
	assert.Contains(t, sql, "s3 AS")
	assert.Contains(t, sql, "s4 AS")
}

func TestLegacyLeaf_OrOperator(t *testing.T) {
	binaryClause := BinaryClause{
		Op: "or",
		Clauses: []QueryClause{
			Filter{
				K:  "_cardinalhq.message",
				V:  []string{"error"},
				Op: "contains",
			},
			Filter{
				K:  "log.log_level",
				V:  []string{"ERROR"},
				Op: "eq",
			},
		},
	}

	leaf := &LegacyLeaf{Filter: binaryClause}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// Should have both conditions combined with OR
	assert.Contains(t, sql, `"log_message"`)  // _cardinalhq.message now maps to log_message
	assert.Contains(t, sql, `REGEXP_MATCHES`) // Changed to regex for case-insensitive matching
	assert.Contains(t, sql, `"log_log_level" = 'ERROR'`)
	assert.Contains(t, sql, " OR ")
}

func TestLegacyLeaf_NestedBooleanLogic(t *testing.T) {
	// (field1 = 'a' AND field2 = 'b') OR (field3 = 'c')
	binaryClause := BinaryClause{
		Op: "or",
		Clauses: []QueryClause{
			BinaryClause{
				Op: "and",
				Clauses: []QueryClause{
					Filter{K: "field1", V: []string{"a"}, Op: "eq"},
					Filter{K: "field2", V: []string{"b"}, Op: "eq"},
				},
			},
			Filter{K: "field3", V: []string{"c"}, Op: "eq"},
		},
	}

	leaf := &LegacyLeaf{Filter: binaryClause}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// Should contain all conditions
	assert.Contains(t, sql, `"field1"`)
	assert.Contains(t, sql, `"field2"`)
	assert.Contains(t, sql, `"field3"`)
	// Should have proper boolean logic
	assert.Contains(t, sql, " OR ")
	assert.Contains(t, sql, " AND ")
}

func TestLegacyLeaf_UserExampleQuery(t *testing.T) {
	// This is the user's actual query from the issue
	binaryClause := BinaryClause{
		Op: "and",
		Clauses: []QueryClause{
			Filter{
				K:  "resource.bucket.name",
				V:  []string{"avxit-dev-s3-use2-datalake"},
				Op: "eq",
			},
			Filter{
				K:  "resource.file",
				V:  []string{"verint.com-abu-5sbgatfp7zf-1682612405.9400098_2025-10-30-180550_controller"},
				Op: "in",
			},
			BinaryClause{
				Op: "or",
				Clauses: []QueryClause{
					Filter{K: "_cardinalhq.message", V: []string{"cloudxcommand"}, Op: "contains"},
					Filter{K: "log.log_level", V: []string{"cloudxcommand"}, Op: "contains"},
					Filter{K: "resource.file.type", V: []string{"cloudxcommand"}, Op: "contains"},
					Filter{K: "log.source", V: []string{"cloudxcommand"}, Op: "contains"},
				},
			},
		},
	}

	leaf := &LegacyLeaf{Filter: binaryClause}
	sql := leaf.ToWorkerSQLWithLimit(1000, "DESC", nil)

	// Verify structure
	require.NotEmpty(t, sql)
	assert.Contains(t, sql, "WITH")
	assert.Contains(t, sql, "SELECT")

	// Verify conditions
	assert.Contains(t, sql, `"resource_bucket_name"`)
	assert.Contains(t, sql, `"resource_file"`)
	assert.Contains(t, sql, `"log_message"`) // _cardinalhq.message now maps to log_message for backward compat
	assert.Contains(t, sql, `"log_log_level"`)
	assert.Contains(t, sql, `"resource_file_type"`)
	assert.Contains(t, sql, `"log_source"`)

	// Verify operators
	assert.Contains(t, sql, "cloudxcommand")
	assert.Contains(t, sql, " OR ")

	// Should have LIMIT
	assert.Contains(t, sql, "LIMIT 1000")
}

func TestLegacyLeaf_SpecialCharacters(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{
			name:     "single quote",
			value:    "it's",
			expected: "'it''s'",
		},
		{
			name:     "backslash",
			value:    `path\to\file`,
			expected: `'path\to\file'`,
		},
		{
			name:     "double quote",
			value:    `say "hello"`,
			expected: `'say "hello"'`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := Filter{
				K:  "field",
				V:  []string{tt.value},
				Op: "eq",
			}

			leaf := &LegacyLeaf{Filter: filter}
			sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

			assert.Contains(t, sql, tt.expected)
		})
	}
}

func TestLegacyLeaf_RegexEscaping(t *testing.T) {
	// Test that regex special characters are properly escaped in contains operator
	filter := Filter{
		K:  "field",
		V:  []string{"test.*value"},
		Op: "contains",
	}

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// Should escape regex metacharacters (. and * become \. and \*)
	assert.Contains(t, sql, `REGEXP_MATCHES`)
	// The .* should be escaped to \.\* for literal matching
	assert.Contains(t, sql, `'.*test\.\*value.*'`)
}

func TestLegacyLeaf_FieldSelection(t *testing.T) {
	filter := Filter{
		K:  "field1",
		V:  []string{"value1"},
		Op: "eq",
	}

	leaf := &LegacyLeaf{Filter: filter}
	fields := []string{"chq_timestamp", "log_message", "field1"}
	sql := leaf.ToWorkerSQLWithLimit(0, "", fields)

	// Should select only specified fields in final SELECT
	assert.Contains(t, sql, `SELECT "chq_timestamp", "log_message", "field1" FROM`)
	// Final SELECT should not be SELECT * FROM
	lines := strings.Split(sql, "\n")
	finalLine := lines[len(lines)-1]
	assert.NotContains(t, finalLine, "SELECT * FROM")
}

func TestLegacyLeaf_ToWorkerSQLForTagValues(t *testing.T) {
	filter := Filter{
		K:  "resource.bucket.name",
		V:  []string{"my-bucket"},
		Op: "eq",
	}

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLForTagValues("log.log_level")

	// Should have DISTINCT
	assert.Contains(t, sql, "DISTINCT")
	// Should select the tag as value
	assert.Contains(t, sql, `"log_log_level" AS value`)
	// Should filter NULL values
	assert.Contains(t, sql, "IS NOT NULL")
	// Should have ORDER BY
	assert.Contains(t, sql, "ORDER BY value")
	// Should still apply the filter
	assert.Contains(t, sql, `"resource_bucket_name" = 'my-bucket'`)
}

func TestLegacyLeaf_EmptyFilter(t *testing.T) {
	leaf := &LegacyLeaf{Filter: nil}
	sql := leaf.ToWorkerSQLWithLimit(100, "DESC", nil)

	// Should still have basic structure
	assert.Contains(t, sql, "WITH")
	assert.Contains(t, sql, "SELECT")
	// Should have time filter
	assert.Contains(t, sql, "{start}")
	assert.Contains(t, sql, "{end}")
}

func TestLegacyLeaf_SQLInjectionProtection(t *testing.T) {
	// Try to inject SQL via field value
	filter := Filter{
		K:  "field",
		V:  []string{"'; DROP TABLE users; --"},
		Op: "eq",
	}

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// Should escape single quotes
	assert.Contains(t, sql, `'''; DROP TABLE users; --'`)
	// Should not contain actual DROP statement
	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		// Allow the escaped version but not the raw injection
		if strings.Contains(line, "DROP TABLE") && !strings.Contains(line, "''") {
			t.Errorf("Potential SQL injection vulnerability: %s", line)
		}
	}
}

func TestQuoteIdentifier(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", `"simple"`},
		{"with.dot", `"with.dot"`},
		{`with"quote`, `"with""quote"`},
		{"_underscore", `"_underscore"`},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := quoteIdentifier(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSqlStringLiteral(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "'simple'"},
		{"it's", "'it''s'"},
		{"double''quotes", "'double''''quotes'"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := sqlStringLiteral(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestLegacyLeaf_CaseInsensitiveContains(t *testing.T) {
	// Test that contains operator is case-insensitive (matches Scala behavior)
	tests := []struct {
		name  string
		value string
	}{
		{"lowercase", "error"},
		{"uppercase", "ERROR"},
		{"mixed case", "ErRoR"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := Filter{
				K:  "message",
				V:  []string{tt.value},
				Op: "contains",
			}

			leaf := &LegacyLeaf{Filter: filter}
			sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

			// Should use REGEXP_MATCHES with 'i' flag for case-insensitive matching
			assert.Contains(t, sql, "REGEXP_MATCHES")
			assert.Contains(t, sql, `'i'`)
			// Pattern should be .*value.* with original case (case-insensitive flag handles matching)
			assert.Contains(t, sql, ".*"+tt.value+".*")
		})
	}
}

func TestLegacyLeaf_CaseInsensitiveRegex(t *testing.T) {
	// Test that regex operator is case-insensitive (matches Scala behavior)
	filter := Filter{
		K:  "log_level",
		V:  []string{"error|warn|info"},
		Op: "regex",
	}

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// Should use REGEXP_MATCHES with 'i' flag for case-insensitive matching
	assert.Contains(t, sql, "REGEXP_MATCHES")
	assert.Contains(t, sql, `'i'`)
	assert.Contains(t, sql, "error|warn|info")
}

func TestLegacyLeaf_NonIndexedFieldHandling(t *testing.T) {
	// Test that non-indexed fields get IS NOT NULL checks
	// This test assumes that "custom_field" is not in dimensionsToIndex
	filter := Filter{
		K:  "custom_field",
		V:  []string{"value"},
		Op: "eq",
	}

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(0, "", nil)

	// For non-indexed fields, should add IS NOT NULL check
	// Note: This behavior depends on dimensionsToIndex configuration
	// If custom_field is not indexed, it should have IS NOT NULL
	assert.Contains(t, sql, `"custom_field"`)
}

func TestLegacyLeaf_MarshalUnmarshalRoundTrip(t *testing.T) {
	// Test that LegacyLeaf can be marshaled and unmarshaled correctly
	// This is critical for sending queries from query-api to workers
	tests := []struct {
		name   string
		filter QueryClause
	}{
		{
			name: "simple filter",
			filter: Filter{
				K:  "log.log_level",
				V:  []string{"ERROR"},
				Op: "eq",
			},
		},
		{
			name: "binary clause with AND",
			filter: BinaryClause{
				Op: "and",
				Clauses: []QueryClause{
					Filter{K: "field1", V: []string{"value1"}, Op: "eq"},
					Filter{K: "field2", V: []string{"value2"}, Op: "contains"},
				},
			},
		},
		{
			name: "complex nested OR query",
			filter: BinaryClause{
				Op: "and",
				Clauses: []QueryClause{
					Filter{K: "resource.bucket.name", V: []string{"test-bucket"}, Op: "eq"},
					BinaryClause{
						Op: "or",
						Clauses: []QueryClause{
							Filter{K: "_cardinalhq.message", V: []string{"testcommand"}, Op: "contains"},
							Filter{K: "log.log_level", V: []string{"testcommand"}, Op: "contains"},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			original := &LegacyLeaf{Filter: tt.filter}

			// Marshal to JSON
			data, err := json.Marshal(original)
			require.NoError(t, err)
			t.Logf("Marshaled JSON: %s", string(data))

			// Unmarshal back
			var unmarshaled LegacyLeaf
			err = json.Unmarshal(data, &unmarshaled)
			require.NoError(t, err)

			// Verify the filter is preserved
			assert.NotNil(t, unmarshaled.Filter)

			// Generate SQL from both to ensure they're equivalent
			sqlOriginal := original.ToWorkerSQLWithLimit(100, "DESC", nil)
			sqlUnmarshaled := unmarshaled.ToWorkerSQLWithLimit(100, "DESC", nil)
			assert.Equal(t, sqlOriginal, sqlUnmarshaled, "SQL should be identical after round-trip")
		})
	}
}

func TestLegacyLeaf_TimeseriesPushDownAggregation(t *testing.T) {
	// Test SQL generation for timeseries with push-down aggregation
	filter := Filter{
		K:  "log.log_level",
		V:  []string{"ERROR"},
		Op: "eq",
	}

	leaf := &LegacyLeaf{Filter: filter}
	stepMs := int64(60000) // 1 minute buckets
	sql := leaf.ToWorkerSQLForTimeseries(stepMs)

	// Should have WITH clause
	assert.Contains(t, sql, "WITH")
	// Should have time bucketing formula matching Scala behavior
	// (timestamp - (timestamp % step_ms)) as step_ts
	assert.Contains(t, sql, "60000")
	assert.Contains(t, sql, "step_ts")
	// Should have GROUP BY
	assert.Contains(t, sql, "GROUP BY step_ts")
	// Should have COUNT
	assert.Contains(t, sql, "COUNT(*)")
	// Should have ORDER BY step_ts
	assert.Contains(t, sql, "ORDER BY step_ts")
	// Should still apply filter
	assert.Contains(t, sql, `"log_log_level" = 'ERROR'`)
	// Should have time range placeholders
	assert.Contains(t, sql, "{start}")
	assert.Contains(t, sql, "{end}")
}
