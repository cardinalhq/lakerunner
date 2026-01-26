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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphRequest_UnmarshalJSON_CustomerIssue(t *testing.T) {
	// This reproduces the exact customer request that was experiencing the filter bug
	jsonData := `{
		"baseExpressions": {
			"a": {
				"dataset": "logs",
				"limit": 1000,
				"order": "DESC",
				"returnResults": true,
				"filter": {
					"q1": {
						"k": "resource.bucket.name",
						"v": ["avxit-dev-s3-use2-datalake"],
						"op": "eq",
						"dataType": "string",
						"extracted": false,
						"computed": false
					},
					"q2": {
						"k": "resource.file",
						"v": ["vitechinc.com-abu-hirw8pmdunp-1736355841.4503462_2025-10-23-193952_dr-client-ingress-psf-gw"],
						"op": "in",
						"dataType": "string",
						"extracted": false,
						"computed": false
					},
					"q3": {
						"k": "resource.file.type",
						"v": ["avxgwstatesync"],
						"op": "in",
						"dataType": "string",
						"extracted": false,
						"computed": false
					},
					"op": "and"
				}
			}
		}
	}`

	var req GraphRequest
	err := json.Unmarshal([]byte(jsonData), &req)
	require.NoError(t, err, "Failed to unmarshal GraphRequest")

	// Verify we have one base expression
	require.Len(t, req.BaseExpressions, 1)
	baseExpr, ok := req.BaseExpressions["a"]
	require.True(t, ok, "Missing base expression 'a'")

	// Verify base expression properties
	assert.Equal(t, "logs", baseExpr.Dataset)
	assert.Equal(t, 1000, baseExpr.Limit)
	assert.Equal(t, "DESC", baseExpr.Order)
	assert.True(t, baseExpr.ReturnResults)

	// Verify the filter is a BinaryClause
	rootClause, ok := baseExpr.Filter.(BinaryClause)
	require.True(t, ok, "Root filter should be a BinaryClause")
	assert.Equal(t, "and", rootClause.Op)

	// Verify we have exactly 3 clauses (q1, q2, q3)
	require.Len(t, rootClause.Clauses, 3, "Should have 3 sub-clauses")

	// Verify clause 1: resource.bucket.name filter
	clause1, ok := rootClause.Clauses[0].(Filter)
	require.True(t, ok, "Clause 1 should be a Filter")
	assert.Equal(t, "resource.bucket.name", clause1.K)
	assert.Equal(t, []string{"avxit-dev-s3-use2-datalake"}, clause1.V)
	assert.Equal(t, "eq", clause1.Op)

	// Verify clause 2: resource.file filter
	clause2, ok := rootClause.Clauses[1].(Filter)
	require.True(t, ok, "Clause 2 should be a Filter")
	assert.Equal(t, "resource.file", clause2.K)
	assert.Equal(t, []string{"vitechinc.com-abu-hirw8pmdunp-1736355841.4503462_2025-10-23-193952_dr-client-ingress-psf-gw"}, clause2.V)
	assert.Equal(t, "in", clause2.Op)

	// Verify clause 3: resource.file.type filter (THE CRITICAL ONE!)
	clause3, ok := rootClause.Clauses[2].(Filter)
	require.True(t, ok, "Clause 3 should be a Filter")
	assert.Equal(t, "resource.file.type", clause3.K, "resource.file.type filter must be parsed")
	assert.Equal(t, []string{"avxgwstatesync"}, clause3.V)
	assert.Equal(t, "in", clause3.Op)
}

func TestBaseExpression_UnmarshalJSON_SimpleFilter(t *testing.T) {
	jsonData := `{
		"dataset": "logs",
		"limit": 100,
		"order": "ASC",
		"returnResults": true,
		"filter": {
			"k": "log.level",
			"v": ["error"],
			"op": "eq",
			"dataType": "string",
			"extracted": false,
			"computed": false
		}
	}`

	var baseExpr BaseExpression
	err := json.Unmarshal([]byte(jsonData), &baseExpr)
	require.NoError(t, err)

	assert.Equal(t, "logs", baseExpr.Dataset)
	assert.Equal(t, 100, baseExpr.Limit)

	filter, ok := baseExpr.Filter.(Filter)
	require.True(t, ok)
	assert.Equal(t, "log.level", filter.K)
	assert.Equal(t, []string{"error"}, filter.V)
	assert.Equal(t, "eq", filter.Op)
}

func TestBaseExpression_UnmarshalJSON_BinaryClauseAnd(t *testing.T) {
	jsonData := `{
		"dataset": "logs",
		"limit": 100,
		"order": "DESC",
		"returnResults": true,
		"filter": {
			"q1": {
				"k": "resource.service.name",
				"v": ["my-service"],
				"op": "eq",
				"dataType": "string",
				"extracted": false,
				"computed": false
			},
			"q2": {
				"k": "log.level",
				"v": ["error", "warn"],
				"op": "in",
				"dataType": "string",
				"extracted": false,
				"computed": false
			},
			"op": "and"
		}
	}`

	var baseExpr BaseExpression
	err := json.Unmarshal([]byte(jsonData), &baseExpr)
	require.NoError(t, err)

	binaryClause, ok := baseExpr.Filter.(BinaryClause)
	require.True(t, ok)
	assert.Equal(t, "and", binaryClause.Op)
	require.Len(t, binaryClause.Clauses, 2)

	q1, ok := binaryClause.Clauses[0].(Filter)
	require.True(t, ok)
	assert.Equal(t, "resource.service.name", q1.K)
	assert.Equal(t, "eq", q1.Op)

	q2, ok := binaryClause.Clauses[1].(Filter)
	require.True(t, ok)
	assert.Equal(t, "log.level", q2.K)
	assert.Equal(t, "in", q2.Op)
	assert.Equal(t, []string{"error", "warn"}, q2.V)
}

func TestBaseExpression_UnmarshalJSON_NestedBinaryClauses(t *testing.T) {
	// Test nested AND clauses: (A AND (B AND C))
	jsonData := `{
		"dataset": "logs",
		"limit": 500,
		"order": "DESC",
		"returnResults": true,
		"filter": {
			"q1": {
				"k": "field_a",
				"v": ["value_a"],
				"op": "eq",
				"dataType": "string",
				"extracted": false,
				"computed": false
			},
			"q2": {
				"q1": {
					"k": "field_b",
					"v": ["value_b"],
					"op": "eq",
					"dataType": "string",
					"extracted": false,
					"computed": false
				},
				"q2": {
					"k": "field_c",
					"v": ["value_c"],
					"op": "eq",
					"dataType": "string",
					"extracted": false,
					"computed": false
				},
				"op": "and"
			},
			"op": "and"
		}
	}`

	var baseExpr BaseExpression
	err := json.Unmarshal([]byte(jsonData), &baseExpr)
	require.NoError(t, err)

	// Root is BinaryClause
	rootClause, ok := baseExpr.Filter.(BinaryClause)
	require.True(t, ok)
	assert.Equal(t, "and", rootClause.Op)
	require.Len(t, rootClause.Clauses, 2)

	// Clause 1 is a simple Filter
	q1Filter, ok := rootClause.Clauses[0].(Filter)
	require.True(t, ok)
	assert.Equal(t, "field_a", q1Filter.K)

	// Clause 2 is another BinaryClause
	q2Clause, ok := rootClause.Clauses[1].(BinaryClause)
	require.True(t, ok)
	assert.Equal(t, "and", q2Clause.Op)
	require.Len(t, q2Clause.Clauses, 2)

	// Q2 Clause 1 is Filter for field_b
	q2q1Filter, ok := q2Clause.Clauses[0].(Filter)
	require.True(t, ok)
	assert.Equal(t, "field_b", q2q1Filter.K)

	// Q2 Clause 2 is Filter for field_c
	q2q2Filter, ok := q2Clause.Clauses[1].(Filter)
	require.True(t, ok)
	assert.Equal(t, "field_c", q2q2Filter.K)
}

func TestUnmarshalQueryClause_InvalidJSON(t *testing.T) {
	invalidJSON := []byte(`{"invalid": "clause"}`)
	_, err := unmarshalQueryClause(invalidJSON)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown QueryClause type")
}

func TestFilter_AllOperators(t *testing.T) {
	operators := []struct {
		op       string
		values   []string
		dataType string
	}{
		{"eq", []string{"value1"}, "string"},
		{"in", []string{"value1", "value2", "value3"}, "string"},
		{"contains", []string{"substring"}, "string"},
		{"regex", []string{"^pattern.*"}, "string"},
		{"gt", []string{"100"}, "number"},
		{"gte", []string{"100"}, "number"},
		{"lt", []string{"100"}, "number"},
		{"lte", []string{"100"}, "number"},
	}

	for _, tc := range operators {
		t.Run(tc.op, func(t *testing.T) {
			valuesJSON, _ := json.Marshal(tc.values)
			jsonData := `{
				"dataset": "logs",
				"limit": 100,
				"order": "DESC",
				"returnResults": true,
				"filter": {
					"k": "test.field",
					"v": ` + string(valuesJSON) + `,
					"op": "` + tc.op + `",
					"dataType": "` + tc.dataType + `",
					"extracted": false,
					"computed": false
				}
			}`

			var baseExpr BaseExpression
			err := json.Unmarshal([]byte(jsonData), &baseExpr)
			require.NoError(t, err)

			filter, ok := baseExpr.Filter.(Filter)
			require.True(t, ok)
			assert.Equal(t, tc.op, filter.Op)
			assert.Equal(t, tc.values, filter.V)
			assert.Equal(t, tc.dataType, filter.DataType)
		})
	}
}

func TestBinaryClause_NonNumericKeys(t *testing.T) {
	// Test parsing BinaryClause with non-numeric keys like "qs3", "qsh1", etc.
	// This is the actual pattern used in production queries
	jsonData := []byte(`{
		"q1": {
			"k": "field1",
			"v": ["value1"],
			"op": "eq",
			"dataType": "string",
			"extracted": false,
			"computed": false
		},
		"q2": {
			"k": "field2",
			"v": ["value2"],
			"op": "eq",
			"dataType": "string",
			"extracted": false,
			"computed": false
		},
		"qs3": {
			"qsh1": {
				"k": "field3",
				"v": ["value3"],
				"op": "contains",
				"dataType": "string",
				"extracted": false,
				"computed": false
			},
			"qsh2": {
				"k": "field4",
				"v": ["value4"],
				"op": "contains",
				"dataType": "string",
				"extracted": false,
				"computed": false
			},
			"op": "or"
		},
		"op": "and"
	}`)

	var bc BinaryClause
	err := json.Unmarshal(jsonData, &bc)
	require.NoError(t, err)

	// Should have 3 clauses: q1, q2, and qs3
	assert.Equal(t, "and", bc.Op)
	assert.Equal(t, 3, len(bc.Clauses), "Should parse all 3 clauses including qs3")

	// First two should be Filters
	filter1, ok := bc.Clauses[0].(Filter)
	require.True(t, ok)
	assert.Equal(t, "field1", filter1.K)

	filter2, ok := bc.Clauses[1].(Filter)
	require.True(t, ok)
	assert.Equal(t, "field2", filter2.K)

	// Third should be a nested BinaryClause (the OR clause)
	nestedBC, ok := bc.Clauses[2].(BinaryClause)
	require.True(t, ok, "qs3 should be parsed as a BinaryClause")
	assert.Equal(t, "or", nestedBC.Op)
	assert.Equal(t, 2, len(nestedBC.Clauses), "qs3 should have 2 sub-clauses")

	// Verify the nested clauses
	nestedFilter1, ok := nestedBC.Clauses[0].(Filter)
	require.True(t, ok)
	assert.Equal(t, "field3", nestedFilter1.K)

	nestedFilter2, ok := nestedBC.Clauses[1].(Filter)
	require.True(t, ok)
	assert.Equal(t, "field4", nestedFilter2.K)
}
