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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// NOTE: These tests validate the filter translation logic for the /api/v1/tags/logs endpoint.
// They do NOT test:
// - The actual endpoint HTTP handler
// - That distinct tag VALUES are returned (not full records)
// - That the tagName parameter is correctly used
// - The SSE streaming behavior
//
// Full integration testing would require:
// - A real database with test data
// - Running query workers
// - Actual Parquet files to query
//
// What these tests DO validate:
// - BaseExpression JSON parsing
// - Filter structure parsing (nested AND/OR clauses)
// - LogQL translation from legacy filter format
// - That the filter is correctly converted to underscored label names

func TestHandleTagsQuery_ParsesFilterCorrectly(t *testing.T) {
	tests := []struct {
		name          string
		requestBody   BaseExpression
		expectedError bool
	}{
		{
			name: "simple eq filter",
			requestBody: BaseExpression{
				Dataset: "logs",
				Limit:   100,
				Order:   "DESC",
				Filter: Filter{
					K:  "resource.bucket.name",
					V:  []string{"my-bucket"},
					Op: "eq",
				},
			},
			expectedError: false,
		},
		{
			name: "complex nested filter",
			requestBody: BaseExpression{
				Dataset: "logs",
				Limit:   1000,
				Order:   "DESC",
				Filter: BinaryClause{
					Op: "and",
					Clauses: []QueryClause{
						Filter{
							K:  "resource.bucket.name",
							V:  []string{"avxit-dev-s3-use2-datalake"},
							Op: "eq",
						},
						Filter{
							K:  "resource.file",
							V:  []string{"mtb.com-abu-8aaf3300-1675268762.79_2025-10-23-113701_controller"},
							Op: "in",
						},
					},
				},
			},
			expectedError: false,
		},
		{
			name: "filter with contains in nested or clause",
			requestBody: BaseExpression{
				Dataset: "logs",
				Limit:   1000,
				Filter: BinaryClause{
					Op: "and",
					Clauses: []QueryClause{
						Filter{
							K:  "resource.bucket.name",
							V:  []string{"my-bucket"},
							Op: "eq",
						},
						BinaryClause{
							Op: "or",
							Clauses: []QueryClause{
								Filter{
									K:  "_cardinalhq.message",
									V:  []string{"cloudxcommands"},
									Op: "contains",
								},
								Filter{
									K:  "log.log_level",
									V:  []string{"cloudxcommands"},
									Op: "contains",
								},
							},
						},
					},
				},
			},
			expectedError: true, // OR is not supported yet
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Note: We can't fully test the endpoint without a real database,
			// but we can verify that the request parsing and LogQL translation works.
			logql, _, err := TranslateToLogQL(tt.requestBody)

			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotEmpty(t, logql)
				t.Logf("Generated LogQL: %s", logql)
			}
		})
	}
}

func TestHandleTagsQuery_RequestBodyParsing(t *testing.T) {
	// Test that the BaseExpression unmarshaling works correctly with complex nested filters
	jsonBody := `{
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
				"v": ["mtb.com-abu-8aaf3300-1675268762.79_2025-10-23-113701_controller"],
				"op": "in",
				"dataType": "string",
				"extracted": false,
				"computed": false
			},
			"op": "and"
		}
	}`

	var req BaseExpression
	err := json.Unmarshal([]byte(jsonBody), &req)
	require.NoError(t, err)

	assert.Equal(t, "logs", req.Dataset)
	assert.Equal(t, 1000, req.Limit)
	assert.Equal(t, "DESC", req.Order)
	assert.True(t, req.ReturnResults)

	// Verify filter structure
	bc, ok := req.Filter.(BinaryClause)
	require.True(t, ok, "expected BinaryClause")
	assert.Equal(t, "and", bc.Op)
	require.Len(t, bc.Clauses, 2)

	// Verify Clause 1
	f1, ok := bc.Clauses[0].(Filter)
	require.True(t, ok, "expected Filter for Clause 1")
	assert.Equal(t, "resource.bucket.name", f1.K)
	assert.Equal(t, []string{"avxit-dev-s3-use2-datalake"}, f1.V)
	assert.Equal(t, "eq", f1.Op)

	// Verify Clause 2
	f2, ok := bc.Clauses[1].(Filter)
	require.True(t, ok, "expected Filter for Clause 2")
	assert.Equal(t, "resource.file", f2.K)
	assert.Equal(t, []string{"mtb.com-abu-8aaf3300-1675268762.79_2025-10-23-113701_controller"}, f2.V)
	assert.Equal(t, "in", f2.Op)

	// Translate to LogQL
	logql, _, err := TranslateToLogQL(req)
	require.NoError(t, err)
	assert.Contains(t, logql, "resource_bucket_name")
	assert.Contains(t, logql, "resource_file")
	t.Logf("Generated LogQL: %s", logql)
}
