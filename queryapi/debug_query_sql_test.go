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
	"fmt"
	"testing"
)

// TestDebugActualQuerySQL generates the SQL for the actual production query to debug
func TestDebugActualQuerySQL(t *testing.T) {
	// Build the exact query from the curl command
	filter := BinaryClause{
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

	leaf := &LegacyLeaf{Filter: filter}
	sql := leaf.ToWorkerSQLWithLimit(1000, "DESC", nil)

	separator := "================================================================================"
	fmt.Println("\n" + separator)
	fmt.Println("GENERATED SQL FOR PRODUCTION QUERY:")
	fmt.Println(separator)
	fmt.Println(sql)
	fmt.Println(separator + "\n")

	// Check that SQL contains the expected patterns
	if sql == "" {
		t.Fatal("Generated SQL is empty")
	}

	// Verify contains operations use case-insensitive regex
	if !contains(sql, "REGEXP_MATCHES") {
		t.Error("SQL should use REGEXP_MATCHES for contains operator")
	}

	if !contains(sql, "'i'") {
		t.Error("SQL should use case-insensitive flag 'i'")
	}

	// Verify OR clause
	if !contains(sql, " OR ") {
		t.Error("SQL should contain OR operator")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && findSubstring(s, substr) >= 0
}

func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
