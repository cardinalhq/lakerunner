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
	"fmt"
	"slices"
	"strings"

	"github.com/cardinalhq/lakerunner/internal/fingerprint"
)

// LegacyLeaf represents a compiled legacy query filter that can be pushed down to workers.
// It contains the filter tree and can generate DuckDB SQL directly without going through LogQL.
type LegacyLeaf struct {
	Filter QueryClause `json:"filter"`
}

// UnmarshalJSON implements custom JSON unmarshaling for LegacyLeaf to handle the QueryClause interface.
func (ll *LegacyLeaf) UnmarshalJSON(data []byte) error {
	type Alias LegacyLeaf
	aux := &struct {
		Filter json.RawMessage `json:"filter"`
		*Alias
	}{
		Alias: (*Alias)(ll),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	clause, err := unmarshalQueryClause(aux.Filter)
	if err != nil {
		return err
	}
	ll.Filter = clause

	return nil
}

// MarshalJSON implements custom JSON marshaling for LegacyLeaf to handle the QueryClause interface.
// The QueryClause interface will be marshaled as its concrete type (Filter or BinaryClause),
// and workers will use unmarshalQueryClause to determine the type based on the fields present.
func (ll *LegacyLeaf) MarshalJSON() ([]byte, error) {
	// Marshal the filter field using its concrete type's JSON representation
	filterJSON, err := json.Marshal(ll.Filter)
	if err != nil {
		return nil, err
	}

	// Build the complete JSON structure
	return json.Marshal(map[string]json.RawMessage{
		"filter": filterJSON,
	})
}

// ToWorkerSQLForTimeseries generates DuckDB SQL for timeseries aggregation with push-down.
// This matches Scala's behavior by doing time bucketing and counting in SQL rather than client-side.
// stepMs is the time bucket size in milliseconds.
func (ll *LegacyLeaf) ToWorkerSQLForTimeseries(stepMs int64) string {
	const baseRel = "{table}"
	const tsCol = "\"chq_timestamp\""

	// Build CTE pipeline similar to regular queries
	var ctes []string
	stageNum := 0

	// Stage 0: SELECT * from base relation
	ctes = append(ctes, fmt.Sprintf("s%d AS (SELECT * FROM %s)", stageNum, baseRel))
	stageNum++

	// Stage 1: Normalize fingerprint type to string
	prevStage := fmt.Sprintf("s%d", stageNum-1)
	ctes = append(ctes, fmt.Sprintf(
		`s%d AS (SELECT %s.* REPLACE(CAST("chq_fingerprint" AS VARCHAR) AS "chq_fingerprint") FROM %s)`,
		stageNum, prevStage, prevStage,
	))
	stageNum++

	// Stage 2: Time window filter
	prevStage = fmt.Sprintf("s%d", stageNum-1)
	timePred := fmt.Sprintf("CAST(%s AS BIGINT) >= {start} AND CAST(%s AS BIGINT) < {end}", tsCol, tsCol)
	ctes = append(ctes, fmt.Sprintf(
		"s%d AS (SELECT %s.* FROM %s WHERE %s)",
		stageNum, prevStage, prevStage, timePred,
	))
	stageNum++

	// Stage 3+: Apply filter conditions
	if ll.Filter != nil {
		whereStages := ll.buildWhereStages(ll.Filter, &stageNum)
		ctes = append(ctes, whereStages...)
	}

	// Final SELECT: time bucketing and aggregation
	// Matches Scala behavior: (timestamp - (timestamp % step_ms)) as step_ts
	prevStage = fmt.Sprintf("s%d", stageNum-1)
	sql := "WITH\n  " + strings.Join(ctes, ",\n  ") +
		fmt.Sprintf("\nSELECT (CAST(%s AS BIGINT) - (CAST(%s AS BIGINT) %% %d)) AS step_ts, COUNT(*) AS count FROM %s GROUP BY step_ts ORDER BY step_ts",
			tsCol, tsCol, stepMs, prevStage)

	return sql
}

// ToWorkerSQLWithLimit generates DuckDB SQL for this legacy query.
// It follows the same CTE (Common Table Expression) pattern as LogLeaf.ToWorkerSQL.
func (ll *LegacyLeaf) ToWorkerSQLWithLimit(limit int, order string, fields []string) string {
	const baseRel = "{table}"
	const tsCol = "\"chq_timestamp\""

	// Build CTE pipeline similar to logql/worker_sql.go
	var ctes []string
	stageNum := 0

	// Stage 0: SELECT * from base relation
	ctes = append(ctes, fmt.Sprintf("s%d AS (SELECT * FROM %s)", stageNum, baseRel))
	stageNum++

	// Stage 1: Normalize fingerprint type to string (same as LogLeaf)
	prevStage := fmt.Sprintf("s%d", stageNum-1)
	ctes = append(ctes, fmt.Sprintf(
		`s%d AS (SELECT %s.* REPLACE(CAST("chq_fingerprint" AS VARCHAR) AS "chq_fingerprint") FROM %s)`,
		stageNum, prevStage, prevStage,
	))
	stageNum++

	// Stage 2: Time window filter (placeholder will be replaced by worker)
	prevStage = fmt.Sprintf("s%d", stageNum-1)
	timePred := fmt.Sprintf("CAST(%s AS BIGINT) >= {start} AND CAST(%s AS BIGINT) < {end}", tsCol, tsCol)
	ctes = append(ctes, fmt.Sprintf(
		"s%d AS (SELECT %s.* FROM %s WHERE %s)",
		stageNum, prevStage, prevStage, timePred,
	))
	stageNum++

	// Stage 3+: Apply filter conditions
	if ll.Filter != nil {
		whereStages := ll.buildWhereStages(ll.Filter, &stageNum)
		ctes = append(ctes, whereStages...)
	}

	// Build final SELECT
	prevStage = fmt.Sprintf("s%d", stageNum-1)
	var selectCols string
	if len(fields) > 0 {
		// Select specific fields
		quotedFields := make([]string, len(fields))
		for i, f := range fields {
			quotedFields[i] = quoteIdentifier(f)
		}
		selectCols = strings.Join(quotedFields, ", ")
	} else {
		selectCols = "*"
	}

	orderClause := ""
	if order != "" {
		orderClause = fmt.Sprintf(" ORDER BY %s %s", tsCol, order)
	}

	limitClause := ""
	if limit > 0 {
		limitClause = fmt.Sprintf(" LIMIT %d", limit)
	}

	sql := "WITH\n  " + strings.Join(ctes, ",\n  ") +
		fmt.Sprintf("\nSELECT %s FROM %s%s%s", selectCols, prevStage, orderClause, limitClause)

	return sql
}

// buildWhereStages recursively builds WHERE clause stages from the filter tree.
// Returns a list of CTE stages.
func (ll *LegacyLeaf) buildWhereStages(clause QueryClause, stageNum *int) []string {
	var stages []string

	switch c := clause.(type) {
	case Filter:
		// Single filter condition
		condition := ll.filterToSQL(c)
		if condition != "" {
			prevStage := fmt.Sprintf("s%d", *stageNum-1)
			stages = append(stages, fmt.Sprintf(
				"s%d AS (SELECT %s.* FROM %s WHERE %s)",
				*stageNum, prevStage, prevStage, condition,
			))
			*stageNum++
		}

	case BinaryClause:
		// Handle AND/OR combinations
		if len(c.Clauses) == 0 {
			return nil
		}

		op := strings.ToUpper(c.Op)
		if op != "AND" && op != "OR" {
			return nil
		}

		if op == "AND" {
			// For AND: apply each clause sequentially as separate stages
			for _, subClause := range c.Clauses {
				subStages := ll.buildWhereStages(subClause, stageNum)
				stages = append(stages, subStages...)
			}
		} else {
			// For OR: combine all conditions into a single WHERE with OR
			var conditions []string
			for _, subClause := range c.Clauses {
				if filter, ok := subClause.(Filter); ok {
					if cond := ll.filterToSQL(filter); cond != "" {
						conditions = append(conditions, cond)
					}
				} else if binaryClause, ok := subClause.(BinaryClause); ok {
					// Nested boolean clause - build its conditions
					nestedCond := ll.buildNestedCondition(binaryClause)
					if nestedCond != "" {
						conditions = append(conditions, "("+nestedCond+")")
					}
				}
			}

			if len(conditions) > 0 {
				prevStage := fmt.Sprintf("s%d", *stageNum-1)
				combinedCond := "(" + strings.Join(conditions, " OR ") + ")"
				stages = append(stages, fmt.Sprintf(
					"s%d AS (SELECT %s.* FROM %s WHERE %s)",
					*stageNum, prevStage, prevStage, combinedCond,
				))
				*stageNum++
			}
		}
	}

	return stages
}

// buildNestedCondition builds a SQL condition from a nested BinaryClause.
func (ll *LegacyLeaf) buildNestedCondition(clause BinaryClause) string {
	if len(clause.Clauses) == 0 {
		return ""
	}

	op := strings.ToUpper(clause.Op)
	var conditions []string

	for _, subClause := range clause.Clauses {
		if filter, ok := subClause.(Filter); ok {
			if cond := ll.filterToSQL(filter); cond != "" {
				conditions = append(conditions, cond)
			}
		} else if nestedBinary, ok := subClause.(BinaryClause); ok {
			if nestedCond := ll.buildNestedCondition(nestedBinary); nestedCond != "" {
				conditions = append(conditions, "("+nestedCond+")")
			}
		}
	}

	if len(conditions) == 0 {
		return ""
	}

	return strings.Join(conditions, " "+op+" ")
}

// filterToSQL converts a single Filter to a SQL WHERE condition.
func (ll *LegacyLeaf) filterToSQL(filter Filter) string {
	if filter.K == "" {
		return ""
	}

	// Normalize label name (dots → underscores, handle _cardinalhq.* → chq_*)
	colName := normalizeLabelName(filter.K)

	// Check if field is indexed - if not indexed, we handle it differently
	// Non-indexed fields might not exist in all segments, so we add IS NOT NULL checks
	fieldIsIndexed := fingerprint.IsIndexed(colName)

	quotedCol := quoteIdentifier(colName)

	switch filter.Op {
	case "eq":
		if len(filter.V) == 0 {
			return ""
		}
		cond := fmt.Sprintf("%s = %s", quotedCol, sqlStringLiteral(filter.V[0]))
		// For non-indexed fields, add IS NOT NULL check to handle missing fields gracefully
		if !fieldIsIndexed {
			cond = fmt.Sprintf("(%s IS NOT NULL AND %s)", quotedCol, cond)
		}
		return cond

	case "in":
		if len(filter.V) == 0 {
			return ""
		}
		var cond string
		if len(filter.V) == 1 {
			// Optimize single value to eq
			cond = fmt.Sprintf("%s = %s", quotedCol, sqlStringLiteral(filter.V[0]))
		} else {
			// Multiple values: use IN
			values := make([]string, len(filter.V))
			for i, v := range filter.V {
				values[i] = sqlStringLiteral(v)
			}
			cond = fmt.Sprintf("%s IN (%s)", quotedCol, strings.Join(values, ", "))
		}
		// For non-indexed fields, add IS NOT NULL check
		if !fieldIsIndexed {
			cond = fmt.Sprintf("(%s IS NOT NULL AND %s)", quotedCol, cond)
		}
		return cond

	case "contains":
		if len(filter.V) == 0 {
			return ""
		}
		// Use case-insensitive regex to match Scala behavior
		// Scala uses: regexp_matches(col, '.*val.*', 'i')
		escapedValue := regexEscape(filter.V[0])
		pattern := ".*" + escapedValue + ".*"
		cond := fmt.Sprintf("REGEXP_MATCHES(%s, %s, 'i')", quotedCol, sqlStringLiteral(pattern))
		// For non-indexed fields, add IS NOT NULL check
		if !fieldIsIndexed {
			cond = fmt.Sprintf("(%s IS NOT NULL AND %s)", quotedCol, cond)
		}
		return cond

	case "regex":
		if len(filter.V) == 0 {
			return ""
		}
		// Use DuckDB's REGEXP_MATCHES function with case-insensitive flag to match Scala behavior
		// Scala uses: regexp_matches(col, pattern, 'i')
		cond := fmt.Sprintf("REGEXP_MATCHES(%s, %s, 'i')", quotedCol, sqlStringLiteral(filter.V[0]))
		// For non-indexed fields, add IS NOT NULL check
		if !fieldIsIndexed {
			cond = fmt.Sprintf("(%s IS NOT NULL AND %s)", quotedCol, cond)
		}
		return cond

	case "gt":
		if len(filter.V) == 0 {
			return ""
		}
		cond := fmt.Sprintf("%s > %s", quotedCol, sqlLiteral(filter.V[0], filter.DataType))
		// For non-indexed fields, add IS NOT NULL check
		if !fieldIsIndexed {
			cond = fmt.Sprintf("(%s IS NOT NULL AND %s)", quotedCol, cond)
		}
		return cond

	case "gte":
		if len(filter.V) == 0 {
			return ""
		}
		cond := fmt.Sprintf("%s >= %s", quotedCol, sqlLiteral(filter.V[0], filter.DataType))
		// For non-indexed fields, add IS NOT NULL check
		if !fieldIsIndexed {
			cond = fmt.Sprintf("(%s IS NOT NULL AND %s)", quotedCol, cond)
		}
		return cond

	case "lt":
		if len(filter.V) == 0 {
			return ""
		}
		cond := fmt.Sprintf("%s < %s", quotedCol, sqlLiteral(filter.V[0], filter.DataType))
		// For non-indexed fields, add IS NOT NULL check
		if !fieldIsIndexed {
			cond = fmt.Sprintf("(%s IS NOT NULL AND %s)", quotedCol, cond)
		}
		return cond

	case "lte":
		if len(filter.V) == 0 {
			return ""
		}
		cond := fmt.Sprintf("%s <= %s", quotedCol, sqlLiteral(filter.V[0], filter.DataType))
		// For non-indexed fields, add IS NOT NULL check
		if !fieldIsIndexed {
			cond = fmt.Sprintf("(%s IS NOT NULL AND %s)", quotedCol, cond)
		}
		return cond

	default:
		return ""
	}
}

// quoteIdentifier quotes a SQL identifier (column name).
func quoteIdentifier(name string) string {
	// DuckDB uses double quotes for identifiers
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// sqlStringLiteral converts a string to a SQL string literal.
func sqlStringLiteral(s string) string {
	// DuckDB uses single quotes for string literals
	escaped := strings.ReplaceAll(s, `'`, `''`)
	return `'` + escaped + `'`
}

// sqlLiteral converts a value to a SQL literal based on its data type.
func sqlLiteral(value string, dataType string) string {
	switch dataType {
	case "number", "int", "integer", "float", "double":
		// Numeric types don't need quotes
		return value
	default:
		// Default to string literal
		return sqlStringLiteral(value)
	}
}

// regexEscape escapes special regex characters for literal matching.
func regexEscape(s string) string {
	// Escape regex metacharacters for literal matching
	specialChars := []string{`\`, `.`, `*`, `+`, `?`, `^`, `$`, `(`, `)`, `[`, `]`, `{`, `}`, `|`}
	result := s
	for _, char := range specialChars {
		result = strings.ReplaceAll(result, char, `\`+char)
	}
	return result
}

// ToWorkerSQLForTagValues generates SQL to get distinct values for a specific tag/label.
// This mirrors the LogLeaf.ToWorkerSQLForTagValues functionality.
func (ll *LegacyLeaf) ToWorkerSQLForTagValues(tagName string) string {
	const baseRel = "{table}"
	const tsCol = "\"chq_timestamp\""

	var ctes []string
	stageNum := 0

	// Stage 0: SELECT * from base relation
	ctes = append(ctes, fmt.Sprintf("s%d AS (SELECT * FROM %s)", stageNum, baseRel))
	stageNum++

	// Stage 1: Time window filter
	prevStage := fmt.Sprintf("s%d", stageNum-1)
	timePred := fmt.Sprintf("CAST(%s AS BIGINT) >= {start} AND CAST(%s AS BIGINT) < {end}", tsCol, tsCol)
	ctes = append(ctes, fmt.Sprintf(
		"s%d AS (SELECT %s.* FROM %s WHERE %s)",
		stageNum, prevStage, prevStage, timePred,
	))
	stageNum++

	// Stage 2+: Apply filter conditions
	if ll.Filter != nil {
		whereStages := ll.buildWhereStages(ll.Filter, &stageNum)
		ctes = append(ctes, whereStages...)
	}

	// Final SELECT: get distinct values for the tag
	prevStage = fmt.Sprintf("s%d", stageNum-1)
	normalizedTag := normalizeLabelName(tagName)
	quotedTag := quoteIdentifier(normalizedTag)

	sql := "WITH\n  " + strings.Join(ctes, ",\n  ") +
		fmt.Sprintf("\nSELECT DISTINCT %s AS value FROM %s WHERE %s IS NOT NULL ORDER BY value",
			quotedTag, prevStage, quotedTag)

	return sql
}

// SortedFields returns a sorted list of fields to ensure consistent JSON marshaling.
func (ll *LegacyLeaf) SortedFields() []string {
	fields := ll.extractFieldsFromFilter(ll.Filter)
	slices.Sort(fields)
	return fields
}

// extractFieldsFromFilter recursively extracts all field names from a filter tree.
func (ll *LegacyLeaf) extractFieldsFromFilter(clause QueryClause) []string {
	var fields []string

	switch c := clause.(type) {
	case Filter:
		if c.K != "" {
			fields = append(fields, c.K)
		}
	case BinaryClause:
		for _, subClause := range c.Clauses {
			fields = append(fields, ll.extractFieldsFromFilter(subClause)...)
		}
	}

	return fields
}
