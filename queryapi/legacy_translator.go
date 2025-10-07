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
	"regexp"
	"strings"
)

// TranslationContext tracks labels seen during translation for debugging.
type TranslationContext struct {
	QueryLabels map[string]string // underscored → dotted
}

// TranslateToLogQL converts a legacy BaseExpression to a LogQL query string.
func TranslateToLogQL(baseExpr BaseExpression) (string, *TranslationContext, error) {
	// Validate dataset
	if baseExpr.Dataset != "logs" {
		return "", nil, fmt.Errorf("only 'logs' dataset is supported, got: %s", baseExpr.Dataset)
	}

	ctx := &TranslationContext{QueryLabels: make(map[string]string)}

	matchers, pipeline, err := filterToLogQL(baseExpr.Filter, ctx)
	if err != nil {
		return "", nil, err
	}

	// Build LogQL query
	// If we have no matchers but have pipeline operations, we need a universal matcher
	if len(matchers) == 0 && len(pipeline) > 0 {
		// Use an empty stream selector {} - LogQL allows this for pipeline-only queries
		logql := "{}"
		if len(pipeline) > 0 {
			logql += " " + strings.Join(pipeline, " ")
		}
		return logql, ctx, nil
	}

	logql := "{" + strings.Join(matchers, ",") + "}"
	if len(pipeline) > 0 {
		logql += " " + strings.Join(pipeline, " ")
	}

	return logql, ctx, nil
}

// filterToLogQL converts a QueryClause to LogQL matchers and pipeline operations.
func filterToLogQL(clause QueryClause, ctx *TranslationContext) ([]string, []string, error) {
	matchers := []string{}
	pipeline := []string{}

	switch c := clause.(type) {
	case Filter:
		normalized := normalizeLabelName(c.K) // dots → underscores
		ctx.QueryLabels[normalized] = c.K     // Remember original

		switch c.Op {
		case "eq":
			if len(c.V) == 0 {
				return nil, nil, fmt.Errorf("eq operator requires at least one value")
			}
			// Escape quotes in value
			escapedVal := escapeLogQLValue(c.V[0])
			matchers = append(matchers, fmt.Sprintf(`%s="%s"`, normalized, escapedVal))

		case "in":
			// LogQL: label=~"^(val1|val2|val3)$" - anchored for exact match
			if len(c.V) == 0 {
				return nil, nil, fmt.Errorf("in operator requires at least one value")
			}
			// Escape each value and join with |
			escaped := make([]string, len(c.V))
			for i, v := range c.V {
				escaped[i] = escapeRegexValue(v)
			}
			pattern := "^(" + strings.Join(escaped, "|") + ")$"
			matchers = append(matchers, fmt.Sprintf(`%s=~"%s"`, normalized, pattern))

		case "contains":
			// LogQL line filter: |~ "pattern"
			// Note: For contains, we add a pipeline filter. If this is the only filter,
			// the stream selector will be empty, which is handled in TranslateToLogQL
			if len(c.V) == 0 {
				return nil, nil, fmt.Errorf("contains operator requires a value")
			}
			escapedVal := escapeRegexValue(c.V[0])
			pipeline = append(pipeline, fmt.Sprintf(`|~ "%s"`, escapedVal))

		case "regex":
			if len(c.V) == 0 {
				return nil, nil, fmt.Errorf("regex operator requires a pattern")
			}
			// The pattern is already a regex, so we don't escape it
			matchers = append(matchers, fmt.Sprintf(`%s=~"%s"`, normalized, c.V[0]))

		case "gt", "gte", "lt", "lte":
			// These would need label_format or parser stage
			return nil, nil, fmt.Errorf("comparison operators not yet supported: %s", c.Op)

		default:
			return nil, nil, fmt.Errorf("unsupported operator: %s", c.Op)
		}

	case BinaryClause:
		m1, p1, err := filterToLogQL(c.Q1, ctx)
		if err != nil {
			return nil, nil, err
		}
		m2, p2, err := filterToLogQL(c.Q2, ctx)
		if err != nil {
			return nil, nil, err
		}

		// Handle AND: combine matchers and pipelines
		if strings.ToLower(c.Op) == "and" {
			matchers = append(matchers, m1...)
			matchers = append(matchers, m2...)
			pipeline = append(pipeline, p1...)
			pipeline = append(pipeline, p2...)
		} else if strings.ToLower(c.Op) == "or" {
			// LogQL doesn't support OR in stream selectors
			// This would require running multiple queries and merging results
			return nil, nil, fmt.Errorf("OR operator not yet supported in LogQL translation")
		} else {
			return nil, nil, fmt.Errorf("unsupported binary operator: %s", c.Op)
		}
	}

	return matchers, pipeline, nil
}

// normalizeLabelName converts dotted label names to underscored names.
// Also handles the old _cardinalhq.* naming convention → chq_*
func normalizeLabelName(dotted string) string {
	// Convert old _cardinalhq.* naming to chq_*
	if strings.HasPrefix(dotted, "_cardinalhq.") {
		rest := dotted[len("_cardinalhq."):]
		dotted = "chq_" + rest
	}
	return strings.ReplaceAll(dotted, ".", "_")
}

// escapeLogQLValue escapes special characters in a LogQL string value.
func escapeLogQLValue(s string) string {
	// Escape backslashes first
	s = strings.ReplaceAll(s, `\`, `\\`)
	// Escape double quotes
	s = strings.ReplaceAll(s, `"`, `\"`)
	return s
}

// escapeRegexValue escapes regex special characters for use in LogQL regex.
var regexEscapePattern = regexp.MustCompile(`([.+*?()\[\]{}^$|\\])`)

func escapeRegexValue(s string) string {
	// Escape regex special characters
	return regexEscapePattern.ReplaceAllString(s, `\$1`)
}
