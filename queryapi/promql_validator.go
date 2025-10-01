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
	"fmt"
	"net/http"

	"github.com/prometheus/prometheus/promql/parser"

	"github.com/cardinalhq/lakerunner/promql"
)

type promQLValidateRequest struct {
	Query    string         `json:"query"`
	Exemplar map[string]any `json:"exemplar,omitempty"`
}

type promQLValidateResponse struct {
	Valid bool `json:"valid"`
}

type promQLValidateErrorResponse struct {
	Valid            bool                   `json:"valid"`
	Error            string                 `json:"error"`                   // parser error message
	Pos              string                 `json:"pos,omitempty"`           // "line:col" (from StartPosInput)
	Line             int                    `json:"line,omitempty"`          // numeric line (1-based)
	Column           int                    `json:"column,omitempty"`        // numeric column (1-based, bytes)
	Near             string                 `json:"near,omitempty"`          // small slice of the query around the error
	MissingFields    []string               `json:"missingFields,omitempty"` // fields referenced but not found in exemplar
	ValidationErrors []FieldValidationError `json:"validationErrors,omitempty"`
}

type FieldValidationError struct {
	Field   string `json:"field"`
	Context string `json:"context"` // "label_matcher", "group_by", "metric_name", etc.
	Message string `json:"message"`
}

func (q *QuerierService) handlePromQLValidate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req promQLValidateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Query == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(promQLValidateErrorResponse{
			Valid: false, Error: "Missing or invalid JSON body; expected {\"query\": \"...\", \"exemplar\": {...}}",
		})
		return
	}

	// First, validate PromQL syntax
	_, err := parser.ParseExpr(req.Query)
	if err != nil {
		handlePromQLParseError(w, req.Query, err)
		return
	}

	// If no exemplar provided, just return syntax validation
	if len(req.Exemplar) == 0 {
		_ = json.NewEncoder(w).Encode(promQLValidateResponse{Valid: true})
		return
	}

	// Validate against exemplar fields
	if validationErr := validateQueryAgainstExemplar(req.Query, req.Exemplar); validationErr != nil {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(*validationErr)
		return
	}

	_ = json.NewEncoder(w).Encode(promQLValidateResponse{Valid: true})
}

func handlePromQLParseError(w http.ResponseWriter, query string, err error) {
	// If it's a PromQL parse error, expose line/column and a little context
	if pe, ok := err.(*parser.ParseErr); ok {
		pr := pe.PositionRange
		start := int(pr.Start)
		end := int(pr.End)

		// Derive line/col (1-based) from the original query string.
		line, col := lineColFromOffset(query, start)
		near := sliceSafe(query, start-10, end+10)

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(promQLValidateErrorResponse{
			Valid:  false,
			Error:  pe.Err.Error(),
			Pos:    pr.StartPosInput(pe.Query, pe.LineOffset),
			Line:   line,
			Column: col,
			Near:   near,
		})
		return
	}

	// Fallback for non-parse errors
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(promQLValidateErrorResponse{Valid: false, Error: err.Error()})
}

func validateQueryAgainstExemplar(query string, exemplar map[string]any) *promQLValidateErrorResponse {
	// Extract available fields from exemplar
	availableFields := extractFieldsFromExemplar(exemplar)

	// Convert to our internal AST using existing PromQL parser
	internalExpr, err := promql.FromPromQL(query)
	if err != nil {
		return &promQLValidateErrorResponse{
			Valid: false,
			Error: fmt.Sprintf("Failed to parse PromQL: %v", err),
		}
	}

	plan, err := promql.Compile(internalExpr)
	if err != nil {
		return &promQLValidateErrorResponse{
			Valid: false,
			Error: fmt.Sprintf("Failed to compile query plan: %v", err),
		}
	}

	var validationErrors []FieldValidationError
	var missingFields []string

	// Validate each leaf in the plan
	for _, leaf := range plan.Leaves {
		errors, missing := validateLeafAgainstExemplar(leaf, availableFields)
		validationErrors = append(validationErrors, errors...)
		missingFields = append(missingFields, missing...)
	}

	// Check final grouping from the plan
	groupBy, without, hasGrouping := promql.FinalGroupingFromPlan(plan)
	if hasGrouping {
		if len(groupBy) > 0 {
			for _, field := range groupBy {
				if !fieldExists(field, availableFields) {
					validationErrors = append(validationErrors, FieldValidationError{
						Field:   field,
						Context: "group_by",
						Message: fmt.Sprintf("Group by field '%s' not found in exemplar data", field),
					})
					missingFields = append(missingFields, field)
				}
			}
		}
		if len(without) > 0 {
			for _, field := range without {
				if !fieldExists(field, availableFields) {
					validationErrors = append(validationErrors, FieldValidationError{
						Field:   field,
						Context: "group_without",
						Message: fmt.Sprintf("Group without field '%s' not found in exemplar data", field),
					})
					missingFields = append(missingFields, field)
				}
			}
		}
	}

	// Remove duplicates from missingFields
	missingFields = removeDuplicates(missingFields)

	if len(validationErrors) > 0 {
		return &promQLValidateErrorResponse{
			Valid:            false,
			Error:            fmt.Sprintf("Query references %d field(s) not found in exemplar data", len(missingFields)),
			MissingFields:    missingFields,
			ValidationErrors: validationErrors,
		}
	}

	return nil
}

func validateLeafAgainstExemplar(leaf promql.BaseExpr, availableFields map[string]struct{}) ([]FieldValidationError, []string) {
	var errors []FieldValidationError
	var missing []string

	// Check metric name
	if leaf.Metric != "" && !fieldExists(leaf.Metric, availableFields) {
		errors = append(errors, FieldValidationError{
			Field:   leaf.Metric,
			Context: "metric_name",
			Message: fmt.Sprintf("Metric '%s' not found in exemplar data", leaf.Metric),
		})
		missing = append(missing, leaf.Metric)
	}

	// Check label matchers
	for _, matcher := range leaf.Matchers {
		if !fieldExists(matcher.Label, availableFields) {
			errors = append(errors, FieldValidationError{
				Field:   matcher.Label,
				Context: "label_matcher",
				Message: fmt.Sprintf("Label '%s' used in matcher not found in exemplar data", matcher.Label),
			})
			missing = append(missing, matcher.Label)
		}
	}

	// Skip leaf-level grouping validation since we check final grouping at plan level
	// This avoids duplicate validation errors

	return errors, missing
}

func extractFieldsFromExemplar(exemplar map[string]any) map[string]struct{} {
	fields := make(map[string]struct{})

	// Keep any flat keys the caller may already provide (e.g., "__name__")
	for k := range exemplar {
		fields[k] = struct{}{}
	}
	if v, ok := exemplar["__name__"]; ok {
		if s, ok := v.(string); ok && s != "" {
			fields[s] = struct{}{}
		}
	}

	rm, ok := exemplar["resourceMetrics"]
	if !ok {
		return fields
	}
	rmArr, ok := rm.([]any)
	if !ok {
		return fields
	}

	str := func(v any) string {
		if s, ok := v.(string); ok {
			return s
		}
		return ""
	}

	for _, rmi := range rmArr {
		rmm, _ := rmi.(map[string]any)

		if res, ok := rmm["resource"].(map[string]any); ok {
			if attrs, ok := res["attributes"].([]any); ok {
				for _, a := range attrs {
					if am, ok := a.(map[string]any); ok {
						if k := str(am["key"]); k != "" {
							fields["resource_"+k] = struct{}{}
						}
					}
				}
			}
		}

		if smAny, ok := rmm["scopeMetrics"].([]any); ok {
			for _, smi := range smAny {
				sm, _ := smi.(map[string]any)
				if metsAny, ok := sm["metrics"].([]any); ok {
					for _, mi := range metsAny {
						m, _ := mi.(map[string]any)

						if name := str(m["name"]); name != "" {
							fields[name] = struct{}{}
						}

						for _, kind := range []string{"sum", "gauge", "histogram", "exponentialHistogram", "summary"} {
							if body, ok := m[kind].(map[string]any); ok {
								if dps, ok := body["dataPoints"].([]any); ok {
									for _, dpi := range dps {
										if dp, ok := dpi.(map[string]any); ok {
											if atts, ok := dp["attributes"].([]any); ok {
												for _, a := range atts {
													if am, ok := a.(map[string]any); ok {
														if k := str(am["key"]); k != "" {
															fields["metric."+k] = struct{}{}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return fields
}

func fieldExists(field string, availableFields map[string]struct{}) bool {
	_, exists := availableFields[field]
	return exists
}

func removeDuplicates(slice []string) []string {
	seen := make(map[string]struct{})
	var result []string

	for _, item := range slice {
		if _, exists := seen[item]; !exists {
			seen[item] = struct{}{}
			result = append(result, item)
		}
	}

	return result
}

func lineColFromOffset(s string, pos int) (line, col int) {
	if pos < 0 {
		return 0, 0
	}
	line = 1
	col = 1
	for i := 0; i < len(s) && i < pos; i++ {
		if s[i] == '\n' {
			line++
			col = 1
		} else {
			col++
		}
	}
	return
}

func sliceSafe(s string, i, j int) string {
	if i < 0 {
		i = 0
	}
	if j < 0 {
		j = 0
	}
	if i > len(s) {
		i = len(s)
	}
	if j > len(s) {
		j = len(s)
	}
	if i >= j {
		return ""
	}
	return s[i:j]
}
