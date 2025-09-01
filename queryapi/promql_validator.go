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
	"net/http"

	"github.com/prometheus/prometheus/promql/parser"
)

type promQLValidateRequest struct {
	Query string `json:"query"`
}

type promQLValidateResponse struct {
	Valid bool `json:"valid"`
}

type promQLValidateErrorResponse struct {
	Valid  bool   `json:"valid"`
	Error  string `json:"error"`            // parser error message
	Pos    string `json:"pos,omitempty"`    // "line:col" (from StartPosInput)
	Line   int    `json:"line,omitempty"`   // numeric line (1-based)
	Column int    `json:"column,omitempty"` // numeric column (1-based, bytes)
	Near   string `json:"near,omitempty"`   // small slice of the query around the error
}

func (q *QuerierService) validatePromQL(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req promQLValidateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Query == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(promQLValidateErrorResponse{
			Valid: false, Error: "Missing or invalid JSON body; expected {\"query\": \"...\"}",
		})
		return
	}

	_, err := parser.ParseExpr(req.Query)
	if err == nil {
		_ = json.NewEncoder(w).Encode(promQLValidateResponse{Valid: true})
		return
	}

	// If it's a PromQL parse error, expose line/column and a little context
	if pe, ok := err.(*parser.ParseErr); ok {
		pr := pe.PositionRange
		start := int(pr.Start)
		end := int(pr.End)

		// Derive line/col (1-based) from the original query string.
		line, col := lineColFromOffset(req.Query, start)

		near := sliceSafe(req.Query, start-10, end+10)

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
