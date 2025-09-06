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
	"strconv"
	"strings"

	"github.com/grafana/loki/v3/pkg/logql/syntax"
	logqlmodel "github.com/grafana/loki/v3/pkg/logqlmodel"
)

type logQLValidateRequest struct {
	Query string `json:"query"`
}

type logQLValidateResponse struct {
	Valid bool `json:"valid"`
}

type logQLValidateErrorResponse struct {
	Valid  bool   `json:"valid"`
	Error  string `json:"error"`            // parser error message
	Pos    string `json:"pos,omitempty"`    // "line:col" (from ParseError)
	Line   int    `json:"line,omitempty"`   // numeric line (1-based)
	Column int    `json:"column,omitempty"` // numeric column (1-based, bytes)
	Near   string `json:"near,omitempty"`   // small slice of the query around the error
}

func (q *QuerierService) handleLogQLValidate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req logQLValidateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Query == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(logQLValidateErrorResponse{
			Valid: false, Error: "Missing or invalid JSON body; expected {\"query\": \"...\"}",
		})
		return
	}

	_, err := syntax.ParseExpr(req.Query)
	if err == nil {
		_ = json.NewEncoder(w).Encode(logQLValidateResponse{Valid: true})
		return
	}

	// If it's a LogQL parse error, expose line/column and a little context
	if pe, ok := err.(logqlmodel.ParseError); ok {
		// Extract line and column from the error message
		// Format is typically "parse error at line X, col Y: message"
		line, col := parseErrorPosition(pe.Error())

		// Get a slice of the query around the error position
		near := sliceSafe(req.Query, col-10, col+10)

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logQLValidateErrorResponse{
			Valid:  false,
			Error:  pe.Error(),
			Pos:    strconv.Itoa(line) + ":" + strconv.Itoa(col),
			Line:   line,
			Column: col,
			Near:   near,
		})
		return
	}

	// Fallback for non-parse errors
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(logQLValidateErrorResponse{Valid: false, Error: err.Error()})
}

// parseErrorPosition extracts line and column from LogQL parse error messages
// Format: "parse error at line X, col Y: message"
func parseErrorPosition(errMsg string) (line, col int) {
	// Default to line 1, col 1 if parsing fails
	line, col = 1, 1

	// Look for "at line X, col Y" pattern
	parts := strings.Split(errMsg, "at line ")
	if len(parts) < 2 {
		return
	}

	// Extract the part after "at line "
	linePart := parts[1]

	// Split by comma to separate line and column
	lineColParts := strings.Split(linePart, ", col ")
	if len(lineColParts) < 2 {
		return
	}

	// Parse line number
	if l, err := strconv.Atoi(strings.TrimSpace(lineColParts[0])); err == nil {
		line = l
	}

	// Parse column number (everything before the colon)
	colPart := strings.Split(lineColParts[1], ":")[0]
	if c, err := strconv.Atoi(strings.TrimSpace(colPart)); err == nil {
		col = c
	}

	return
}
