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
	"strings"

	"github.com/cardinalhq/lakerunner/logql"
)

type logQLValidateRequest struct {
	Query           string         `json:"query"`
	Exemplar        map[string]any `json:"exemplar,omitempty"`
	StreamAttribute string         `json:"stream_attribute,omitempty"`
}

type logQLValidateResponse struct {
	Valid bool   `json:"valid"`
	Error string `json:"error,omitempty"`
}

func (q *QuerierService) handleLogQLValidate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var req logQLValidateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || strings.TrimSpace(req.Query) == "" {
		writeAPIError(w, http.StatusBadRequest, InvalidJSON,
			`missing or invalid JSON body; expected {"query":"...", "exemplar":"... (optional)"}`)
		return
	}

	ast, err := logql.FromLogQL(req.Query)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid LogQL: "+err.Error())
		return
	}

	// Validate equality matcher requirement (regardless of exemplar)
	if err := ValidateEqualityMatcherRequirement(ast); err != nil {
		writeAPIError(w, http.StatusBadRequest, ValidationFailed, err.Error())
		return
	}

	// Validate stream attribute requirement if specified
	if req.StreamAttribute != "" {
		if err := ValidateStreamAttributeRequirement(ast, req.StreamAttribute); err != nil {
			writeAPIError(w, http.StatusBadRequest, ValidationFailed, err.Error())
			return
		}
	}

	// If no exemplar provided, return valid (syntax + equality matcher check only)
	if len(req.Exemplar) == 0 {
		_ = json.NewEncoder(w).Encode(logQLValidateResponse{Valid: true})
		return
	}

	vr, err := ValidateLogQLAgainstExemplar(r.Context(), req.Query, req.Exemplar)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, ValidationFailed, "validation failed: "+err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(vr)
}
