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
	"github.com/cardinalhq/lakerunner/logql"
	"net/http"
	"strings"
)

type logQLValidateRequest struct {
	Query    string `json:"query"`
	Exemplar string `json:"exemplar,omitempty"`
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

	if _, err := logql.FromLogQL(req.Query); err != nil {
		writeAPIError(w, http.StatusBadRequest, ErrInvalidExpr, "invalid LogQL: "+err.Error())
		return
	}

	if strings.TrimSpace(req.Exemplar) == "" {
		_ = json.NewEncoder(w).Encode(logQLValidateResponse{Valid: true})
		return
	}

	if _, err := ValidateLogQLAgainstExemplar(r.Context(), req.Query, req.Exemplar); err != nil {
		status, code := statusAndCodeForRuntimeError(err)
		writeAPIError(w, status, code, "validation failed: "+err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(logQLValidateResponse{Valid: true})
}

func (q *QuerierService) sendError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(logQLValidateResponse{Valid: false, Error: err.Error()})
}
