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
	Query    string         `json:"query"`
	Exemplar map[string]any `json:"exemplar,omitempty"`
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

	// If no exemplar provided, just return valid (syntax check only)
	if len(req.Exemplar) == 0 {
		_ = json.NewEncoder(w).Encode(logQLValidateResponse{Valid: true})
		return
	}

	me, err := json.Marshal(req.Exemplar)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, InvalidJSON, "invalid exemplar: "+err.Error())
		return
	}
	payload := string(me)

	vr, err := ValidateLogQLAgainstExemplar(r.Context(), req.Query, payload)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, ValidationFailed, "validation failed: "+err.Error())
		return
	}

	_ = json.NewEncoder(w).Encode(vr)
}
