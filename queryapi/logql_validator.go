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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Query == "" {
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(logQLValidateResponse{
			Valid: false, Error: `Missing or invalid JSON body; expected {"query":"...", "exemplar":"... (optional)"}`,
		})
		return
	}

	// If there’s no exemplar, we just check the syntax/compile path.
	if req.Exemplar == "" {
		// Basic compile sanity (no DB work).
		if _, err := logql.FromLogQL(req.Query); err != nil {
			q.sendError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(logQLValidateResponse{Valid: true})
		return
	}

	// Full-path validation with exemplar → ingest → run worker SQL.
	if _, err := ValidateLogQLAgainstExemplar(r.Context(), req.Query, req.Exemplar); err != nil {
		q.sendError(w, err)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(logQLValidateResponse{Valid: true})
}

func (q *QuerierService) sendError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusBadRequest)
	err = json.NewEncoder(w).Encode(logQLValidateResponse{Valid: false, Error: err.Error()})
}
