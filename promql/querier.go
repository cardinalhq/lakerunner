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

package promql

import (
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/google/uuid"
	"net/http"
)

type QuerierService struct {
	mdb *lrdb.Store
}

func (q *QuerierService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	orgID := r.URL.Query().Get("orgId")
	if orgID == "" {
		http.Error(w, "missing orgId", http.StatusBadRequest)
		return
	}
	s := r.URL.Query().Get("s")
	e := r.URL.Query().Get("e")
	if s == "" || e == "" {
		http.Error(w, "missing s/e", http.StatusBadRequest)
		return
	}

	startTs, endTs, err := dateutils.ToStartEnd(s, e)
	if err != nil {
		http.Error(w, "invalid s/e: "+err.Error(), http.StatusBadRequest)
		return
	}
	if startTs >= endTs {
		http.Error(w, "start must be < end", http.StatusBadRequest)
		return
	}

	orgUUID, err := uuid.Parse(orgID)
	if err != nil {
		http.Error(w, "invalid orgId: "+err.Error(), http.StatusBadRequest)
		return
	}

	prom := r.URL.Query().Get("q")
	if prom == "" {
		http.Error(w, "missing query expression", http.StatusBadRequest)
		return
	}

	promExpr, err := FromPromQL(prom)
	if err != nil {
		http.Error(w, "invalid query expression: "+err.Error(), http.StatusBadRequest)
		return
	}

	queryPlan, err := Compile(promExpr)
	if err != nil {
		http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
		return
	}

	q.Evaluate(orgUUID, startTs, endTs, queryPlan, true)

}
