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
	"encoding/json"
	"fmt"
	"github.com/cardinalhq/lakerunner/lrdb"
	"github.com/cardinalhq/oteltools/pkg/dateutils"
	"github.com/google/uuid"
	"log/slog"
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

	reverse := r.URL.Query().Get("reverse")
	reverseSort := true
	if reverse != "" {
		reverseSort = reverse == "true"
	}

	promExpr, err := FromPromQL(prom)
	if err != nil {
		http.Error(w, "invalid query expression: "+err.Error(), http.StatusBadRequest)
		return
	}

	plan, err := Compile(promExpr)
	if err != nil {
		http.Error(w, "compile error: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Kick off evaluation; reverseSort can be toggled if you add a query param.
	resultsCh, err := q.Evaluate(r.Context(), orgUUID, startTs, endTs, plan, reverseSort)
	if err != nil {
		http.Error(w, "evaluate error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// SSE setup
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Disable proxy buffering if behind nginx etc.
	w.Header().Set("X-Accel-Buffering", "no")

	writeSSE := func(event string, v any) error {
		// Marshal payload (if any) to a single JSON line.
		var data []byte
		var err error
		if v != nil {
			data, err = json.Marshal(v)
			if err != nil {
				return err
			}
		} else {
			data = []byte(`null`)
		}
		// Write SSE frame
		if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
			return err
		}
		if _, err := w.Write([]byte("data: ")); err != nil {
			return err
		}
		if _, err := w.Write(data); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return err
		}
		flusher.Flush()
		return nil
	}

	// Stream results until channel closes or client disconnects.
	notify := r.Context().Done()
	for {
		select {
		case <-notify:
			// client went away; stop work
			slog.Info("client disconnected; stopping stream")
			return
		case res, ok := <-resultsCh:
			if !ok {
				// End of stream: send a final "done" event.
				_ = writeSSE("done", map[string]string{"status": "ok"})
				return
			}
			// Stream one result tick
			if err := writeSSE("result", res); err != nil {
				slog.Error("write SSE failed", "error", err)
				return
			}
		}
	}
}
