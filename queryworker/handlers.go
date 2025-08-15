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

package queryworker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/cardinalhq/lakerunner/promql"
)

func (s *Service) handlePushdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var request promql.PushDownRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		slog.Error("Failed to decode pushdown request", "error", err)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	slog.Info("Received pushdown request",
		"exprID", request.BaseExpr.ID,
		"startTs", request.StartTs,
		"endTs", request.EndTs,
		"segmentCount", len(request.Segments))

	// Set up SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ctx := r.Context()
	resultsCh := make(chan promql.SketchInput, 1024)

	// Process segments in a goroutine
	go func() {
		defer close(resultsCh)

		if err := s.processSegments(ctx, request, resultsCh); err != nil {
			slog.Error("Failed to process segments", "error", err)
			// Send error event
			s.writeSSE(w, flusher, "error", map[string]string{"error": err.Error()})
			return
		}
	}()

	// Stream results
	for {
		select {
		case <-ctx.Done():
			slog.Info("Client disconnected")
			return
		case result, ok := <-resultsCh:
			if !ok {
				// End of stream
				s.writeSSE(w, flusher, "done", map[string]string{"status": "complete"})
				return
			}
			if err := s.writeSSE(w, flusher, "data", result); err != nil {
				slog.Error("Failed to write SSE", "error", err)
				return
			}
		}
	}
}

func (s *Service) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().Unix(),
		"service":   "query-worker",
	}
	json.NewEncoder(w).Encode(response)
}

func (s *Service) writeSSE(w http.ResponseWriter, flusher http.Flusher, event string, data interface{}) error {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	if _, err := fmt.Fprintf(w, "event: %s\n", event); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "data: %s\n\n", jsonData); err != nil {
		return err
	}
	flusher.Flush()
	return nil
}
