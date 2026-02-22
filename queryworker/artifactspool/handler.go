// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package artifactspool

import (
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

// Handler serves artifact files over HTTP.
// Route pattern: GET /api/v1/artifacts/{workID}
type Handler struct {
	spool *Spool
}

// NewHandler creates an HTTP handler backed by the given spool.
func NewHandler(spool *Spool) *Handler {
	return &Handler{spool: spool}
}

// ServeHTTP handles artifact download requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract workID from path: /api/v1/artifacts/{workID}
	workID := strings.TrimPrefix(r.URL.Path, "/api/v1/artifacts/")
	if workID == "" || workID == r.URL.Path {
		http.Error(w, "missing work_id", http.StatusBadRequest)
		return
	}

	filePath, checksum, ok := h.spool.Get(workID)
	if !ok {
		http.Error(w, "artifact not found", http.StatusNotFound)
		return
	}

	slog.Debug("Serving artifact",
		slog.String("work_id", workID),
		slog.String("file_path", filePath))

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("X-Artifact-Checksum", checksum)

	// Use http.ServeFile for range support and proper Content-Length.
	http.ServeFile(w, r, filePath)
}

// RegisterRoutes adds the artifact handler to a mux.
func RegisterRoutes(mux *http.ServeMux, spool *Spool) {
	handler := NewHandler(spool)
	mux.Handle("/api/v1/artifacts/", handler)
}

// ArtifactURL returns the full URL for fetching an artifact from this worker.
func ArtifactURL(workerAddr, workID string) string {
	return "http://" + workerAddr + "/api/v1/artifacts/" + workID
}

// ContentLengthFromResponse extracts Content-Length from an HTTP response.
func ContentLengthFromResponse(resp *http.Response) int64 {
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		n, _ := strconv.ParseInt(cl, 10, 64)
		return n
	}
	return -1
}
