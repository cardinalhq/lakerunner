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
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ServeArtifact(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	content := []byte("parquet file content")
	path := writeTestArtifact(t, dir, "w1", content)
	checksum, _, err := s.Register("w1", path)
	require.NoError(t, err)

	handler := NewHandler(s)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/artifacts/w1", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, checksum, rec.Header().Get("X-Artifact-Checksum"))

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	assert.Equal(t, content, body)
}

func TestHandler_NotFound(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)

	handler := NewHandler(s)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/artifacts/nonexistent", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)

	handler := NewHandler(s)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/artifacts/w1", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_MissingWorkID(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)

	handler := NewHandler(s)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/artifacts/", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRegisterRoutes(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)

	mux := http.NewServeMux()
	RegisterRoutes(mux, s)

	// Should be routable.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/artifacts/test", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	// 404 (no artifact) but not 405 (route exists).
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestArtifactURL(t *testing.T) {
	url := ArtifactURL("worker-1:8081", "w123")
	assert.Equal(t, "http://worker-1:8081/api/v1/artifacts/w123", url)
}
