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

package artifactfetcher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sha256Checksum(data []byte) string {
	h := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(h[:])
}

func TestFetcher_FetchArtifact(t *testing.T) {
	content := []byte("parquet data here")
	checksum := sha256Checksum(content)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	f := NewFetcher()
	data, err := f.FetchArtifact(t.Context(), srv.URL+"/artifacts/w1", checksum)
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

func TestFetcher_FetchArtifact_NoChecksum(t *testing.T) {
	content := []byte("data")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	f := NewFetcher()
	data, err := f.FetchArtifact(t.Context(), srv.URL, "")
	require.NoError(t, err)
	assert.Equal(t, content, data)
}

func TestFetcher_FetchArtifact_ChecksumMismatch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("actual content"))
	}))
	defer srv.Close()

	f := NewFetcher()
	_, err := f.FetchArtifact(t.Context(), srv.URL, "sha256:0000000000000000000000000000000000000000000000000000000000000000")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "checksum mismatch")
}

func TestFetcher_FetchArtifact_ServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	f := NewFetcher()
	_, err := f.FetchArtifact(t.Context(), srv.URL, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestFetcher_FetchArtifact_ContextCanceled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("data"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	f := NewFetcher()
	_, err := f.FetchArtifact(ctx, srv.URL, "")
	assert.Error(t, err)
}

func TestFetcher_FetchArtifact_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	f := NewFetcher()
	_, err := f.FetchArtifact(t.Context(), srv.URL, "")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "status 404")
}
