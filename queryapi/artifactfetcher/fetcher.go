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
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	DefaultTimeout  = 30 * time.Second
	MaxArtifactSize = 256 * 1024 * 1024 // 256 MB
)

// Fetcher retrieves artifact data from query workers via HTTP.
// Implements queryapi/workmanager.ArtifactFetcher.
type Fetcher struct {
	client *http.Client
}

// NewFetcher creates a new artifact fetcher.
func NewFetcher() *Fetcher {
	return &Fetcher{
		client: &http.Client{
			Timeout: DefaultTimeout,
		},
	}
}

// FetchArtifact downloads artifact data from the given URL and verifies the checksum.
func (f *Fetcher) FetchArtifact(ctx context.Context, artifactURL string, expectedChecksum string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, artifactURL, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		recordFetchError("http_error")
		return nil, fmt.Errorf("fetch artifact: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		recordFetchError("http_error")
		return nil, fmt.Errorf("artifact fetch returned status %d", resp.StatusCode)
	}

	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxArtifactSize+1))
	if err != nil {
		recordFetchError("read_error")
		return nil, fmt.Errorf("read artifact body: %w", err)
	}
	if int64(len(data)) > MaxArtifactSize {
		recordFetchError("size_exceeded")
		return nil, fmt.Errorf("artifact exceeds max size (%d bytes)", MaxArtifactSize)
	}

	if expectedChecksum != "" {
		if err := verifyChecksum(data, expectedChecksum); err != nil {
			recordFetchError("checksum_mismatch")
			return nil, err
		}
	}

	return data, nil
}

func verifyChecksum(data []byte, expected string) error {
	h := sha256.Sum256(data)
	actual := "sha256:" + hex.EncodeToString(h[:])
	if actual != expected {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expected, actual)
	}
	return nil
}
