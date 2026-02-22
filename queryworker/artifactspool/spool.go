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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

const (
	UnACKedTTL      = 10 * time.Minute
	PostACKTTL      = 30 * time.Second
	CleanupInterval = 30 * time.Second
)

// artifactEntry tracks a single artifact file on disk.
type artifactEntry struct {
	workID   string
	filePath string
	size     int64
	checksum string // "sha256:<hex>"
	created  time.Time
	acked    bool
	ackedAt  time.Time
}

// Spool manages query result artifact files on the worker's local disk.
type Spool struct {
	baseDir string

	mu      sync.RWMutex
	entries map[string]*artifactEntry // workID → entry

	stopCh chan struct{}
	done   chan struct{}
}

// NewSpool creates a new artifact spool rooted at baseDir.
func NewSpool(baseDir string) (*Spool, error) {
	if err := os.MkdirAll(baseDir, 0o755); err != nil {
		return nil, fmt.Errorf("create spool dir: %w", err)
	}
	return &Spool{
		baseDir: baseDir,
		entries: make(map[string]*artifactEntry),
		stopCh:  make(chan struct{}),
		done:    make(chan struct{}),
	}, nil
}

// Start begins the background cleanup goroutine.
func (s *Spool) Start() {
	go s.cleanupLoop()
}

// Stop halts the cleanup goroutine and waits for it to finish.
func (s *Spool) Stop() {
	close(s.stopCh)
	<-s.done
}

// PathForWork returns the file path where an artifact for workID should be written.
func (s *Spool) PathForWork(workID string) string {
	return filepath.Join(s.baseDir, workID+".parquet")
}

// Register records a completed artifact. The file at filePath must already exist.
// Returns the checksum and size.
func (s *Spool) Register(workID, filePath string) (checksum string, size int64, err error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", 0, fmt.Errorf("open artifact: %w", err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	n, err := io.Copy(h, f)
	if err != nil {
		return "", 0, fmt.Errorf("compute checksum: %w", err)
	}

	checksum = "sha256:" + hex.EncodeToString(h.Sum(nil))

	s.mu.Lock()
	s.entries[workID] = &artifactEntry{
		workID:   workID,
		filePath: filePath,
		size:     n,
		checksum: checksum,
		created:  time.Now(),
	}
	s.mu.Unlock()

	return checksum, n, nil
}

// Get returns the file path and checksum for a registered artifact.
func (s *Spool) Get(workID string) (filePath, checksum string, ok bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, exists := s.entries[workID]
	if !exists {
		return "", "", false
	}
	return e.filePath, e.checksum, true
}

// Acknowledge marks an artifact as fetched by the API.
func (s *Spool) Acknowledge(workID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, exists := s.entries[workID]; exists {
		e.acked = true
		e.ackedAt = time.Now()
	}
}

// Remove deletes an artifact from disk and tracking.
func (s *Spool) Remove(workID string) {
	s.mu.Lock()
	e, exists := s.entries[workID]
	if exists {
		delete(s.entries, workID)
	}
	s.mu.Unlock()

	if exists {
		if err := os.Remove(e.filePath); err != nil && !os.IsNotExist(err) {
			slog.Warn("Failed to remove artifact file",
				slog.String("work_id", workID),
				slog.Any("error", err))
		}
	}
}

// Count returns the number of tracked artifacts.
func (s *Spool) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// TotalBytes returns the total size of tracked artifacts.
func (s *Spool) TotalBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var total int64
	for _, e := range s.entries {
		total += e.size
	}
	return total
}

// EvictBytes removes artifacts to free at least the requested number of bytes.
// Eviction order: ACKed artifacts first, then oldest unACKed.
// Returns the number of bytes freed.
func (s *Spool) EvictBytes(bytesToFree int64) int64 {
	s.mu.RLock()
	type candidate struct {
		workID  string
		size    int64
		acked   bool
		ackedAt time.Time
		created time.Time
	}
	candidates := make([]candidate, 0, len(s.entries))
	for workID, e := range s.entries {
		candidates = append(candidates, candidate{
			workID:  workID,
			size:    e.size,
			acked:   e.acked,
			ackedAt: e.ackedAt,
			created: e.created,
		})
	}
	s.mu.RUnlock()

	// Sort: ACKed first (oldest ACK first), then unACKed (oldest created first).
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].acked != candidates[j].acked {
			return candidates[i].acked
		}
		if candidates[i].acked {
			return candidates[i].ackedAt.Before(candidates[j].ackedAt)
		}
		return candidates[i].created.Before(candidates[j].created)
	})

	var freed int64
	for _, c := range candidates {
		if freed >= bytesToFree {
			break
		}
		s.Remove(c.workID)
		freed += c.size
	}

	if freed > 0 {
		slog.Info("Artifact spool eviction",
			slog.Int64("freed_bytes", freed),
			slog.Int64("requested_bytes", bytesToFree))
	}
	return freed
}

func (s *Spool) cleanupLoop() {
	defer close(s.done)
	ticker := time.NewTicker(CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.expireEntries()
		}
	}
}

func (s *Spool) expireEntries() {
	now := time.Now()
	var toRemove []string

	s.mu.RLock()
	for workID, e := range s.entries {
		if e.acked && now.Sub(e.ackedAt) > PostACKTTL {
			toRemove = append(toRemove, workID)
		} else if !e.acked && now.Sub(e.created) > UnACKedTTL {
			toRemove = append(toRemove, workID)
		}
	}
	s.mu.RUnlock()

	for _, workID := range toRemove {
		slog.Debug("Expiring artifact", slog.String("work_id", workID))
		s.Remove(workID)
	}
}
