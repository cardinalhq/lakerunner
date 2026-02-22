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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTestArtifact(t *testing.T, dir, workID string, content []byte) string {
	t.Helper()
	path := filepath.Join(dir, workID+".parquet")
	require.NoError(t, os.WriteFile(path, content, 0o644))
	return path
}

func expectedChecksum(data []byte) string {
	h := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(h[:])
}

func TestNewSpool_CreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "artifacts")
	s, err := NewSpool(dir)
	require.NoError(t, err)
	assert.NotNil(t, s)

	info, err := os.Stat(dir)
	require.NoError(t, err)
	assert.True(t, info.IsDir())
}

func TestSpool_RegisterAndGet(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	content := []byte("test parquet data")
	path := writeTestArtifact(t, dir, "w1", content)

	checksum, size, err := s.Register("w1", path)
	require.NoError(t, err)
	assert.Equal(t, expectedChecksum(content), checksum)
	assert.Equal(t, int64(len(content)), size)

	gotPath, gotChecksum, ok := s.Get("w1")
	assert.True(t, ok)
	assert.Equal(t, path, gotPath)
	assert.Equal(t, checksum, gotChecksum)
}

func TestSpool_GetNotFound(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)

	_, _, ok := s.Get("nonexistent")
	assert.False(t, ok)
}

func TestSpool_RegisterNonexistentFile(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)

	_, _, err = s.Register("w1", "/nonexistent/file.parquet")
	assert.Error(t, err)
}

func TestSpool_Acknowledge(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	path := writeTestArtifact(t, dir, "w1", []byte("data"))
	_, _, err = s.Register("w1", path)
	require.NoError(t, err)

	s.Acknowledge("w1")

	s.mu.RLock()
	e := s.entries["w1"]
	s.mu.RUnlock()
	assert.True(t, e.acked)
	assert.False(t, e.ackedAt.IsZero())
}

func TestSpool_AcknowledgeUnknown(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)
	// Should not panic.
	s.Acknowledge("nonexistent")
}

func TestSpool_Remove(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	content := []byte("data to remove")
	path := writeTestArtifact(t, dir, "w1", content)
	_, _, err = s.Register("w1", path)
	require.NoError(t, err)

	s.Remove("w1")

	_, _, ok := s.Get("w1")
	assert.False(t, ok)
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestSpool_RemoveUnknown(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)
	// Should not panic.
	s.Remove("nonexistent")
}

func TestSpool_CountAndTotalBytes(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	assert.Equal(t, 0, s.Count())
	assert.Equal(t, int64(0), s.TotalBytes())

	path1 := writeTestArtifact(t, dir, "w1", []byte("short"))
	path2 := writeTestArtifact(t, dir, "w2", []byte("longer content here"))

	_, _, err = s.Register("w1", path1)
	require.NoError(t, err)
	_, _, err = s.Register("w2", path2)
	require.NoError(t, err)

	assert.Equal(t, 2, s.Count())
	assert.Equal(t, int64(len("short")+len("longer content here")), s.TotalBytes())
}

func TestSpool_PathForWork(t *testing.T) {
	s, err := NewSpool("/tmp/spool")
	require.NoError(t, err)
	assert.True(t, strings.HasSuffix(s.PathForWork("w1"), "w1.parquet"))
	assert.True(t, strings.HasPrefix(s.PathForWork("w1"), "/tmp/spool/"))
}

func TestSpool_ExpireUnACKed(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	path := writeTestArtifact(t, dir, "w1", []byte("data"))
	_, _, err = s.Register("w1", path)
	require.NoError(t, err)

	// Backdate the entry.
	s.mu.Lock()
	s.entries["w1"].created = time.Now().Add(-UnACKedTTL - time.Second)
	s.mu.Unlock()

	s.expireEntries()

	assert.Equal(t, 0, s.Count())
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestSpool_ExpirePostACK(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	path := writeTestArtifact(t, dir, "w1", []byte("data"))
	_, _, err = s.Register("w1", path)
	require.NoError(t, err)

	s.Acknowledge("w1")

	// Backdate the ACK.
	s.mu.Lock()
	s.entries["w1"].ackedAt = time.Now().Add(-PostACKTTL - time.Second)
	s.mu.Unlock()

	s.expireEntries()

	assert.Equal(t, 0, s.Count())
}

func TestSpool_DoesNotExpireFresh(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	path := writeTestArtifact(t, dir, "w1", []byte("data"))
	_, _, err = s.Register("w1", path)
	require.NoError(t, err)

	s.expireEntries()

	assert.Equal(t, 1, s.Count())
}

func TestSpool_StartStop(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)

	s.Start()
	s.Stop()
	// Should not hang or panic.
}

func TestSpool_EvictBytes_ACKedFirst(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	// Register three artifacts.
	p1 := writeTestArtifact(t, dir, "w1", []byte("aaaaa"))     // 5 bytes
	p2 := writeTestArtifact(t, dir, "w2", []byte("bbbbbbbbb")) // 9 bytes
	p3 := writeTestArtifact(t, dir, "w3", []byte("ccc"))       // 3 bytes

	_, _, err = s.Register("w1", p1)
	require.NoError(t, err)
	_, _, err = s.Register("w2", p2)
	require.NoError(t, err)
	_, _, err = s.Register("w3", p3)
	require.NoError(t, err)

	// ACK w2 so it's evicted first.
	s.Acknowledge("w2")

	// Evict 10 bytes. Should remove w2 (9 bytes, ACKed) then w1 or w3 for the rest.
	freed := s.EvictBytes(10)
	assert.GreaterOrEqual(t, freed, int64(10))
	// w2 (ACKed) should be gone.
	_, _, ok := s.Get("w2")
	assert.False(t, ok)
}

func TestSpool_EvictBytes_NoEntries(t *testing.T) {
	s, err := NewSpool(t.TempDir())
	require.NoError(t, err)

	freed := s.EvictBytes(100)
	assert.Equal(t, int64(0), freed)
}

func TestSpool_EvictBytes_FreesExact(t *testing.T) {
	dir := t.TempDir()
	s, err := NewSpool(dir)
	require.NoError(t, err)

	p1 := writeTestArtifact(t, dir, "w1", []byte("12345678")) // 8 bytes
	_, _, err = s.Register("w1", p1)
	require.NoError(t, err)

	// Request to free 5 bytes. Will evict w1 (8 bytes, more than enough).
	freed := s.EvictBytes(5)
	assert.Equal(t, int64(8), freed)
	assert.Equal(t, 0, s.Count())
}
