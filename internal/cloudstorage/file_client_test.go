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

package cloudstorage

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/stretchr/testify/require"
)

func TestFileClientLifecycle(t *testing.T) {
	base := t.TempDir()
	provider := NewFileClientProvider(base)
	client, err := provider.NewClient(context.Background(), storageprofile.StorageProfile{})
	require.NoError(t, err)

	// Create source file
	src := filepath.Join(base, "src.txt")
	require.NoError(t, os.WriteFile(src, []byte("hello"), 0o644))

	// Upload to bucket/key
	require.NoError(t, client.UploadObject(context.Background(), "bucket", "path/file.txt", src))

	// Download and verify
	tmp := t.TempDir()
	dst, size, notFound, err := client.DownloadObject(context.Background(), tmp, "bucket", "path/file.txt")
	require.NoError(t, err)
	require.False(t, notFound)
	require.Equal(t, int64(5), size)
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	require.Equal(t, "hello", string(data))

	// Delete
	require.NoError(t, client.DeleteObject(context.Background(), "bucket", "path/file.txt"))
	_, _, notFound, err = client.DownloadObject(context.Background(), tmp, "bucket", "path/file.txt")
	require.NoError(t, err)
	require.True(t, notFound)
}
