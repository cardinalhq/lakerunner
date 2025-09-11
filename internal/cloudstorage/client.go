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

	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// Client provides a unified interface for cloud storage operations across different providers
type Client interface {
	// DownloadObject downloads an object from cloud storage to a local file
	// Returns the temp filename, size, whether object was not found, and error
	DownloadObject(ctx context.Context, tmpdir, bucket, key string) (filename string, size int64, notFound bool, err error)

	// UploadObject uploads a local file to cloud storage
	UploadObject(ctx context.Context, bucket, key, sourceFilename string) error

	// DeleteObject deletes an object from cloud storage
	DeleteObject(ctx context.Context, bucket, key string) error

	// DeleteObjects deletes multiple objects from cloud storage in a single batch operation
	// Returns any keys that failed to delete and any error encountered
	DeleteObjects(ctx context.Context, bucket string, keys []string) (failed []string, err error)
}

// ClientProvider creates cloud storage clients based on a storage profile.
// Implementations may target real cloud providers or local filesystem mocks
// used for testing.
type ClientProvider interface {
	NewClient(ctx context.Context, profile storageprofile.StorageProfile) (Client, error)
}

// NewClient delegates client creation to the provided ClientProvider. It is a
// convenience wrapper to preserve existing call sites that expect a package
// level function.
func NewClient(ctx context.Context, provider ClientProvider, profile storageprofile.StorageProfile) (Client, error) {
	return provider.NewClient(ctx, profile)
}
