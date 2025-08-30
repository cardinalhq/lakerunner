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

package objstore

import (
	"context"
	"io"
)

// ObjectStore provides simple get/put/delete operations for object storage.
// This is a minimal interface that can be implemented by different storage backends.
type ObjectStore interface {
	// Get retrieves an object and returns a reader for its contents
	Get(ctx context.Context, bucket, key string) (io.ReadCloser, error)

	// Put stores an object from the provided reader
	Put(ctx context.Context, bucket, key string, reader io.Reader) error

	// Delete removes an object
	Delete(ctx context.Context, bucket, key string) error

	// Exists checks if an object exists (returns true, nil if exists)
	Exists(ctx context.Context, bucket, key string) (bool, error)

	// IsNotFoundError checks if an error indicates the object was not found
	IsNotFoundError(err error) bool
}

// ObjectInfo contains metadata about an object
type ObjectInfo struct {
	Key          string
	Size         int64
	LastModified int64
	ETag         string
}

// ExtendedObjectStore provides additional operations beyond basic get/put/delete
type ExtendedObjectStore interface {
	ObjectStore

	// List objects with a given prefix
	List(ctx context.Context, bucket, prefix string) ([]ObjectInfo, error)

	// GetInfo retrieves metadata about an object without downloading it
	GetInfo(ctx context.Context, bucket, key string) (*ObjectInfo, error)
}
