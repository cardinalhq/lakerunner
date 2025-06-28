// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package helpers

import (
	"golang.org/x/sys/unix"
)

// FSUsage holds the on-disk usage stats for a given filesystem.
type FSUsage struct {
	// Bytes
	TotalBytes uint64 // total capacity (in bytes)
	FreeBytes  uint64 // bytes available to non-root users
	UsedBytes  uint64 // bytes currently in use  (TotalBytes - FreeBytes)

	// Inodes
	TotalInodes uint64 // total number of inodes
	FreeInodes  uint64 // number of free inodes
	UsedInodes  uint64 // number of used inodes (TotalInodes - FreeInodes)
}

// DiskUsage returns FSUsage for the filesystem that contains 'path'.
// It fills in total/free/used for both bytes and inodes.
// If an error occurs (e.g. path doesn’t exist), err will be non-nil.
func DiskUsage(path string) (FSUsage, error) {
	var st unix.Statfs_t
	if err := unix.Statfs(path, &st); err != nil {
		return FSUsage{}, err
	}

	totalBytes := st.Blocks * uint64(st.Bsize)
	freeBytes := st.Bavail * uint64(st.Bsize)
	usedBytes := totalBytes - freeBytes

	totalInodes := st.Files
	freeInodes := st.Ffree
	usedInodes := totalInodes - freeInodes

	return FSUsage{
		TotalBytes:  totalBytes,
		FreeBytes:   freeBytes,
		UsedBytes:   usedBytes,
		TotalInodes: totalInodes,
		FreeInodes:  freeInodes,
		UsedInodes:  usedInodes,
	}, nil
}
