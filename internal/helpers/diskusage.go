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
