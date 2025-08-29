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

package constants

const (
	TargetFileSizeBytes          = int64(1_100_000)        // 1.1MB target file size
	WriterTargetSizeBytesLogs    = int64(50 * 1024 * 1024) // 50MB for logs writer
	WriterTargetSizeBytesMetrics = int64(1024 * 1024)      // 1MB for metrics writer
	MaxLineSizeBytes             = 1024 * 1024             // 1MB max line size for scanners
	HTTPBodyLimitBytes           = int64(1024 * 1024)      // 1MB HTTP request body limit
)
