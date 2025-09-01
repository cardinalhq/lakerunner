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

package compaction

import (
	"log/slog"

	"github.com/cardinalhq/lakerunner/internal/metricsprocessing"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
)

type input struct {
	ReaderStack  *metricsprocessing.ReaderStackResult
	FrequencyMs  int64
	TmpDir       string
	Logger       *slog.Logger
	RecordsLimit int64
}

type result struct {
	Results     []parquetwriter.Result
	TotalRows   int64
	OutputBytes int64
}

type stats struct {
	TotalRows        int64
	OutputFiles      int
	InputFiles       int
	InputBytes       int64
	OutputBytes      int64
	CompressionRatio float64
}
