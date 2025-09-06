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

package ingestion

import (
	"github.com/cardinalhq/lakerunner/internal/exemplar"
	"github.com/cardinalhq/lakerunner/internal/parquetwriter"
	"github.com/cardinalhq/lakerunner/internal/processing/ingest"
)

// minuteSlotKey uniquely identifies a writer for a specific 60-second boundary
type minuteSlotKey struct {
	dateint int32
	minute  int // minute within the day (0-1439)
	slot    int
}

// input contains all parameters needed for metric ingestion
type input struct {
	Items             []ingest.IngestItem
	TmpDir            string
	IngestDateint     int32
	RPFEstimate       int64
	ExemplarProcessor *exemplar.Processor
	Config            Config
}

// result contains the output of metric ingestion
type result struct {
	Results     []parquetwriter.Result
	RowsRead    int64
	RowsErrored int64
}

// fileInfo holds information about a downloaded file
type fileInfo struct {
	item        ingest.IngestItem
	tmpfilename string
}
