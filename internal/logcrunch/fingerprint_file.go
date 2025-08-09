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

package logcrunch

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
)

type SplitKey struct {
	DateInt       int32
	IngestDateint int32
}

type HourlyResult struct {
	FileName     string
	FileSize     int64
	RecordCount  int64
	Fingerprints mapset.Set[int64]
	FirstTS      int64
	LastTS       int64
}

// ProcessAndSplit reads every record from fh, groups rows by dateint,
// fingerprints them, buffers each group to disk (so we never buffer in RAM),
// then does a second pass to derive a minimal schema and write Parquet files.
func ProcessAndSplit(ll *slog.Logger, fh *filecrunch.FileHandle, tmpdir string, ingestDateint int32, rpf_estimate int64) (map[SplitKey]HourlyResult, error) {
	type gs struct {
		writer  *buffet.Writer
		prints  mapset.Set[int64]
		firstTS int64
		lastTS  int64
	}
	groups := make(map[SplitKey]*gs)

	// 1st pass: read input and feed buffet writers
	reader := parquet.NewReader(fh.File, fh.Schema)
	defer reader.Close()
	for {
		rec := map[string]any{}
		if err := reader.Read(&rec); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("reading parquet: %w", err)
		}
		tsRaw, ok := rec["_cardinalhq.timestamp"]
		if !ok || tsRaw == nil {
			ll.Warn("Skipping record without timestamp", slog.Any("record", rec))
			continue
		}
		var ms int64
		switch v := tsRaw.(type) {
		case int64:
			ms = v
		case float64:
			ms = int64(v)
		default:
			ll.Warn("Skipping record with non-int64/float64 timestamp", slog.Any("record", rec))
			continue
		}
		dateint, _ := helpers.MSToDateintHour(ms)
		key := SplitKey{DateInt: dateint, IngestDateint: ingestDateint}

		st, exists := groups[key]
		if !exists {
			w, err := buffet.NewWriter(fh.File.Name(), tmpdir, fh.Nodes, rpf_estimate)
			if err != nil {
				return nil, err
			}
			st = &gs{
				writer: w,
				prints: mapset.NewSet[int64](),
			}
			groups[key] = st
		}
		if st.firstTS == 0 {
			st.firstTS = ms
		}
		if ms < st.firstTS {
			st.firstTS = ms
		}
		if ms > st.lastTS {
			st.lastTS = ms
		}

		// feed the row
		if err := st.writer.Write(rec); err != nil {
			return nil, err
		}

		addfp(fh.Schema, rec, st.prints)
	}

	// 2nd pass: close each buffet.Writer → get parquet + metadata
	results := make(map[SplitKey]HourlyResult, len(groups))
	for key, st := range groups {
		res, err := st.writer.Close()
		if err != nil {
			return nil, err
		}
		if len(res) == 0 {
			// No records for this key, skip it
			continue
		}
		results[key] = HourlyResult{
			FileName:     res[0].FileName,
			RecordCount:  res[0].RecordCount,
			FileSize:     res[0].FileSize,
			Fingerprints: st.prints,
			FirstTS:      st.firstTS,
			LastTS:       st.lastTS,
		}
	}
	return results, nil
}

func addfp(schema *parquet.Schema, row map[string]any, accum mapset.Set[int64]) {
	vals := make(map[string]mapset.Set[string])
	for _, cols := range schema.Columns() {
		for _, col := range cols {
			vals[col] = mapset.NewSet[string]()
		}
	}

	seenrows := map[string]struct{}{}
	for k, v := range row {
		if v != nil {
			if !slices.Contains(DimensionsToIndex, k) && asString(v) != "" {
				vals[k].Add(ExistsRegex)
				seenrows[k] = struct{}{}
				continue
			}
			s := asString(v)
			if s == "" {
				continue
			}
			vals[k].Add(s)
			seenrows[k] = struct{}{}

		}
	}

	// remove any columns in vals that are not in seenrows
	for k := range vals {
		if _, ok := seenrows[k]; !ok {
			delete(vals, k)
		}
	}

	for f := range ToFingerprints(vals).Iter() {
		accum.Add(f)
	}
}

func asString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case int, int8, int16, int32, int64:
		return fmt.Sprintf("%d", val)
	case float32, float64:
		return fmt.Sprintf("%f", val)
	default:
		return fmt.Sprintf("%v", v)
	}
}
