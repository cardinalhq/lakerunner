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

package buffet

import (
	"container/heap"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/cardinalhq/lakerunner/internal/helpers"
)

const maxRowsSortBuffer = 5000

type SplitKey struct {
	DateInt       int32
	Hour          int16
	IngestDateint int32
	FileIndex     int16 // Support multiple files per hour (0, 1, 2, ...)
}

type HourlyResult struct {
	FileName     string
	FileSize     int64
	RecordCount  int64
	Fingerprints mapset.Set[int64]
	FirstTS      int64
	LastTS       int64
}

type gs struct {
	chunks  []string
	buf     []map[string]any
	firstTS int64
	lastTS  int64
}

// ProcessAndSplit reads every record from fh, groups rows by dateint,
// fingerprints them, buffers each group to disk, sorts each buffer by
// `_cardinalhq.timestamp`, and finally merge-sorts the buffers into Parquet
// files in time order.
func ProcessAndSplit(ll *slog.Logger, fh *filecrunch.FileHandle, tmpdir string, ingestDateint int32, rpfEstimate int64) (map[SplitKey]HourlyResult, error) {
	groups := make(map[SplitKey]*gs)

	// 1st pass: read input and write sorted chunks to disk per group.
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
		ms := getMS(tsRaw)
		dateint, hour := helpers.MSToDateintHour(ms)
		key := SplitKey{DateInt: dateint, Hour: hour, IngestDateint: ingestDateint, FileIndex: 0}

		st, exists := groups[key]
		if !exists {
			st = &gs{
				buf: make([]map[string]any, 0, maxRowsSortBuffer),
			}
			groups[key] = st
		}
		// copy row for buffering
		row := make(map[string]any, len(rec))
		for k, v := range rec {
			row[k] = v
		}
		st.buf = append(st.buf, row)
		if len(st.buf) >= maxRowsSortBuffer {
			if err := flushChunk(st, tmpdir); err != nil {
				return nil, err
			}
		}
		if st.firstTS == 0 || ms < st.firstTS {
			st.firstTS = ms
		}
		if ms > st.lastTS {
			st.lastTS = ms
		}
	}

	// flush remaining buffers
	for _, st := range groups {
		if len(st.buf) > 0 {
			if err := flushChunk(st, tmpdir); err != nil {
				return nil, err
			}
		}
	}

	// 2nd pass: merge chunks into Parquet files.
	results := make(map[SplitKey]HourlyResult)
	for key, st := range groups {
		if len(st.chunks) == 0 {
			continue
		}
		w, err := NewWriter(fh.File.Name(), tmpdir, fh.Nodes, rpfEstimate)
		if err != nil {
			return nil, err
		}
		res, err := mergeChunks(w, st.chunks)
		if err != nil {
			return nil, err
		}
		if len(res) == 0 {
			continue
		}

		// Handle multiple files from buffet writer
		for i, fileResult := range res {
			fileKey := key
			fileKey.FileIndex = int16(i)

			results[fileKey] = HourlyResult{
				FileName:     fileResult.FileName,
				RecordCount:  fileResult.RecordCount,
				FileSize:     fileResult.FileSize,
				Fingerprints: fileResult.Fingerprints,
				FirstTS:      st.firstTS, // All files share same time range bounds
				LastTS:       st.lastTS,
			}
		}
	}
	return results, nil
}

func getMS(v any) int64 {
	switch ts := v.(type) {
	case int64:
		return ts
	case float64:
		return int64(ts)
	default:
		return 0
	}
}

func flushChunk(st *gs, tmpdir string) error {
	sort.Slice(st.buf, func(i, j int) bool {
		return getMS(st.buf[i]["_cardinalhq.timestamp"]) < getMS(st.buf[j]["_cardinalhq.timestamp"])
	})
	f, err := os.CreateTemp(tmpdir, "tschunk-*.gob")
	if err != nil {
		return err
	}
	enc := gob.NewEncoder(f)
	for _, r := range st.buf {
		if err := enc.Encode(r); err != nil {
			_ = f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	st.chunks = append(st.chunks, f.Name())
	st.buf = st.buf[:0]
	return nil
}

type chunkReader struct {
	f   *os.File
	dec *gob.Decoder
	row map[string]any
}

func newChunkReader(path string) (*chunkReader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	cr := &chunkReader{f: f, dec: gob.NewDecoder(f)}
	if err := cr.next(); err != nil {
		if errors.Is(err, io.EOF) {
			_ = f.Close()
			return nil, nil
		}
		_ = f.Close()
		return nil, err
	}
	return cr, nil
}

func (c *chunkReader) next() error {
	var m map[string]any
	if err := c.dec.Decode(&m); err != nil {
		return err
	}
	c.row = m
	return nil
}

func (c *chunkReader) Close() error { return c.f.Close() }

type chunkHeap []*chunkReader

func (h chunkHeap) Len() int { return len(h) }
func (h chunkHeap) Less(i, j int) bool {
	return getMS(h[i].row["_cardinalhq.timestamp"]) < getMS(h[j].row["_cardinalhq.timestamp"])
}
func (h chunkHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *chunkHeap) Push(x any) {
	*h = append(*h, x.(*chunkReader))
}

func (h *chunkHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[:n-1]
	return x
}

func mergeChunks(w *Writer, paths []string) ([]Result, error) {
	h := &chunkHeap{}
	for _, p := range paths {
		cr, err := newChunkReader(p)
		if err != nil {
			return nil, err
		}
		if cr != nil {
			heap.Push(h, cr)
		}
	}
	for h.Len() > 0 {
		cr := heap.Pop(h).(*chunkReader)
		if err := w.Write(cr.row); err != nil {
			_ = cr.Close()
			return nil, err
		}
		if err := cr.next(); err != nil {
			if !errors.Is(err, io.EOF) {
				_ = cr.Close()
				return nil, err
			}
			_ = cr.Close()
		} else {
			heap.Push(h, cr)
		}
	}
	res, err := w.Close()
	for _, p := range paths {
		_ = os.Remove(p)
	}
	return res, err
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
