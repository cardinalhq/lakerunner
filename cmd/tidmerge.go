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

package cmd

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"

	"slices"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/parquet-go/parquet-go"
)

// TIDMerger rolls up multiple Parquet files into multiple files.
// It is expected that the files are already sorted by TID, so
// reading from them will yield rows in TID order.

type MergeStats struct {
	DatapointsOutOfRange int64
}

type TIDMerger struct {
	files        []string
	tmpdir       string
	nodes        map[string]parquet.Node
	targetSize   int64
	sizeEstimate int64
	interval     int32
	startTS      int64
	endTS        int64

	stats MergeStats
}

type WriteResult struct {
	FileName    string
	RecordCount int64
	FileSize    int64
	TidCount    int32 // number of unique TIDs in the file
}

// NewTIDMerger creates a new TIDMerger instance.
// the interval must be exactly one interval between startTS and endTS.
// startTS and endTS are used to determine the timestamp of the merged rows.
// StartTS is inclusive, endTS is exclusive.0
func NewTIDMerger(tmpdir string, files []string, interval int32, targetSize int64, sizeEstimate int64, startTS int64, endTS int64) (*TIDMerger, error) {
	tslen := endTS - startTS
	if tslen%int64(interval) != 0 {
		return nil, fmt.Errorf("startTS %d and endTS %d must be aligned with interval %d", startTS, endTS, interval)
	}
	return &TIDMerger{
		files:        files,
		interval:     interval,
		targetSize:   targetSize,
		sizeEstimate: sizeEstimate,
		tmpdir:       tmpdir,
		nodes:        make(map[string]parquet.Node),
		startTS:      startTS,
		endTS:        endTS,
	}, nil
}

func (m *TIDMerger) validate() error {
	if len(m.files) == 0 {
		return errors.New("no files to merge in TIDMerger")
	}
	if slices.Contains(m.files, "") {
		return errors.New("empty file name in TIDMerger")
	}
	return nil
}

// readerState holds a parquet reader, an in-memory buffer of rows,
// and exposes the current row and its TID.
const readBatchSize = 16

type readerState struct {
	reader   *parquet.GenericReader[map[string]any]
	buffer   []map[string]any
	idx      int
	closed   bool
	fileName string

	current    map[string]any
	currentTID int64
}

// loadNextBatch reads up to readBatchSize rows into buffer, updates current/currentTID,
// and marks closed if EOF is reached.
func (rs *readerState) loadNextBatch() error {
	if rs.closed {
		return nil
	}

	temp := make([]map[string]any, readBatchSize)
	for i := range readBatchSize {
		temp[i] = make(map[string]any)
	}
	n, err := rs.reader.Read(temp)
	if n == 0 && errors.Is(err, io.EOF) {
		rs.closed = true
		rs.buffer = nil
		rs.current = nil
		return nil
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("failed reading parquet batch from %s: %w", rs.fileName, err)
	}

	if n == 0 {
		rs.closed = true
		rs.buffer = nil
		rs.current = nil
		return nil
	}

	rs.buffer = temp[:n]
	rs.idx = 0
	rs.current = rs.buffer[0]
	tidVal, ok := rs.current["_cardinalhq.tid"].(int64)
	if !ok {
		return fmt.Errorf("file %s: row does not contain a valid int64 _cardinalhq.tid", rs.fileName)
	}
	rs.currentTID = tidVal
	return nil
}

// advance moves to the next row in buffer or loads the next batch if needed.
func (rs *readerState) advance() error {
	if rs.closed {
		return nil
	}
	rs.idx++
	if rs.idx < len(rs.buffer) {
		rs.current = rs.buffer[rs.idx]
		tidVal, ok := rs.current["_cardinalhq.tid"].(int64)
		if !ok {
			return fmt.Errorf("file %s: row does not contain a valid int64 _cardinalhq.tid", rs.fileName)
		}
		rs.currentTID = tidVal
		return nil
	}
	return rs.loadNextBatch()
}

// Merge opens all input files, merges rows sorted by TID, and writes merged output.
func (m *TIDMerger) Merge() ([]WriteResult, MergeStats, error) {
	if err := m.validate(); err != nil {
		return nil, m.stats, fmt.Errorf("invalid merge config: %w", err)
	}

	infiles := make([]*filecrunch.FileHandle, 0, len(m.files))
	defer func() {
		for _, fh := range infiles {
			if err := fh.Close(); err != nil {
				slog.Error("Failed to close file handle", "file", fh.File.Name(), "error", err)
			}
		}
	}()

	readers := make([]*parquet.GenericReader[map[string]any], 0, len(m.files))
	defer func() {
		for _, rdr := range readers {
			if rdr != nil {
				if err := rdr.Close(); err != nil {
					slog.Error("Failed to close parquet reader", "error", err)
				}
			}
		}
	}()

	nodeBuilder := buffet.NewNodeMapBuilder()
	for _, file := range m.files {
		fh, err := filecrunch.LoadSchemaForFile(file)
		if err != nil {
			return nil, m.stats, fmt.Errorf("failed to load schema for file %s: %w", file, err)
		}
		infiles = append(infiles, fh)

		reader := parquet.NewGenericReader[map[string]any](fh.File, fh.Schema)
		readers = append(readers, reader)

		if err := nodeBuilder.AddNodes(fh.Nodes); err != nil {
			return nil, m.stats, fmt.Errorf("failed to add nodes for file %s: %w", file, err)
		}
	}
	// ensure that we have the schema we will be writing
	if err := nodeBuilder.Add(map[string]any{
		"_cardinalhq.tid":       int64(0),
		"_cardinalhq.timestamp": int64(0),
		"_cardinalhq.name":      "",
		"sketch":                []byte{},
		"rollup_count":          float64(0),
		"rollup_sum":            float64(0),
		"rollup_avg":            float64(0),
		"rollup_max":            float64(0),
		"rollup_min":            float64(0),
		"rollup_p25":            float64(0),
		"rollup_p50":            float64(0),
		"rollup_p75":            float64(0),
		"rollup_p90":            float64(0),
		"rollup_p95":            float64(0),
		"rollup_p99":            float64(0),
	}); err != nil {
		return nil, m.stats, fmt.Errorf("failed to add rollup/sketch nodes for writing: %w", err)
	}

	m.nodes = nodeBuilder.Build()

	writer, err := buffet.NewWriter("tid-merger-*", m.tmpdir, m.nodes, m.targetSize, m.sizeEstimate,
		buffet.WithGroupFunc(GroupTIDGroupFunc),
		buffet.WithStatsProvider(&TidAccumulatorProvider{}),
	)
	if err != nil {
		return nil, m.stats, fmt.Errorf("failed to create writer: %w", err)
	}
	defer func() { writer.Abort() }()

	states := make([]*readerState, len(readers))
	for i, rdr := range readers {
		states[i] = &readerState{
			reader:   rdr,
			buffer:   nil,
			idx:      0,
			closed:   false,
			fileName: m.files[i],
		}
	}

	for _, rs := range states {
		if err := rs.loadNextBatch(); err != nil {
			return nil, m.stats, err
		}
	}

	for {
		smallest := int64(math.MaxInt64)
		foundAny := false
		for _, rs := range states {
			if rs.closed {
				continue
			}
			if rs.currentTID < smallest {
				smallest = rs.currentTID
				foundAny = true
			}
		}
		if !foundAny {
			break
		}

		groupedRows := make([]map[string]any, 0, len(states))
		for _, rs := range states {
			if rs.closed {
				continue
			}
			// Collect all rows from this reader with the current smallest TID
			for !rs.closed && rs.currentTID == smallest {
				groupedRows = append(groupedRows, rs.current)
				if err := rs.advance(); err != nil {
					return nil, m.stats, err
				}
			}
		}

		combinedRows := m.mergeRows(groupedRows)
		if len(combinedRows) > 0 {
			for _, row := range combinedRows {
				if err := writer.Write(row); err != nil {
					return nil, m.stats, fmt.Errorf("failed to write row: %w", err)
				}
			}
		}
	}

	results, err := writer.Close()
	if err != nil {
		return nil, m.stats, fmt.Errorf("error closing writer: %w", err)
	}

	ret := make([]WriteResult, 0, len(results))
	for _, res := range results {
		var stats TidAccumulatorResult
		stats, _ = res.Stats.(TidAccumulatorResult)
		ret = append(ret, WriteResult{
			FileName:    res.FileName,
			RecordCount: res.RecordCount,
			FileSize:    res.FileSize,
			TidCount:    int32(stats.Cardinality),
		})
	}

	return ret, m.stats, nil
}

type mergekey struct {
	tid       int64
	timestamp int64
	name      string
}

type mergeaccumulator struct {
	row           map[string]any
	sketch        *ddsketch.DDSketch
	contributions int
}

func tsInRange(ts, startTS, endTS int64) bool {
	return ts >= startTS && ts < endTS
}

// mergeRows combines multiple rows with the same TID into a single row.
func (m *TIDMerger) mergeRows(rows []map[string]any) []map[string]any {
	if len(rows) <= 1 {
		ts, ok := getTimestampFromRecord(rows[0])
		if !ok {
			slog.Error("Row does not contain a valid int64 _cardinalhq.timestamp", "row", rows[0])
			return nil // invalid row, cannot merge
		}
		if !tsInRange(ts, m.startTS, m.endTS) {
			m.stats.DatapointsOutOfRange++
			slog.Warn("Row timestamp out of range", "timestamp", ts, "startTS", m.startTS, "endTS", m.endTS)
			return nil // out of range, cannot merge
		}
		rows[0]["_cardinalhq.timestamp"] = ts / int64(m.interval) * int64(m.interval) // align to interval
		return rows
	}

	merged := make(map[mergekey]*mergeaccumulator)
	for _, row := range rows {
		rowTS, ok := getTimestampFromRecord(row)
		if !ok {
			slog.Error("Row does not contain a valid int64 _cardinalhq.timestamp", "row", row)
			continue
		}

		if !tsInRange(rowTS, m.startTS, m.endTS) {
			m.stats.DatapointsOutOfRange++
			continue
		}

		key, sketchBytes, err := makekey(row, int32(m.interval))
		if err != nil {
			slog.Error("Failed to make key for row", "error", err, "row", row)
			continue
		}
		sketch, err := DecodeSketch(sketchBytes)
		if err != nil {
			slog.Error("Failed to decode sketch", "error", err)
			continue
		}

		if _, exists := merged[key]; !exists {
			merged[key] = &mergeaccumulator{
				row:           row,
				contributions: 1,
			}
			merged[key].sketch = sketch
		} else {
			// accumulate contributions
			merged[key].contributions++
			if err := merged[key].sketch.MergeWith(sketch); err != nil {
				slog.Error("Failed to merge sketch", "error", err)
				continue
			}
		}
	}

	result := make([]map[string]any, 0, len(merged))
	for key, acc := range merged {
		if acc.contributions > 1 {
			if err := updateFromSketch(acc); err != nil {
				slog.Error("Failed to update row from sketch", "error", err)
				continue
			}
		}

		acc.row["_cardinalhq.timestamp"] = key.timestamp
		result = append(result, acc.row)
	}

	return result
}

func updateFromSketch(acc *mergeaccumulator) error {
	rollupCountIn := acc.sketch.GetCount()
	rollupSumIn := acc.sketch.GetSum()

	acc.row["rollup_count"] = rollupCountIn
	acc.row["rollup_sum"] = rollupSumIn
	acc.row["rollup_avg"] = rollupSumIn / rollupCountIn

	rollupMaxIn, err := acc.sketch.GetMaxValue()
	if err != nil {
		return fmt.Errorf("getting max value from sketch: %w", err)
	}
	acc.row["rollup_max"] = rollupMaxIn

	rollupMinIn, err := acc.sketch.GetMinValue()
	if err != nil {
		return fmt.Errorf("getting min value from sketch: %w", err)
	}
	acc.row["rollup_min"] = rollupMinIn

	quantiles, err := acc.sketch.GetValuesAtQuantiles([]float64{0.25, 0.50, 0.75, 0.90, 0.95, 0.99})
	if err != nil {
		return fmt.Errorf("getting quantiles from sketch: %w", err)
	}
	acc.row["rollup_p25"] = quantiles[0]
	acc.row["rollup_p50"] = quantiles[1]
	acc.row["rollup_p75"] = quantiles[2]
	acc.row["rollup_p90"] = quantiles[3]
	acc.row["rollup_p95"] = quantiles[4]
	acc.row["rollup_p99"] = quantiles[5]

	acc.row["sketch"] = EncodeSketch(acc.sketch)

	return nil
}

var (
	ErrorInvalidSketchType = errors.New("invalid sketch type, expected []byte or string")
	ErrorInvalidTID        = errors.New("record does not contain a valid int64 _cardinalhq.tid")
	ErrorInvalidTimestamp  = errors.New("record does not contain a valid int64 _cardinalhq.timestamp")
	ErrorInvalidName       = errors.New("record does not contain a valid string _cardinalhq.name")
)

func getTimestampFromRecord(rec map[string]any) (int64, bool) {
	timestamp, ok := rec["_cardinalhq.timestamp"].(int64)
	return timestamp, ok
}

func makekey(rec map[string]any, interval int32) (key mergekey, sketchBytes []byte, err error) {
	tid, ok := rec["_cardinalhq.tid"].(int64)
	if !ok {
		return key, nil, ErrorInvalidTID
	}
	timestamp, ok := getTimestampFromRecord(rec)
	if !ok {
		return key, nil, ErrorInvalidTimestamp
	}
	name, ok := rec["_cardinalhq.name"].(string)
	if !ok {
		return key, nil, ErrorInvalidName
	}
	sketchVal, ok := rec["sketch"]
	if !ok {
		return key, nil, ErrorInvalidSketchType
	}
	switch v := sketchVal.(type) {
	case []byte:
		sketchBytes = v
	case string:
		sketchBytes = []byte(v)
	default:
		return key, nil, ErrorInvalidSketchType
	}

	return mergekey{
		tid:       tid,
		timestamp: (timestamp / int64(interval)) * int64(interval), // start of interval
		name:      name,
	}, sketchBytes, nil
}

func GroupTIDGroupFunc(prev, current map[string]any) bool {
	ptid, ok := prev["_cardinalhq.tid"].(int64)
	if !ok {
		return false
	}
	ctid, ok := current["_cardinalhq.tid"].(int64)
	if !ok {
		return false
	}
	return ptid == ctid
}

func calculateTargetRecordsPerFile(recordCount int, estimatedBytesPerRecord int, targetFileSize int64) int64 {
	if recordCount <= 0 || estimatedBytesPerRecord <= 0 || targetFileSize <= 0 {
		return 0
	}

	totalRecords := int64(recordCount)
	eBPR := int64(estimatedBytesPerRecord)
	totalSize := totalRecords * eBPR
	if totalSize <= targetFileSize {
		return totalRecords
	}
	filesNeeded := min((totalSize+targetFileSize-1)/targetFileSize, totalRecords)
	return (totalRecords + filesNeeded - 1) / filesNeeded
}
