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

// Package buffet provides a buffered Parquet writer that
//  1. gob-spills every row to disk,
//  2. remembers which columns ever had non-nil values,
//  3. on Close(), splits buffered rows into one or more Parquet files
//     (based on max rows per file and an optional grouping function),
//     streams rows back out into Parquet files, and returns a slice of Results.
//
// The minimal schema is built from any column written to the Writer with non-nil values.
// This means that, depending on the pattern of your written data and how the files are
// split on output, a file may have columns that are entirely empty.  As we use
// this as a final stage output from reading many input files, this is usually not a
// problem, as the input file's records will typically be spread throughout the output files,
// resulting in all the columns being used anyway.
package buffet

import (
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"

	"github.com/parquet-go/parquet-go"
)

// GroupFunc determines whether two consecutive rows must be written to the same Parquet file.
// Return true to force previous and current rows into the same file, even if maxRowsPerFile is reached.
type GroupFunc func(prev, current map[string]any) bool

// Define the per-file accumulator interface:
type StatsAccumulator interface {
	// Add is called once per row.
	Add(row map[string]any)
	// Finalize is called exactly once, after the last row.
	// It can return any summary type (e.g. a struct, map, number…).
	Finalize() any
}

// Define the provider interface that the caller implements:
type StatsProvider interface {
	// NewAccumulator is called whenever you open a new Parquet file.
	NewAccumulator() StatsAccumulator
}

const FileSizeUnavailable = int64(-1)

type Writer struct {
	// spill
	tmp     *os.File
	encoder *gob.Encoder
	// state for gob spill
	count int64
	// for schema and parquet output
	baseName                string
	schemaNodes             map[string]parquet.Node
	tmpdir                  string
	targetSize              int64
	estimatedBytesPerRecord int64
	groupFunc               GroupFunc
	closed                  bool
	seenCols                map[string]bool
	keepCols                []string
	statsProv               StatsProvider
	currentAcc              StatsAccumulator
}

type Result struct {
	FileName    string // path to the .parquet file, in temp dir
	RecordCount int64  // how many rows in this file
	FileSize    int64  // size of the Parquet file in bytes
	Stats       any    // stats from StatsAccumulator, if provided
}

func init() {
	// Register the concrete types we expect in record values.
	gob.Register(map[string]any{})
	gob.Register(int64(0))
	gob.Register(float64(0))
	gob.Register(string(""))
	gob.Register(bool(false))
}

type WriterOption interface {
	apply(*Writer)
}

func WithGroupFunc(f GroupFunc) WriterOption {
	return &groupFuncOption{f: f}
}

type groupFuncOption struct {
	f GroupFunc
}

func (o *groupFuncOption) apply(w *Writer) {
	w.groupFunc = o.f
}

func WithStatsProvider(p StatsProvider) WriterOption {
	return &statsProviderOption{p: p}
}

type statsProviderOption struct {
	p StatsProvider
}

func (o *statsProviderOption) apply(w *Writer) {
	w.statsProv = o.p
}

// WithStatsAccumulator is a convenience function to use a StatsProvider with a single accumulator.
// NewWriter sets up a new spill file and remembers your base schema.
// baseName is used as the “row group name” in NewSchema (for metadata).
// maxRowsPerFile controls how many rows to write per Parquet file. If <= 0, all rows go into a single file.
// groupFunc can be nil; if non-nil, it is used to force rows into the same file even if maxRowsPerFile is reached.
// Writers are not concurrency-safe.
//
// All files are created in tmpdir, and this directory must be cleaned up by the caller.
// Files will remain in tmpdir both after Close() and if an error occurs.
// The common pattern is to create a per-work-item tmpdir, use it, and remove the
// entire tmpdir when all files are no longer needed.
func NewWriter(
	baseName string,
	tmpdir string,
	schemaNodes map[string]parquet.Node,
	targetSize int64,
	estimatedBytesPerRecord int64,
	opts ...WriterOption,
) (*Writer, error) {
	tmp, err := os.CreateTemp(tmpdir, "buffet-*.gob")
	if err != nil {
		return nil, fmt.Errorf("buffet: create temp spill file: %w", err)
	}

	for name, node := range schemaNodes {
		if name == "" {
			return nil, fmt.Errorf("buffet: schema node name cannot be empty")
		}
		if node == nil {
			return nil, fmt.Errorf("buffet: schema node %q cannot be nil", name)
		}
	}

	writer := &Writer{
		tmp:                     tmp,
		encoder:                 gob.NewEncoder(tmp),
		baseName:                baseName,
		schemaNodes:             schemaNodes,
		tmpdir:                  tmpdir,
		targetSize:              targetSize,
		estimatedBytesPerRecord: estimatedBytesPerRecord,
		seenCols:                make(map[string]bool),
	}

	for _, opt := range opts {
		opt.apply(writer)
	}

	return writer, nil
}

// Write gob-encodes one record (map[string]any) and increments the spill count.
func (w *Writer) Write(row map[string]any) error {
	if w.closed {
		return ErrAlreadyClosed
	}

	// ensure we don't have any columns that are not in the schema
	for k := range row {
		if _, ok := w.schemaNodes[k]; !ok {
			return fmt.Errorf("buffet: write row with column %q not in the schema node list", k)
		}
	}

	if err := w.encoder.Encode(row); err != nil {
		return fmt.Errorf("buffet: gob-encode row: %w", err)
	}
	w.count++

	for k, v := range row {
		if v != nil {
			w.seenCols[k] = true
		}
	}

	return nil
}

var (
	ErrAlreadyClosed = errors.New("buffet: writer already closed")
)

// Abort stops any further operations, closes the spill file, and removes it.
func (w *Writer) Abort() {
	if w.closed {
		return
	}
	w.closed = true
	if w.tmp != nil {
		_ = w.tmp.Close()
		_ = os.Remove(w.tmp.Name())
	}
}

// Close finalizes the spill, splits rows into Parquet files based on maxRowsPerFile and groupFunc,
// writes out those Parquet files, cleans up the temp spill, and returns a slice of Results.
// If no rows were written (count == 0), it removes the spill and returns an empty slice.
func (w *Writer) Close() ([]Result, error) {
	if w.closed {
		return nil, ErrAlreadyClosed
	}
	w.closed = true

	// --- tear down the spill file ---
	if err := w.tmp.Close(); err != nil {
		return nil, fmt.Errorf("buffet: close spill: %w", err)
	}
	if w.count == 0 {
		os.Remove(w.tmp.Name())
		return nil, nil
	}

	in, err := os.Open(w.tmp.Name())
	if err != nil {
		return nil, fmt.Errorf("buffet: reopen spill: %w", err)
	}
	defer func() {
		in.Close()
		os.Remove(w.tmp.Name())
	}()
	decoder := gob.NewDecoder(in)

	// --- compute global keepCols & schema once ---
	w.keepCols = make([]string, 0, len(w.seenCols))
	for c := range w.seenCols {
		w.keepCols = append(w.keepCols, c)
	}
	sort.Strings(w.keepCols)
	nodes := make(map[string]parquet.Node, len(w.keepCols))
	for _, c := range w.keepCols {
		nodes[c] = w.schemaNodes[c]
	}
	schema := parquet.NewSchema(w.baseName, parquet.Group(nodes))
	wc, err := parquet.NewWriterConfig(WriterOptions(w.tmpdir, schema)...)
	if err != nil {
		return nil, fmt.Errorf("buffet: writer config: %w", err)
	}

	var outFile *os.File
	var pw *parquet.GenericWriter[map[string]any]
	var rowsInFile int64

	openNext := func() error {
		f, err := os.CreateTemp(w.tmpdir, "buffet-*.parquet")
		if err != nil {
			return err
		}
		outFile = f
		pw = parquet.NewGenericWriter[map[string]any](f, wc)
		if w.statsProv != nil {
			w.currentAcc = w.statsProv.NewAccumulator()
		}
		rowsInFile = 0
		return nil
	}

	closeCurrent := func() {
		if pw != nil {
			pw.Close()
		}
		if outFile != nil {
			outFile.Close()
		}
		if rowsInFile == 0 && outFile != nil {
			os.Remove(outFile.Name())
		}
	}

	var results []Result
	var prevRow map[string]any

	var maxRowsPerFile int64
	if w.targetSize > 0 && w.estimatedBytesPerRecord > 0 {
		maxRowsPerFile = max(w.targetSize/w.estimatedBytesPerRecord, 1)
	}

	for {
		var row map[string]any
		if err := decoder.Decode(&row); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			closeCurrent()
			return nil, fmt.Errorf("buffet: gob-decode: %w", err)
		}

		if pw == nil {
			if err := openNext(); err != nil {
				return nil, fmt.Errorf("buffet: open first parquet file: %w", err)
			}
		}

		if maxRowsPerFile > 0 && rowsInFile >= maxRowsPerFile &&
			!(w.groupFunc != nil && prevRow != nil && w.groupFunc(prevRow, row)) {
			closeCurrent()
			results = append(results, Result{
				FileName:    outFile.Name(),
				RecordCount: rowsInFile,
			})
			if err := openNext(); err != nil {
				return nil, fmt.Errorf("buffet: open next parquet file: %w", err)
			}
		}

		filtered := make(map[string]any, len(w.keepCols))
		for _, c := range w.keepCols {
			filtered[c] = row[c]
		}

		if _, err := pw.Write([]map[string]any{filtered}); err != nil {
			pw.Close()
			outFile.Close()
			os.Remove(outFile.Name())
			return nil, fmt.Errorf("buffet: write row: %w", err)
		}
		if w.currentAcc != nil {
			w.currentAcc.Add(filtered)
		}
		rowsInFile++
		prevRow = row
	}

	if pw != nil {
		closeCurrent()
		if rowsInFile > 0 {
			var stats any
			if w.currentAcc != nil {
				stats = w.currentAcc.Finalize()
			}
			results = append(results, Result{
				FileName:    outFile.Name(),
				RecordCount: rowsInFile,
				Stats:       stats,
			})
		}
	}

	for i := range results {
		if info, err := os.Stat(results[i].FileName); err == nil {
			results[i].FileSize = info.Size()
		} else {
			results[i].FileSize = FileSizeUnavailable
		}
	}

	return results, nil
}
