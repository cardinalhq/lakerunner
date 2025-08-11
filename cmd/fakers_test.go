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
	"context"
	"fmt"
	"io"
	"maps"
	"os"

	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	"github.com/parquet-go/parquet-go"
)

type fakeFetcher struct {
	// key -> response
	resp map[string]struct {
		tmp      string
		sz       int64
		notFound bool
		err      error
	}
	calls []string
}

func (f *fakeFetcher) Download(ctx context.Context, bucket, key, tmpdir string) (string, int64, bool, error) {
	f.calls = append(f.calls, key)
	r, ok := f.resp[key]
	if !ok {
		return "", 0, false, fmt.Errorf("unexpected key: %s", key)
	}
	return r.tmp, r.sz, r.notFound, r.err
}

func buildNodes(fields map[string]any) map[string]parquet.Node {
	b := buffet.NewNodeMapBuilder()
	_ = b.Add(fields)
	return b.Build()
}

type fakeOpener struct {
	// tmpfile path -> error to return from LoadSchemaForFile
	fail map[string]error
	// what Nodes to pre-populate for any returned handle
	nodes map[string]parquet.Node
	// optional error to return from NewGenericReader
	readerErr error
}

func (o fakeOpener) LoadSchemaForFile(path string) (*filecrunch.FileHandle, error) {
	if err, ok := o.fail[path]; ok && err != nil {
		return nil, err
	}
	return &filecrunch.FileHandle{
		File:   &os.File{},        // dummy, not read from
		Schema: &parquet.Schema{}, // dummy
		Nodes:  cloneNodes(o.nodes),
	}, nil
}

func (o fakeOpener) NewGenericMapReader(f *os.File, schema *parquet.Schema) (GenericMapReader, error) {
	if o.readerErr != nil {
		return nil, o.readerErr
	}
	// Return a no-op reader that implements the interface.
	return fakeGenericReader{}, nil
}

type fakeGenericReader struct{}

func (fakeGenericReader) Read(dst []map[string]any) (int, error) {
	return 0, io.EOF
}

func (fakeGenericReader) Close() error { return nil }

func cloneNodes(in map[string]parquet.Node) map[string]parquet.Node {
	if in == nil {
		return map[string]parquet.Node{}
	}
	out := make(map[string]parquet.Node, len(in))
	maps.Copy(out, in)
	return out
}

// --- fakes for copyAll ---

// fakeBatchReader is a GenericMapReader that yields a fixed stream of records.
type fakeBatchReader struct {
	batches [][]map[string]any // batches to return on successive Read calls
	i       int                // next batch index
	err     error              // final error after last batch (e.g., to simulate failure)
	closed  bool
}

func (r *fakeBatchReader) Read(dst []map[string]any) (int, error) {
	if r.i >= len(rb(r)) {
		if r.err != nil {
			// return error exactly once, then behave like EOF
			err := r.err
			r.err = nil
			return 0, err
		}
		return 0, io.EOF
	}
	b := rb(r)[r.i]
	r.i++
	n := copy(dst, b)
	return n, nil
}

func (r *fakeBatchReader) Close() error { r.closed = true; return nil }

func rb(r *fakeBatchReader) [][]map[string]any { return r.batches }

// fakeFileOpenerWithReaders returns the provided readers in order for each call.
type fakeFileOpenerWithReaders struct {
	readers []GenericMapReader
	nodes   map[string]parquet.Node
}

func (o *fakeFileOpenerWithReaders) LoadSchemaForFile(path string) (*filecrunch.FileHandle, error) {
	return &filecrunch.FileHandle{
		File:   &os.File{},        // unused by fakes
		Schema: &parquet.Schema{}, // unused by fakes
		Nodes:  cloneNodes(o.nodes),
	}, nil
}

func (o *fakeFileOpenerWithReaders) NewGenericMapReader(f *os.File, schema *parquet.Schema) (GenericMapReader, error) {
	if len(o.readers) == 0 {
		return nil, fmt.Errorf("no reader available")
	}
	r := o.readers[0]
	o.readers = o.readers[1:]
	return r, nil
}

// fakeWriter records writes for assertions.
type fakeWriter struct {
	recs []map[string]any
}

func (w *fakeWriter) Write(m map[string]any) error {
	// shallow copy to avoid aliasing surprises in assertions
	cp := make(map[string]any, len(m))
	for k, v := range m {
		cp[k] = v
	}
	w.recs = append(w.recs, cp)
	return nil
}

func (w *fakeWriter) Close() ([]buffet.Result, error) { return nil, nil }
