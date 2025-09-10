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
	"errors"
	"fmt"
	"io"

	"github.com/parquet-go/parquet-go"

	"github.com/cardinalhq/lakerunner/internal/filecrunch"
)

// dropFieldNames defines fields to be removed during log processing
var dropFieldNames = []string{
	"minute",
	"hour",
	"day",
	"month",
	"year",
}

// mergeNodes merges schemas from opened handles.
func mergeNodes(handles []*filecrunch.FileHandle) (map[string]parquet.Node, error) {
	cap := 0
	for _, h := range handles {
		if h != nil {
			cap += len(h.Nodes)
		}
	}
	nodes := make(map[string]parquet.Node, cap)
	for _, h := range handles {
		if h == nil || h.Nodes == nil {
			continue
		}
		if err := filecrunch.MergeNodes(h, nodes); err != nil {
			return nil, err
		}
	}
	return nodes, nil
}

func computeDropSet(nodes map[string]parquet.Node) map[string]struct{} {
	drop := map[string]struct{}{}
	for _, name := range dropFieldNames {
		if _, ok := nodes[name]; ok {
			drop[name] = struct{}{}
		}
	}
	return drop
}

// normalizeRecord drops fields and coerces _cardinalhq.timestamp to int64.
func normalizeRecord(rec map[string]any, drop map[string]struct{}) (map[string]any, error) {
	if len(drop) != 0 {
		for k := range drop {
			delete(rec, k)
		}
	}
	v, ok := rec["_cardinalhq.timestamp"]
	if !ok {
		return nil, errors.New("missing _cardinalhq.timestamp")
	}
	switch t := v.(type) {
	case int64:
		// ok
	case int32:
		rec["_cardinalhq.timestamp"] = int64(t)
	case float64:
		rec["_cardinalhq.timestamp"] = int64(t)
	default:
		return nil, fmt.Errorf("unexpected _cardinalhq.timestamp type %T", v)
	}
	return rec, nil
}

// copyAll writes all rows from each handle to writer; returns total written rows.
func copyAll(
	ctx context.Context,
	open FileOpener,
	writer Writer,
	handles []*filecrunch.FileHandle,
) (int64, error) {
	var total int64
	const batchSize = 4096
	batch := make([]map[string]any, batchSize)
	for i := range batch {
		batch[i] = mapPool.Get().(map[string]any)
	}
	defer func() {
		for i := range batch {
			mapPool.Put(batch[i])
		}
	}()

	for _, h := range handles {
		if err := ctx.Err(); err != nil {
			return total, err
		}

		dropSet := computeDropSet(h.Nodes)

		r, err := open.NewGenericMapReader(h.File, h.Schema)
		if err != nil {
			return total, err
		}

		for {
			if err := ctx.Err(); err != nil {
				_ = r.Close()
				return total, err
			}
			n, err := r.Read(batch)
			if n > 0 {
				// Normalize and write each record in the batch.
				for i := range n {
					rec, nerr := normalizeRecord(batch[i], dropSet)
					if nerr != nil {
						_ = r.Close()
						return total, nerr
					}
					if werr := writer.Write(rec); werr != nil {
						_ = r.Close()
						return total, werr
					}
					for k := range batch[i] {
						delete(batch[i], k)
					}
				}
				total += int64(n)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				_ = r.Close()
				return total, err
			}
		}
		// non-fatal close
		_ = r.Close()
	}
	return total, nil
}
