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

package pipeline

import (
	"context"
	"io"
	"slices"
)

// InMemorySortReader buffers all rows in memory, sorts them, then returns in order
type InMemorySortReader struct {
	reader  Reader
	pool    *batchPool
	less    func(Row, Row) bool
	allRows []Row
	pos     int
	loaded  bool
}

// SortInMemory creates a reader that sorts all input rows in memory
func SortInMemory(reader Reader, less func(Row, Row) bool) Reader {
	return &InMemorySortReader{
		reader: reader,
		pool:   globalBatchPool,
		less:   less,
	}
}

func (s *InMemorySortReader) Next(ctx context.Context) (*Batch, error) {
	if !s.loaded {
		if err := s.loadAllRows(ctx); err != nil {
			return nil, err
		}
		slices.SortFunc(s.allRows, func(a, b Row) int {
			if s.less(a, b) {
				return -1
			}
			if s.less(b, a) {
				return 1
			}
			return 0
		})
		s.loaded = true
	}

	if s.pos >= len(s.allRows) {
		return nil, io.EOF
	}

	batch := s.pool.Get()
	batchCapacity := 1000 // default batch size
	end := s.pos + batchCapacity
	if end > len(s.allRows) {
		end = len(s.allRows)
	}

	for i := s.pos; i < end; i++ {
		row := batch.AddRow()
		for k, v := range s.allRows[i] {
			row[k] = v
		}
	}
	s.pos = end

	return batch, nil
}

func (s *InMemorySortReader) loadAllRows(ctx context.Context) error {
	for {
		batch, err := s.reader.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		for i := 0; i < batch.Len(); i++ {
			row := batch.Get(i)
			s.allRows = append(s.allRows, copyRow(row))
		}
	}
	return nil
}

func (s *InMemorySortReader) Close() error {
	return s.reader.Close()
}
