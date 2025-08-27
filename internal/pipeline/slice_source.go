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

import "io"

// SliceSource is a tiny source for examples/tests.
// Swap this with your Parquet reader that yields up to batchPool.sz rows each Next().
type SliceSource struct {
	data   []Row
	pos    int
	owner  *batchPool
	closed bool
}

func NewSliceSource(data []Row, pool *batchPool) *SliceSource {
	return &SliceSource{data: data, owner: pool}
}

func (s *SliceSource) Next() (*Batch, error) {
	if s.closed {
		return nil, io.EOF
	}
	if s.pos >= len(s.data) {
		return nil, io.EOF
	}
	b := s.owner.Get()
	upper := s.pos + cap(b.Rows)
	if upper > len(s.data) {
		upper = len(s.data)
	}
	b.Rows = append(b.Rows, s.data[s.pos:upper]...)
	s.pos = upper
	return b, nil
}

func (s *SliceSource) Close() error { 
	s.closed = true
	return nil 
}