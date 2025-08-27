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

// MapReader: 1→1 mapping (optionally drop by returning (nil,false))
type MapReader struct {
	in   Reader
	pool *batchPool
	fn   func(Row) (Row, bool) // return ok=false to drop
}

func Map(in Reader, pool *batchPool, fn func(Row) (Row, bool)) *MapReader {
	return &MapReader{in: in, pool: pool, fn: fn}
}

func (m *MapReader) Next() (*Batch, error) {
	for {
		in, err := m.in.Next()
		if err != nil {
			return nil, err
		}
		out := m.pool.Get()
		for _, r := range in.Rows {
			if nr, ok := m.fn(r); ok {
				// IMPORTANT: assume fn returns a fresh Row or same row but treated ephemeral.
				// If fn wants to retain, it must copy.
				out.Rows = append(out.Rows, nr)
				if len(out.Rows) == cap(out.Rows) {
					return out, nil // return early if full; remaining rows will be processed on next call
				}
			}
		}
		if len(out.Rows) > 0 {
			return out, nil
		}
		// otherwise loop to pull next input batch
	}
}

func (m *MapReader) Close() error { return m.in.Close() }

// FilterReader: keep rows where pred==true (thin wrapper around Map)
func Filter(in Reader, pool *batchPool, pred func(Row) bool) *MapReader {
	return Map(in, pool, func(r Row) (Row, bool) { return r, pred(r) })
}
