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
	in Reader
	fn func(Row) (Row, bool) // return ok=false to drop
}

func Map(in Reader, fn func(Row) (Row, bool)) *MapReader {
	return &MapReader{in: in, fn: fn}
}

func (m *MapReader) Next() (*Batch, error) {
	for {
		in, err := m.in.Next()
		if err != nil {
			return nil, err
		}
		out := globalBatchPool.Get()
		for i := 0; i < in.Len(); i++ {
			sourceRow := in.Get(i)
			if mappedRow, ok := m.fn(sourceRow); ok {
				// IMPORTANT: assume fn returns a fresh Row or same row but treated ephemeral.
				// If fn wants to retain, it must copy.
				targetRow := out.AddRow()
				for k, v := range mappedRow {
					targetRow[k] = v
				}
				// Check if batch is full (reaching pre-allocated capacity)
				if out.Len() >= 1000 { // Use default batch size
					return out, nil
				}
			}
		}
		if out.Len() > 0 {
			return out, nil
		}
		// otherwise loop to pull next input batch
	}
}

func (m *MapReader) Close() error { return m.in.Close() }

// FilterReader: keep rows where pred==true (thin wrapper around Map)
func Filter(in Reader, pred func(Row) bool) *MapReader {
	return Map(in, func(r Row) (Row, bool) { return r, pred(r) })
}
