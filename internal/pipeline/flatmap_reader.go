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

// FlatMapReader: 1→N mapping (including 0 or many)
type FlatMapReader struct {
	in    Reader
	pool  *batchPool
	fn    func(Row, func(Row))
	stash *Batch // carry-over when expansion exceeds batch cap
}

func FlatMap(in Reader, pool *batchPool, fn func(Row, func(Row))) *FlatMapReader {
	return &FlatMapReader{in: in, pool: pool, fn: fn}
}

func (f *FlatMapReader) Next() (*Batch, error) {
	// drain stash first
	if f.stash != nil && len(f.stash.Rows) > 0 {
		out := f.stash
		f.stash = nil
		return out, nil
	}
	for {
		in, err := f.in.Next()
		if err != nil {
			return nil, err
		}
		out := f.pool.Get()
		emit := func(r Row) {
			out.Rows = append(out.Rows, r)
			if len(out.Rows) == cap(out.Rows) {
				// Stash the full batch and continue accumulating into a fresh one
				if f.stash == nil {
					f.stash = out
				} else {
					// should not happen, but just in case
					panic("unexpected multiple stash")
				}
				out = f.pool.Get()
			}
		}
		for _, r := range in.Rows {
			f.fn(r, emit)
		}
		if f.stash != nil {
			// we filled exactly one batch; return it now, keep 'out' for next call if it has rows
			if len(out.Rows) > 0 {
				// we have leftover; keep as next stash
				if f.stash == nil {
					f.stash = out
				} else {
					// unlikely path
					panic("unexpected stash collision")
				}
			} else {
				// recycle empty
				f.pool.Put(out)
			}
			out := f.stash
			f.stash = nil
			return out, nil
		}
		if len(out.Rows) > 0 {
			return out, nil
		}
		// otherwise loop to pull next input batch
	}
}

func (f *FlatMapReader) Close() error { return f.in.Close() }