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

import "context"

// FlatMapReader: 1→N mapping (including 0 or many)
type FlatMapReader struct {
	in    Reader
	fn    func(Row, func(Row))
	stash *Batch // carry-over when expansion exceeds batch cap
}

func FlatMap(in Reader, fn func(Row, func(Row))) *FlatMapReader {
	return &FlatMapReader{in: in, fn: fn}
}

func (f *FlatMapReader) Next(ctx context.Context) (*Batch, error) {
	// drain stash first
	if f.stash != nil && f.stash.Len() > 0 {
		out := f.stash
		f.stash = nil
		return out, nil
	}
	for {
		in, err := f.in.Next(ctx)
		if err != nil {
			return nil, err
		}
		out := globalBatchPool.Get()
		emit := func(r Row) {
			targetRow := out.AddRow()
			for k, v := range r {
				targetRow[k] = v
			}
			if out.Len() >= 1000 {
				// Stash the full batch and continue accumulating into a fresh one
				if f.stash == nil {
					f.stash = out
				} else {
					// should not happen, but just in case
					panic("unexpected multiple stash")
				}
				out = globalBatchPool.Get()
			}
		}
		for i := 0; i < in.Len(); i++ {
			r := in.Get(i)
			f.fn(r, emit)
		}
		if f.stash != nil {
			// we filled exactly one batch; return it now, keep 'out' for next call if it has rows
			if out.Len() > 0 {
				// we have leftover; keep as next stash
				if f.stash == nil {
					f.stash = out
				} else {
					// unlikely path
					panic("unexpected stash collision")
				}
			} else {
				// recycle empty
				globalBatchPool.Put(out)
			}
			out := f.stash
			f.stash = nil
			return out, nil
		}
		if out.Len() > 0 {
			return out, nil
		}
		// otherwise loop to pull next input batch
	}
}

func (f *FlatMapReader) Close() error { return f.in.Close() }
