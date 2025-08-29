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

// Package pipeline provides a streaming data processing pipeline with memory-safe row ownership.
// This package addresses memory ownership issues by establishing clear ownership semantics:
// batches are owned by the Reader that returns them, and consumers must copy data they wish to retain.
package pipeline

// Reader is a pull-based iterator over Batches.
// Next returns (nil, io.EOF) when the stream ends.
type Reader interface {
	Next() (*Batch, error)
	Close() error
}
