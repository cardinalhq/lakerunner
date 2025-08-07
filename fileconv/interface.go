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

package fileconv

type Reader interface {
	// Returns the next row of data.  It will return (nil, true, nil) when there are no more rows.
	// row and done are only valid if err is nil.
	GetRow() (row map[string]any, done bool, err error)
	Close() error
}

type Writer interface {
	// WriteRow writes a row of data.  It returns an error if the write fails.
	WriteRow(row map[string]any) error
	Close() error
}
