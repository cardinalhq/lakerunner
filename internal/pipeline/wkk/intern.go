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

package wkk

import "unique"

type rowkey string

type RowKey = unique.Handle[rowkey]

func NewRowKey(s string) RowKey {
	return unique.Make(rowkey(s))
}

var (
	RowKeyCTID       = NewRowKey("_cardinalhq.tid")
	RowKeyCName      = NewRowKey("_cardinalhq.name")
	RowKeyCTimestamp = NewRowKey("_cardinalhq.timestamp")
)
