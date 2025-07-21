// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
