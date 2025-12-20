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

package filereader

import (
	"errors"
	"io"
)

// errorReaderImpl is a test helper that simulates read errors
type errorReaderImpl struct {
	shouldError bool
}

func (e *errorReaderImpl) Read(p []byte) (int, error) {
	if e.shouldError {
		return 0, errors.New("simulated read error")
	}
	return 0, io.EOF
}
