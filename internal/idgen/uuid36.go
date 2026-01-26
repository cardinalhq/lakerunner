// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package idgen

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/google/uuid"
)

func UUIDToBase36(id uuid.UUID) string {
	bi := new(big.Int).SetBytes(id[:])

	ret := bi.Text(36)
	const fixedLength = 25
	if len(ret) < fixedLength {
		ret = strings.Repeat("0", fixedLength-len(ret)) + ret
	}
	return ret
}

// base36ToUUID converts a base36 string (assumed to be 25 characters long)
// back into a uuid.UUID.
func base36ToUUID(s string) (uuid.UUID, error) {
	bi, ok := new(big.Int).SetString(s, 36)
	if !ok {
		return uuid.Nil, fmt.Errorf("invalid base36 string: %s", s)
	}
	if bi.BitLen() > 128 {
		return uuid.Nil, fmt.Errorf("number too large for UUID: %s", s)
	}
	b := bi.Bytes()

	if len(b) < 16 {
		padded := make([]byte, 16)
		copy(padded[16-len(b):], b)
		b = padded
	}
	var id uuid.UUID
	copy(id[:], b)
	return id, nil
}
