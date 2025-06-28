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

package dbase

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

// Base36ToUUID converts a base36 string (assumed to be 25 characters long)
// back into a uuid.UUID.
func Base36ToUUID(s string) (uuid.UUID, error) {
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
