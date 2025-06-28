// This file is part of CardinalHQ, Inc.
//
// CardinalHQ, Inc. proprietary and confidential.
// Unauthorized copying, distribution, or modification of this file,
// via any medium, is strictly prohibited without prior written consent.
//
// Copyright 2024-2025 CardinalHQ, Inc. All rights reserved.

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
