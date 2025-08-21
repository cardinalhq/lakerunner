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

package cmd

import (
	"crypto/rand"
	"encoding/base32"
	"strings"
)

// generateOperationID creates a unique operation ID for tracking atomic operations
// Format: randomBase32
func generateOperationID(index int) string {
	return generateShortID()
}

// generateShortID creates a short random base32 ID for operation tracking
func generateShortID() string {
	b := make([]byte, 5) // 5 bytes = 8 base32 chars
	_, _ = rand.Read(b)  // errors from rand.Read are rare and not critical for operation IDs
	return strings.TrimRight(base32.StdEncoding.EncodeToString(b), "=")
}
