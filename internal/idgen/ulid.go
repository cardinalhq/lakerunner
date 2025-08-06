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

package idgen

import (
	crand "crypto/rand"
	"time"

	"github.com/oklog/ulid/v2"
)

type InlineULIDGenerator struct{}

var _ IDGenerator = &InlineULIDGenerator{}

func (i *InlineULIDGenerator) Make(_ time.Time) string {
	return ulid.Make().String()
}

type ULIDGenerator struct {
	entropy *ulid.MonotonicEntropy
}

var _ IDGenerator = &ULIDGenerator{}

func NewULIDGenerator() *ULIDGenerator {
	return &ULIDGenerator{
		entropy: ulid.Monotonic(crand.Reader, 0),
	}
}

func (u *ULIDGenerator) Make(t time.Time) string {
	return ulid.MustNew(ulid.Timestamp(t), u.entropy).String()
}
