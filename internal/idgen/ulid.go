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
