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
	"errors"
	"math/rand/v2"
	"time"

	"github.com/sony/sonyflake"
)

var DefaultFlakeGenerator *SonyFlakeGenerator

func init() {
	var err error
	DefaultFlakeGenerator, err = NewFlakeGenerator()
	if err != nil {
		panic(err)
	}
}

type SonyFlakeGenerator struct {
	sf *sonyflake.Sonyflake
}

func NewFlakeGenerator() (*SonyFlakeGenerator, error) {
	settings := sonyflake.Settings{
		StartTime: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	sf, err := sonyflake.New(settings)
	if err != nil {
		return nil, err
	}
	if sf == nil {
		return nil, errors.New("failed to create Sonyflake instance")
	}
	return &SonyFlakeGenerator{sf: sf}, nil
}

// NextID returns a positive int64 that'll increase roughly in time order.
func (sf *SonyFlakeGenerator) NextID() int64 {
	v, err := sf.sf.NextID()
	if err != nil {
		return rand.Int64()
	}
	return int64(v)
}
