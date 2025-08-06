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
