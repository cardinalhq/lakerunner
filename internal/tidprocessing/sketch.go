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

package tidprocessing

import (
	"fmt"
	"sync"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/mapping"
	"github.com/DataDog/sketches-go/ddsketch/store"
)

var (
	defaultRelAcc  = 0.01
	mappingOnce    sync.Once
	sharedMapping  mapping.IndexMapping
	mappingInitErr error
)

func getSharedMapping() (mapping.IndexMapping, error) {
	mappingOnce.Do(func() {
		sharedMapping, mappingInitErr = mapping.NewLogarithmicMapping(defaultRelAcc)
	})
	return sharedMapping, mappingInitErr
}

func EncodeSketch(sketch *ddsketch.DDSketch) []byte {
	var buf []byte
	sketch.Encode(&buf, false)
	return buf
}

func DecodeSketch(data []byte) (*ddsketch.DDSketch, error) {
	m, err := getSharedMapping()
	if err != nil {
		return nil, err
	}
	return ddsketch.DecodeDDSketch(data, store.DefaultProvider, m)
}

func Merge(sketch *ddsketch.DDSketch, other *ddsketch.DDSketch) error {
	return sketch.MergeWith(other)
}

func MergeEncodedSketch(a, b []byte) ([]byte, error) {
	skA, err := DecodeSketch(a)
	if err != nil {
		return nil, fmt.Errorf("decoding sketch A: %w", err)
	}
	skB, err := DecodeSketch(b)
	if err != nil {
		return nil, fmt.Errorf("decoding sketch B: %w", err)
	}
	if err := Merge(skA, skB); err != nil {
		return nil, fmt.Errorf("merging sketches: %w", err)
	}
	return EncodeSketch(skA), nil
}
