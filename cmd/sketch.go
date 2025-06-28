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

package cmd

import (
	"fmt"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/DataDog/sketches-go/ddsketch/mapping"
	"github.com/DataDog/sketches-go/ddsketch/store"
)

func EncodeSketch(sketch *ddsketch.DDSketch) []byte {
	var buf []byte
	sketch.Encode(&buf, false)
	return buf
}

func DecodeSketch(data []byte) (*ddsketch.DDSketch, error) {
	m, err := mapping.NewLogarithmicMapping(0.01)
	if err != nil {
		return nil, err
	}
	sk, err := ddsketch.DecodeDDSketch(data, store.DefaultProvider, m)
	if err != nil {
		return nil, err
	}
	return sk, nil
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
