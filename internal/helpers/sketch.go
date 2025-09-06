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

package helpers

import (
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

// EncodeSketch encodes a DDSketch to bytes.
func EncodeSketch(sketch *ddsketch.DDSketch) []byte {
	var buf []byte
	sketch.Encode(&buf, false)
	return buf
}

// DecodeSketch decodes a DDSketch from bytes using shared mapping and dense store.
func DecodeSketch(data []byte) (*ddsketch.DDSketch, error) {
	m, err := getSharedMapping()
	if err != nil {
		return nil, err
	}
	return ddsketch.DecodeDDSketch(data, store.DenseStoreConstructor, m)
}
