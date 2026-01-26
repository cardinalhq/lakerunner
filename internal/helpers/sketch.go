// Copyright (C) 2025-2026 CardinalHQ, Inc
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

// sketchPool provides reusable DDSketch instances.
// Sketches are cleared before being returned to the pool.
var sketchPool = sync.Pool{
	New: func() any {
		m, err := getSharedMapping()
		if err != nil {
			return nil
		}
		return ddsketch.NewDDSketchFromStoreProvider(m, store.DenseStoreConstructor)
	},
}

func getSharedMapping() (mapping.IndexMapping, error) {
	mappingOnce.Do(func() {
		sharedMapping, mappingInitErr = mapping.NewLogarithmicMapping(defaultRelAcc)
	})
	return sharedMapping, mappingInitErr
}

// GetSketch returns a DDSketch from the pool, ready for use.
// The sketch is cleared and ready for new values.
func GetSketch() (*ddsketch.DDSketch, error) {
	s := sketchPool.Get()
	if s == nil {
		// Pool.New failed, create directly
		m, err := getSharedMapping()
		if err != nil {
			return nil, err
		}
		return ddsketch.NewDDSketchFromStoreProvider(m, store.DenseStoreConstructor), nil
	}
	sketch := s.(*ddsketch.DDSketch)
	sketch.Clear()
	return sketch, nil
}

// PutSketch returns a DDSketch to the pool for reuse.
// The sketch is NOT cleared here - Clear() is called on Get to avoid
// clearing sketches that may never be reused.
func PutSketch(s *ddsketch.DDSketch) {
	if s != nil {
		sketchPool.Put(s)
	}
}

// EncodeSketch encodes a DDSketch to bytes.
func EncodeSketch(sketch *ddsketch.DDSketch) []byte {
	var buf []byte
	sketch.Encode(&buf, false)
	return buf
}

// EncodeAndReturnSketch encodes a DDSketch to bytes and returns it to the pool.
// This is a convenience function for the common pattern of encoding then returning.
func EncodeAndReturnSketch(sketch *ddsketch.DDSketch) []byte {
	var buf []byte
	sketch.Encode(&buf, false)
	PutSketch(sketch)
	return buf
}
