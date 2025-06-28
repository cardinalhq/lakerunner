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
	"github.com/cardinalhq/lakerunner/internal/buffet"
	mapset "github.com/deckarep/golang-set/v2"
)

type TidAccumulatorProvider struct{}

func (p *TidAccumulatorProvider) NewAccumulator() buffet.StatsAccumulator {
	return NewTidAccumulator()
}

type TidAccumulator struct {
	set mapset.Set[int64]
}

func NewTidAccumulator() *TidAccumulator {
	return &TidAccumulator{
		set: mapset.NewSet[int64](),
	}
}

func (a *TidAccumulator) Add(row map[string]any) {
	if tid, ok := row["_cardinalhq.tid"].(int64); ok {
		a.set.Add(tid)
	}
}

type TidAccumulatorResult struct {
	Cardinality int // number of unique TIDs
}

func (a *TidAccumulator) Finalize() any {
	return TidAccumulatorResult{
		Cardinality: a.set.Cardinality(),
	}
}
