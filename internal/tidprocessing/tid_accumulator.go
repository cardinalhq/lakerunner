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
	mapset "github.com/deckarep/golang-set/v2"

	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/cardinalhq/lakerunner/internal/helpers"
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
	if tid, ok := helpers.GetTIDValue(row, "_cardinalhq.tid"); ok {
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
