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

package filereader

import (
	"testing"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func BenchmarkBatchOperations(b *testing.B) {
	b.Run("TakeRow", func(b *testing.B) {
		batch := pipeline.GetBatch()
		defer pipeline.ReturnBatch(batch)

		// Add some rows
		for i := 0; i < 100; i++ {
			row := batch.AddRow()
			row[wkk.RowKeyCName] = "test"
			row[wkk.RowKeyCTID] = int64(i)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Take a row and return it
			row := batch.TakeRow(i % batch.Len())
			if row != nil {
				pipeline.ReturnPooledRow(row)
			}
		}
	})

	b.Run("ReplaceRow", func(b *testing.B) {
		batch := pipeline.GetBatch()
		defer pipeline.ReturnBatch(batch)

		// Add some rows
		for i := 0; i < 100; i++ {
			row := batch.AddRow()
			row[wkk.RowKeyCName] = "test"
			row[wkk.RowKeyCTID] = int64(i)
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			// Get a new row and replace
			newRow := pipeline.GetPooledRow()
			newRow[wkk.RowKeyCName] = "replaced"
			batch.ReplaceRow(i%batch.Len(), newRow)
		}
	})
}
