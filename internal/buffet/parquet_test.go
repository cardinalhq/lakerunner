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

package buffet

import (
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriterWithAllNodeTypes(t *testing.T) {
	mapschema := map[string]any{
		"int8":   int8(1),
		"[]int8": []int8{1, 2, 3},

		"int16":   int16(3),
		"[]int16": []int16{1, 2, 3},

		"int32":   int32(4),
		"[]int32": []int32{1, 2, 3},

		"int64":   int64(5),
		"[]int64": []int64{1, 2, 3},

		"float32":   float32(1.2),
		"[]float32": []float32{1.1, 2.2, 3.3},

		"float64":   float64(6.7),
		"[]float64": []float64{1.1, 2.2, 3.3},

		"string":   "hello",
		"[]string": []string{"one", "two", "three"},

		"byte":   byte(2),
		"[]byte": []byte{1, 2, 3, 4, 5},

		"bool":   true,
		"[]bool": []bool{true, false, true},
	}

	nodemapBuilder := NewNodeMapBuilder()
	require.NoError(t, nodemapBuilder.Add(mapschema))
	nodes := nodemapBuilder.Build()

	schema, err := ParquetSchemaFromNodemap("test_schema", nodes)
	require.NoError(t, err)

	wc, err := parquet.NewWriterConfig(WriterOptions("/tmp", schema)...)
	require.NoError(t, err)

	out, err := os.CreateTemp("/tmp", "test-*.parquet")
	require.NoError(t, err)
	defer func() {
		_ = os.Remove(out.Name())
		_ = out.Close()
	}()

	pw := parquet.NewGenericWriter[map[string]any](out, wc)
	defer func() {
		assert.NoError(t, pw.Close())
	}()

	for range 1000 {
		n, err := pw.Write([]map[string]any{mapschema})
		require.NoError(t, err)
		assert.Equal(t, 1, n)
	}
}

// func xTestReadWrite(t *testing.T) {
// 	fh, err := filecrunch.LoadSchemaForFile("/Users/mgraff/Downloads/tbl_294590228450738.parquet")
// 	require.NoError(t, err)

// 	out, err := os.CreateTemp("/tmp", "ctest-*.parquet")
// 	require.NoError(t, err)
// 	pw := parquet.NewGenericWriter[map[string]any](out, writerOptions(fh.Schema)...)

// 	pr := parquet.NewGenericReader[map[string]any](fh.File, fh.Schema)
// 	for {
// 		rows := make([]map[string]any, 1000)
// 		for i := range 1000 {
// 			rows[i] = make(map[string]any)
// 		}
// 		n, err := pr.Read(rows)
// 		rows = rows[:n]
// 		_, _ = pw.Write(rows)
// 		if errors.Is(err, io.EOF) {
// 			break
// 		}
// 		require.NoError(t, err)
// 	}
// 	pw.Close()
// 	out.Close()
// }
