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
