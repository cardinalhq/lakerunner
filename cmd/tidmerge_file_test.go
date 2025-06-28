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
	"errors"
	"io"
	"os"
	"testing"

	"github.com/DataDog/sketches-go/ddsketch"
	"github.com/cardinalhq/lakerunner/internal/buffet"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func sketchOf(t *testing.T, values ...float64) []byte {
	s, err := ddsketch.NewDefaultDDSketch(0.01)
	require.NoError(t, err)

	for _, v := range values {
		err = s.Add(v)
		require.NoError(t, err)
	}

	b := EncodeSketch(s)
	return b
}

func writeParquet(filename string, schema *parquet.Schema, rows []map[string]any) error {
	wc, err := parquet.NewWriterConfig(buffet.WriterOptions("", schema)...)
	if err != nil {
		return err
	}
	of, err := os.Create(filename)
	if err != nil {
		return err
	}

	pw := parquet.NewGenericWriter[map[string]any](of, wc)

	n, err := pw.Write(rows)
	if err != nil {
		return err
	}
	if n != len(rows) {
		return errors.New("Write did not write all rows")
	}

	err = pw.Close()
	if err != nil {
		return err
	}

	return of.Close()
}

func readParquet(schema *parquet.Schema, filename string) ([]map[string]any, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	pr := parquet.NewGenericReader[map[string]any](f, schema)

	ret := []map[string]any{}
	for {
		buffer := make([]map[string]any, 1000)
		for i := range len(buffer) {
			buffer[i] = map[string]any{}
		}
		n, err := pr.Read(buffer)
		buffer = buffer[:n]
		if n > 0 {
			ret = append(ret, buffer...)
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
	}
	return ret, nil
}

func TestTIDMergeWithRows(t *testing.T) {
	tmpdir, err := os.MkdirTemp("", "tidmerge_test")
	require.NoError(t, err)
	defer os.RemoveAll(tmpdir)

	rows1 := []map[string]any{
		{
			"_cardinalhq.tid":       int64(1),
			"_cardinalhq.name":      "metric1",
			"sketch":                sketchOf(t, 1),
			"_cardinalhq.timestamp": int64(1749505550),
		},
		{
			"_cardinalhq.tid":       int64(1),
			"_cardinalhq.name":      "metric1",
			"sketch":                sketchOf(t, 1, 2),
			"_cardinalhq.timestamp": int64(1749505550),
		},
	}

	rows2 := []map[string]any{
		{
			"_cardinalhq.tid":       int64(1),
			"_cardinalhq.name":      "metric1",
			"sketch":                sketchOf(t, 1, 2),
			"_cardinalhq.timestamp": int64(1749505650),
		},
		{
			"_cardinalhq.tid":       int64(2),
			"_cardinalhq.name":      "metric2",
			"sketch":                sketchOf(t, 2),
			"_cardinalhq.timestamp": int64(1749505551),
		},
	}

	rows3 := []map[string]any{
		{
			"_cardinalhq.tid":       int64(1),
			"_cardinalhq.name":      "metric1",
			"sketch":                sketchOf(t, 1, 2),
			"_cardinalhq.timestamp": int64(1749505650),
		},
		{
			"_cardinalhq.tid":       int64(3),
			"_cardinalhq.name":      "metric3",
			"sketch":                sketchOf(t, 3),
			"_cardinalhq.timestamp": int64(1749517552),
		},
		{
			"_cardinalhq.tid":       int64(3),
			"_cardinalhq.name":      "metric3",
			"sketch":                sketchOf(t, 3),
			"_cardinalhq.timestamp": int64(1749515652),
		},
	}

	nmb := buffet.NewNodeMapBuilder()
	require.NoError(t, nmb.Add(rows1[0]))
	require.NoError(t, nmb.Add(rows2[0]))
	require.NoError(t, nmb.Add(rows3[0]))
	nodes := nmb.Build()
	schema, err := buffet.ParquetSchemaFromNodemap("merger", nodes)
	require.NoError(t, err)

	err = writeParquet(tmpdir+"/test1.parquet", schema, rows1)
	require.NoError(t, err)
	err = writeParquet(tmpdir+"/test2.parquet", schema, rows2)
	require.NoError(t, err)
	err = writeParquet(tmpdir+"/test3.parquet", schema, rows3)
	require.NoError(t, err)

	tm, err := NewTIDMerger(tmpdir,
		[]string{
			tmpdir + "/test1.parquet",
			tmpdir + "/test2.parquet",
			tmpdir + "/test3.parquet",
		},
		10000,
		1000000,
		100,
		1749505552/10000*10000,
		1749505552/10000*10000+20000,
	)
	require.NoError(t, err)

	results, stats, err := tm.Merge()
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, int64(3), results[0].RecordCount)
	assert.Equal(t, int32(3), results[0].TidCount)
	assert.Equal(t, int64(0), stats.DatapointsOutOfRange)

	rows, err := readParquet(schema, results[0].FileName)
	require.NoError(t, err)

	assert.Len(t, rows, 3)

	for _, row := range rows {
		_, ok := row["sketch"].([]byte)
		if !ok {
			_, ok = row["sketch"].(string)
		}
		require.True(t, ok, "Expected sketch to be []byte or string, got %T", row["sketch"])
		delete(row, "sketch")
		t.Log(row)
	}

	assert.Equal(t, map[string]any{
		"_cardinalhq.tid":       int64(1),
		"_cardinalhq.name":      "metric1",
		"_cardinalhq.timestamp": int64(1749500000),
	}, rows[0])
	assert.Equal(t, map[string]any{
		"_cardinalhq.tid":       int64(2),
		"_cardinalhq.name":      "metric2",
		"_cardinalhq.timestamp": int64(1749500000),
	}, rows[1])
	assert.Equal(t, map[string]any{
		"_cardinalhq.tid":       int64(3),
		"_cardinalhq.name":      "metric3",
		"_cardinalhq.timestamp": int64(1749510000),
	}, rows[2])
}
