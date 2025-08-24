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
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"sort"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/filecrunch"
)

func TestAddfp(t *testing.T) {

	row := map[string]any{
		"_cardinalhq.timestamp": int64(1234567890),
		"resource.file":         "testfile.log",
	}

	tagValuesByFieldName := map[string]mapset.Set[string]{}
	for k, v := range row {
		if str, ok := v.(string); ok && str != "" {
			tagValuesByFieldName[k] = mapset.NewSet(str)
		}
	}

	fingerprintSet := ToFingerprints(tagValuesByFieldName)

	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("resource.file", "testfile.log")))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("resource.file", ExistsRegex)))
}

func TestAddfp_ComplexRow(t *testing.T) {
	row := map[string]any{
		"_cardinalhq.fingerprint":    "-8086437514702464098",
		"_cardinalhq.hostname":       "",
		"_cardinalhq.id":             "cu3f0k905lps739g3bpg",
		"_cardinalhq.level":          "",
		"_cardinalhq.message":        "REDACTED",
		"_cardinalhq.name":           "log.events",
		"_cardinalhq.telemetry_type": "logs",
		"_cardinalhq.timestamp":      int64(1736891912664),
		"_cardinalhq.tokenMap":       "REDACTED",
		"_cardinalhq.tokens":         "REDACTED",
		"_cardinalhq.value":          1,
		"env.collector_id":           "",
		"env.collector_name":         "",
		"env.customer_id":            "",
		"log.application":            nil,
		"log.controller_ip":          nil,
		"log.index_level_0":          "0",
		"log.log_level":              "warn",
		"log.method":                 nil,
		"log.module":                 "ssl",
		"log.pid":                    "1453",
		"log.protocol":               nil,
		"log.referer":                nil,
		"log.size":                   nil,
		"log.source":                 "127.0.0.1",
		"log.status":                 nil,
		"log.url":                    nil,
		"log.user_agent":             nil,
		"resource.bucket.name":       "bucket-name",
		"resource.file.name":         "REDACTED",
		"resource.file.type":         "errorlog",
		"resource.file":              "REDACTED",
	}
	tagValuesByFieldName := map[string]mapset.Set[string]{}
	for k, v := range row {
		if str, ok := v.(string); ok && str != "" {
			tagValuesByFieldName[k] = mapset.NewSet(str)
		}
	}

	fingerprintSet := ToFingerprints(tagValuesByFieldName)

	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("resource.file", "REDACTED")))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("_cardinalhq.id", ExistsRegex)))

	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("log.index_level_0", ExistsRegex)))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("log.log_level", ExistsRegex)))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("log.module", ExistsRegex)))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("log.pid", ExistsRegex)))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("log.source", ExistsRegex)))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("resource.bucket.name", ExistsRegex)))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("resource.file.name", ExistsRegex)))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("resource.file.type", ExistsRegex)))

	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("_cardinalhq.message", ExistsRegex)))

	computed := ComputeFingerprint("_cardinalhq.name", "logs")
	assert.True(t, fingerprintSet.Contains(computed))
	assert.True(t, fingerprintSet.Contains(ComputeFingerprint("_cardinalhq.name", ExistsRegex)))
}

func TestProcessAndSplitSortsByTimestamp(t *testing.T) {
	tmpDir := t.TempDir()
	nodes := map[string]parquet.Node{
		"_cardinalhq.timestamp": parquet.Int(64),
		"msg":                   parquet.String(),
	}
	schema := filecrunch.SchemaFromNodes(nodes)
	inputFile, err := os.CreateTemp(tmpDir, "unsorted-*.parquet")
	require.NoError(t, err)
	defer os.Remove(inputFile.Name())
	pw := parquet.NewWriter(inputFile, schema)
	base := int64(1_700_000_000_000)
	for i := 0; i < maxRowsSortBuffer+1000; i++ {
		ts := base - int64(i)
		row := map[string]any{
			"_cardinalhq.timestamp": ts,
			"msg":                   fmt.Sprintf("m%d", i),
		}
		require.NoError(t, pw.Write(row))
	}
	require.NoError(t, pw.Close())
	require.NoError(t, inputFile.Close())

	fh, err := filecrunch.LoadSchemaForFile(inputFile.Name())
	require.NoError(t, err)
	defer fh.Close()
	ll := slog.New(slog.NewTextHandler(io.Discard, nil))
	res, err := ProcessAndSplit(ll, fh, tmpDir, 0, 0)
	require.NoError(t, err)
	require.Len(t, res, 1)
	for _, hr := range res {
		f, err := os.Open(hr.FileName)
		require.NoError(t, err)
		pr := parquet.NewReader(f, schema)
		var prev int64 = math.MinInt64
		for j := int64(0); j < hr.RecordCount; j++ {
			row := map[string]any{}
			require.NoError(t, pr.Read(&row))
			ts := row["_cardinalhq.timestamp"].(int64)
			require.GreaterOrEqual(t, ts, prev)
			prev = ts
		}
		require.NoError(t, pr.Close())
		require.NoError(t, f.Close())
	}
}

func TestProcessAndSplit_HourBoundaries(t *testing.T) {
	tmpDir := t.TempDir()
	nodes := map[string]parquet.Node{
		"_cardinalhq.timestamp": parquet.Int(64),
		"msg":                   parquet.String(),
	}
	schema := filecrunch.SchemaFromNodes(nodes)

	tests := []struct {
		name           string
		records        []map[string]any
		expectedSplits int
		validateKeys   func(t *testing.T, keys []SplitKey)
	}{
		{
			name: "Records within same hour",
			records: []map[string]any{
				{"_cardinalhq.timestamp": int64(1672531200000), "msg": "msg1"}, // 2023-01-01 00:00:00
				{"_cardinalhq.timestamp": int64(1672532400000), "msg": "msg2"}, // 2023-01-01 00:20:00
				{"_cardinalhq.timestamp": int64(1672534799000), "msg": "msg3"}, // 2023-01-01 00:59:59
			},
			expectedSplits: 1,
			validateKeys: func(t *testing.T, keys []SplitKey) {
				assert.Len(t, keys, 1)
				assert.Equal(t, int32(20230101), keys[0].DateInt)
				assert.Equal(t, int16(0), keys[0].Hour)
			},
		},
		{
			name: "Records spanning multiple hours",
			records: []map[string]any{
				{"_cardinalhq.timestamp": int64(1672531200000), "msg": "msg1"}, // 2023-01-01 00:00:00 (hour 0)
				{"_cardinalhq.timestamp": int64(1672534800000), "msg": "msg2"}, // 2023-01-01 01:00:00 (hour 1)
				{"_cardinalhq.timestamp": int64(1672538400000), "msg": "msg3"}, // 2023-01-01 02:00:00 (hour 2)
			},
			expectedSplits: 3,
			validateKeys: func(t *testing.T, keys []SplitKey) {
				assert.Len(t, keys, 3)

				// Sort keys for predictable testing
				sort.Slice(keys, func(i, j int) bool {
					return keys[i].Hour < keys[j].Hour
				})

				assert.Equal(t, int32(20230101), keys[0].DateInt)
				assert.Equal(t, int16(0), keys[0].Hour)

				assert.Equal(t, int32(20230101), keys[1].DateInt)
				assert.Equal(t, int16(1), keys[1].Hour)

				assert.Equal(t, int32(20230101), keys[2].DateInt)
				assert.Equal(t, int16(2), keys[2].Hour)
			},
		},
		{
			name: "Records crossing day and hour boundaries",
			records: []map[string]any{
				{"_cardinalhq.timestamp": int64(1672614000000), "msg": "msg1"}, // 2023-01-01 23:00:00
				{"_cardinalhq.timestamp": int64(1672617600000), "msg": "msg2"}, // 2023-01-02 00:00:00
			},
			expectedSplits: 2,
			validateKeys: func(t *testing.T, keys []SplitKey) {
				assert.Len(t, keys, 2)

				// Sort keys for predictable testing
				sort.Slice(keys, func(i, j int) bool {
					if keys[i].DateInt != keys[j].DateInt {
						return keys[i].DateInt < keys[j].DateInt
					}
					return keys[i].Hour < keys[j].Hour
				})

				// First record: 2023-01-01 hour 23
				assert.Equal(t, int32(20230101), keys[0].DateInt)
				assert.Equal(t, int16(23), keys[0].Hour)

				// Second record: 2023-01-02 hour 0
				assert.Equal(t, int32(20230102), keys[1].DateInt)
				assert.Equal(t, int16(0), keys[1].Hour)
			},
		},
		{
			name: "Records at exact hour boundaries",
			records: []map[string]any{
				{"_cardinalhq.timestamp": int64(1672534799999), "msg": "msg1"}, // 2023-01-01 00:59:59.999 (hour 0)
				{"_cardinalhq.timestamp": int64(1672534800000), "msg": "msg2"}, // 2023-01-01 01:00:00.000 (hour 1)
			},
			expectedSplits: 2,
			validateKeys: func(t *testing.T, keys []SplitKey) {
				assert.Len(t, keys, 2)

				// Sort keys for predictable testing
				sort.Slice(keys, func(i, j int) bool {
					return keys[i].Hour < keys[j].Hour
				})

				assert.Equal(t, int32(20230101), keys[0].DateInt)
				assert.Equal(t, int16(0), keys[0].Hour)

				assert.Equal(t, int32(20230101), keys[1].DateInt)
				assert.Equal(t, int16(1), keys[1].Hour)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create input file
			inputFile, err := os.CreateTemp(tmpDir, "test-*.parquet")
			require.NoError(t, err)
			defer os.Remove(inputFile.Name())

			pw := parquet.NewWriter(inputFile, schema)
			for _, record := range tt.records {
				require.NoError(t, pw.Write(record))
			}
			require.NoError(t, pw.Close())
			require.NoError(t, inputFile.Close())

			// Load and process
			fh, err := filecrunch.LoadSchemaForFile(inputFile.Name())
			require.NoError(t, err)
			defer fh.Close()

			ll := slog.New(slog.NewTextHandler(io.Discard, nil))
			results, err := ProcessAndSplit(ll, fh, tmpDir, 20230101, 1000)
			require.NoError(t, err)

			// Validate number of splits
			assert.Len(t, results, tt.expectedSplits)

			// Extract and validate keys
			var keys []SplitKey
			for key := range results {
				keys = append(keys, key)
			}
			tt.validateKeys(t, keys)

			// Validate that each split has the correct hour in the key
			for key, result := range results {
				// Read the output file and verify all timestamps are in the correct hour
				f, err := os.Open(result.FileName)
				require.NoError(t, err)

				pr := parquet.NewReader(f, schema)
				for i := int64(0); i < result.RecordCount; i++ {
					row := map[string]any{}
					require.NoError(t, pr.Read(&row))
					ts := row["_cardinalhq.timestamp"].(int64)

					// Verify this timestamp belongs to the expected hour
					expectedDateint, expectedHour := MSToDateintHour(ts)
					assert.Equal(t, key.DateInt, expectedDateint, "Record timestamp %d should be in dateint %d, got %d", ts, key.DateInt, expectedDateint)
					assert.Equal(t, key.Hour, expectedHour, "Record timestamp %d should be in hour %d, got %d", ts, key.Hour, expectedHour)
				}
				require.NoError(t, pr.Close())
				require.NoError(t, f.Close())
			}
		})
	}
}

func TestSplitKey_HourField(t *testing.T) {
	tests := []struct {
		name     string
		key1     SplitKey
		key2     SplitKey
		areEqual bool
	}{
		{
			name:     "Same dateint and hour",
			key1:     SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230101, FileIndex: 0},
			key2:     SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230101, FileIndex: 0},
			areEqual: true,
		},
		{
			name:     "Same dateint, different hour",
			key1:     SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230101, FileIndex: 0},
			key2:     SplitKey{DateInt: 20230101, Hour: 6, IngestDateint: 20230101, FileIndex: 0},
			areEqual: false,
		},
		{
			name:     "Different dateint, same hour",
			key1:     SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230101, FileIndex: 0},
			key2:     SplitKey{DateInt: 20230102, Hour: 5, IngestDateint: 20230101, FileIndex: 0},
			areEqual: false,
		},
		{
			name:     "Different ingest dateint",
			key1:     SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230101, FileIndex: 0},
			key2:     SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230102, FileIndex: 0},
			areEqual: false,
		},
		{
			name:     "Different file index",
			key1:     SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230101, FileIndex: 0},
			key2:     SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230101, FileIndex: 1},
			areEqual: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.areEqual {
				assert.Equal(t, tt.key1, tt.key2)
			} else {
				assert.NotEqual(t, tt.key1, tt.key2)
			}
		})
	}
}

func TestSplitKey_AsMapKey(t *testing.T) {
	// Test that SplitKey works correctly as a map key
	results := make(map[SplitKey]string)

	key1 := SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230101, FileIndex: 0}
	key2 := SplitKey{DateInt: 20230101, Hour: 6, IngestDateint: 20230101, FileIndex: 0}
	key3 := SplitKey{DateInt: 20230101, Hour: 5, IngestDateint: 20230101, FileIndex: 0} // Same as key1

	results[key1] = "hour5"
	results[key2] = "hour6"
	results[key3] = "hour5_updated" // Should overwrite key1

	assert.Len(t, results, 2)
	assert.Equal(t, "hour5_updated", results[key1])
	assert.Equal(t, "hour5_updated", results[key3])
	assert.Equal(t, "hour6", results[key2])
}

// Helper function to convert milliseconds to dateint and hour (duplicated from helpers for testing)
func MSToDateintHour(ms int64) (int32, int16) {
	t := time.UnixMilli(ms).UTC()
	dateint := int32(t.Year()*10000 + int(t.Month())*100 + t.Day())
	hour := int16(t.Hour())
	return dateint, hour
}
