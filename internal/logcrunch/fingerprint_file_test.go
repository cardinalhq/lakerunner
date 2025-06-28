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

package logcrunch

import (
	"testing"

	"github.com/cardinalhq/lakerunner/internal/filecrunch"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
)

func TestAddfp(t *testing.T) {
	nodes := map[string]parquet.Node{
		"_cardinalhq.timestamp": parquet.Int(64),
		"resource.file":         parquet.String(),
	}
	schema := filecrunch.SchemaFromNodes(nodes)

	row := map[string]any{
		"_cardinalhq.timestamp": int64(1234567890),
		"resource.file":         "testfile.log",
	}

	accum := mapset.NewSet[int64]()
	addfp(schema, row, accum)

	sa := accum.ToSlice()
	assert.ElementsMatch(t, []int64{
		ComputeFingerprint("_cardinalhq.timestamp", ExistsRegex),
		ComputeFingerprint("resource.file", "testfile.log"),
		ComputeFingerprint("resource.file", ExistsRegex),
	}, sa)
}

func TestAddfp_ComplexRow(t *testing.T) {
	nodes := map[string]parquet.Node{
		"_cardinalhq.fingerprint":    parquet.String(),
		"_cardinalhq.hostname":       parquet.String(),
		"_cardinalhq.id":             parquet.String(),
		"_cardinalhq.level":          parquet.String(),
		"_cardinalhq.message":        parquet.String(),
		"_cardinalhq.name":           parquet.String(),
		"_cardinalhq.telemetry_type": parquet.String(),
		"_cardinalhq.timestamp":      parquet.Int(64),
		"_cardinalhq.tokenMap":       parquet.String(),
		"_cardinalhq.tokens":         parquet.String(),
		"_cardinalhq.value":          parquet.Int(64),
		"env.collector_id":           parquet.String(),
		"env.collector_name":         parquet.String(),
		"env.customer_id":            parquet.String(),
		"log.application":            parquet.String(),
		"log.controller_ip":          parquet.String(),
		"log.index_level_0":          parquet.String(),
		"log.log_level":              parquet.String(),
		"log.method":                 parquet.String(),
		"log.module":                 parquet.String(),
		"log.pid":                    parquet.String(),
		"log.protocol":               parquet.String(),
		"log.referer":                parquet.String(),
		"log.size":                   parquet.String(),
		"log.source":                 parquet.String(),
		"log.status":                 parquet.String(),
		"log.url":                    parquet.String(),
		"log.user_agent":             parquet.String(),
		"resource.bucket.name":       parquet.String(),
		"resource.file.name":         parquet.String(),
		"resource.file.type":         parquet.String(),
		"resource.file":              parquet.String(),
	}
	schema := filecrunch.SchemaFromNodes(nodes)

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

	accum := mapset.NewSet[int64]()
	addfp(schema, row, accum)

	sa := accum.ToSlice()
	assert.ElementsMatch(t, []int64{
		ComputeFingerprint("_cardinalhq.fingerprint", ExistsRegex),
		ComputeFingerprint("_cardinalhq.id", ExistsRegex),
		ComputeFingerprint("_cardinalhq.timestamp", ExistsRegex),
		ComputeFingerprint("_cardinalhq.tokenMap", ExistsRegex),
		ComputeFingerprint("_cardinalhq.tokens", ExistsRegex),
		ComputeFingerprint("_cardinalhq.value", ExistsRegex),
		ComputeFingerprint("log.index_level_0", ExistsRegex),
		ComputeFingerprint("log.log_level", ExistsRegex),
		ComputeFingerprint("log.module", ExistsRegex),
		ComputeFingerprint("log.pid", ExistsRegex),
		ComputeFingerprint("log.source", ExistsRegex),
		ComputeFingerprint("resource.bucket.name", ExistsRegex),
		ComputeFingerprint("resource.file.name", ExistsRegex),
		ComputeFingerprint("resource.file.type", ExistsRegex),

		ComputeFingerprint("_cardinalhq.message", ExistsRegex),

		ComputeFingerprint("_cardinalhq.name", "log"),
		ComputeFingerprint("_cardinalhq.name", "og."),
		ComputeFingerprint("_cardinalhq.name", "g.e"),
		ComputeFingerprint("_cardinalhq.name", ".ev"),
		ComputeFingerprint("_cardinalhq.name", "eve"),
		ComputeFingerprint("_cardinalhq.name", "ven"),
		ComputeFingerprint("_cardinalhq.name", "ent"),
		ComputeFingerprint("_cardinalhq.name", "nts"),
		ComputeFingerprint("_cardinalhq.name", ExistsRegex),

		ComputeFingerprint("_cardinalhq.telemetry_type", ExistsRegex),

		ComputeFingerprint("resource.file", "REDACTED"),
		ComputeFingerprint("resource.file", ExistsRegex),
	}, sa)
}
