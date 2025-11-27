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

package perftest

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/cardinalhq/lakerunner/internal/filereader"
	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// BenchmarkReadAndFingerprint measures the cost of:
// 1. Reading raw OTEL files → pipeline.Row (already optimized)
// 2. Fingerprinting log messages (NEW - focus of this phase)
//
// This isolates fingerprinting performance without Parquet writing.
func BenchmarkReadAndFingerprint(b *testing.B) {
	// Find raw OTEL files
	files, err := filepath.Glob(filepath.Join(testDataDir, "raw", "logs_*.binpb.gz"))
	if err != nil || len(files) == 0 {
		b.Skip("No raw test data found. Run: ./scripts/download-perf-testdata.sh raw 10")
	}

	// Use the largest file for testing
	testFile := files[0]
	for _, f := range files {
		fInfo, _ := os.Stat(f)
		testInfo, _ := os.Stat(testFile)
		if fInfo.Size() > testInfo.Size() {
			testFile = f
		}
	}

	ctx := context.Background()
	stat, _ := os.Stat(testFile)
	fileSize := stat.Size()

	b.Logf("Using file: %s (%d bytes compressed)", filepath.Base(testFile), fileSize)

	// Force single-core operation to match production pattern
	oldMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	// Create fingerprinter and trie cluster manager ONCE (reused across iterations like production)
	fp := fingerprinter.NewFingerprinter()
	trieClusterManager := fingerprinter.NewTrieClusterManager(0.5)

	b.ResetTimer()

	var totalLogsProcessed int64
	var totalFingerprints int64

	for i := 0; i < b.N; i++ {
		b.StopTimer()

		timer := NewTimer()
		sampler := NewMemorySampler(timer, 50*1000000) // Sample every 50ms
		sampler.Start()

		// Create reader for raw OTEL log file
		options := filereader.ReaderOptions{
			SignalType: filereader.SignalTypeLogs,
			BatchSize:  1000,
			OrgID:      "test-org",
		}

		b.StartTimer()

		reader, err := filereader.ReaderForFileWithOptions(testFile, options)
		if err != nil {
			b.Fatal(err)
		}

		logsRead := int64(0)
		fingerprintsGenerated := int64(0)

		for {
			batch, err := reader.Next(ctx)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}

			logsRead += int64(batch.Len())
			timer.AddLogs(int64(batch.Len()))

			// Fingerprint each log message
			for j := 0; j < batch.Len(); j++ {
				row := batch.Get(j)
				if msg, ok := row[wkk.RowKeyCMessage].(string); ok && msg != "" {
					fingerprintVal, _, err := fp.Fingerprint(msg, trieClusterManager)
					if err == nil && fingerprintVal != 0 {
						fingerprintsGenerated++
					}
				}
			}

			pipeline.ReturnBatch(batch)
		}

		totalLogsProcessed += logsRead
		totalFingerprints += fingerprintsGenerated

		_ = reader.Close()

		b.StopTimer()
		sampler.Stop()
		metrics := timer.Stop()

		if i == 0 {
			b.Logf("\n%s", metrics.Report("Read → Fingerprint"))
			b.Logf("Read %d logs, generated %d fingerprints (%.1f%% coverage)",
				logsRead, fingerprintsGenerated, float64(fingerprintsGenerated)/float64(logsRead)*100)
		}
	}

	// Report overall metrics
	b.ReportMetric(float64(totalLogsProcessed)/float64(b.N)/b.Elapsed().Seconds()*float64(b.N), "logs/sec")
	b.ReportMetric(float64(totalLogsProcessed)/float64(b.N)/b.Elapsed().Seconds()*float64(b.N), "logs/sec/core")
	b.ReportMetric(float64(totalFingerprints)/float64(b.N)/b.Elapsed().Seconds()*float64(b.N), "fingerprints/sec")
}
