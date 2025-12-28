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

package metricsprocessing

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/cardinalhq/lakerunner/internal/duckdbx"
	"github.com/cardinalhq/lakerunner/internal/fingerprint"
)

// TestFingerprintSpeedComparison compares fingerprinting speed between Rust and Go
func TestFingerprintSpeedComparison(t *testing.T) {
	testFile := filepath.Join("..", "..", "testdata", "traces", "otel-traces.binpb.gz")
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		t.Skipf("Test file does not exist: %s", testFile)
	}

	ctx := context.Background()
	tmpDir := t.TempDir()
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	// Copy test file to temp dir
	testData, err := os.ReadFile(testFile)
	require.NoError(t, err)
	segmentFile := filepath.Join(tmpDir, "segment.binpb.gz")
	require.NoError(t, os.WriteFile(segmentFile, testData, 0644))

	// Create DuckDB connection
	db, err := duckdbx.NewDB()
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	require.NoError(t, err)
	defer release()

	err = duckdbx.LoadOtelBinpbExtension(ctx, conn)
	require.NoError(t, err)

	// Benchmark Rust extension reading with fingerprints
	t.Run("RustExtension", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT chq_fingerprint, resource_service_name, span_trace_id
			FROM otel_traces_read('%s', customer_id='%s')
		`, segmentFile, orgID)

		start := time.Now()
		rows, err := conn.QueryContext(ctx, query)
		require.NoError(t, err)

		var count int
		var totalFingerprints int
		for rows.Next() {
			var fpJSON string
			var serviceName, traceID *string
			require.NoError(t, rows.Scan(&fpJSON, &serviceName, &traceID))
			count++
			// Count fingerprints (rough estimate from JSON length)
			totalFingerprints += (len(fpJSON) / 20) // ~20 chars per fingerprint
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())

		rustDuration := time.Since(start)
		t.Logf("Rust extension: %d spans, ~%d fingerprints in %v", count, totalFingerprints, rustDuration)
		t.Logf("Rust: %.2f spans/ms, %.2f fingerprints/ms", float64(count)/rustDuration.Seconds()/1000, float64(totalFingerprints)/rustDuration.Seconds()/1000)
	})

	// Benchmark reading without fingerprints (baseline)
	t.Run("RustExtensionNoFingerprint", func(t *testing.T) {
		query := fmt.Sprintf(`
			SELECT resource_service_name, span_trace_id, span_name
			FROM otel_traces_read('%s', customer_id='%s')
		`, segmentFile, orgID)

		start := time.Now()
		rows, err := conn.QueryContext(ctx, query)
		require.NoError(t, err)

		var count int
		for rows.Next() {
			var serviceName, traceID, spanName *string
			require.NoError(t, rows.Scan(&serviceName, &traceID, &spanName))
			count++
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())

		baselineDuration := time.Since(start)
		t.Logf("Rust extension (no fingerprints): %d spans in %v", count, baselineDuration)
	})

	// Benchmark Go fingerprinting on equivalent data
	t.Run("GoFingerprinting", func(t *testing.T) {
		// First, get the raw attribute data from Rust
		query := fmt.Sprintf(`
			SELECT
				resource_service_name,
				resource_k8s_namespace_name,
				resource_k8s_cluster_name,
				span_trace_id,
				span_name,
				span_kind
			FROM otel_traces_read('%s', customer_id='%s')
		`, segmentFile, orgID)

		rows, err := conn.QueryContext(ctx, query)
		require.NoError(t, err)

		type spanData struct {
			serviceName   string
			namespace     string
			cluster       string
			traceID       string
			spanName      string
			spanKind      string
		}

		var spans []spanData
		for rows.Next() {
			var sn, ns, cl, tid, name, kind *string
			require.NoError(t, rows.Scan(&sn, &ns, &cl, &tid, &name, &kind))
			sd := spanData{}
			if sn != nil {
				sd.serviceName = *sn
			}
			if ns != nil {
				sd.namespace = *ns
			}
			if cl != nil {
				sd.cluster = *cl
			}
			if tid != nil {
				sd.traceID = *tid
			}
			if name != nil {
				sd.spanName = *name
			}
			if kind != nil {
				sd.spanKind = *kind
			}
			spans = append(spans, sd)
		}
		require.NoError(t, rows.Close())
		require.NoError(t, rows.Err())

		// Now benchmark Go fingerprinting
		start := time.Now()
		var totalFingerprints int
		for _, span := range spans {
			tagValues := map[string]mapset.Set[string]{
				"chq_telemetry_type":          mapset.NewSet("traces"),
				"resource_service_name":       mapset.NewSet(span.serviceName),
				"resource_k8s_namespace_name": mapset.NewSet(span.namespace),
				"resource_k8s_cluster_name":   mapset.NewSet(span.cluster),
				"span_trace_id":               mapset.NewSet(span.traceID),
				"span_name":                   mapset.NewSet(span.spanName),
				"span_kind":                   mapset.NewSet(span.spanKind),
			}
			fps := fingerprint.ToFingerprints(tagValues)
			totalFingerprints += fps.Cardinality()
		}
		goDuration := time.Since(start)

		t.Logf("Go fingerprinting: %d spans, %d fingerprints in %v", len(spans), totalFingerprints, goDuration)
		t.Logf("Go: %.2f spans/ms, %.2f fingerprints/ms", float64(len(spans))/goDuration.Seconds()/1000, float64(totalFingerprints)/goDuration.Seconds()/1000)
	})
}

// BenchmarkRustFingerprinting benchmarks the Rust extension fingerprinting
func BenchmarkRustFingerprinting(b *testing.B) {
	testFile := filepath.Join("..", "..", "testdata", "traces", "otel-traces.binpb.gz")
	if _, err := os.Stat(testFile); os.IsNotExist(err) {
		b.Skipf("Test file does not exist: %s", testFile)
	}

	ctx := context.Background()
	tmpDir := b.TempDir()
	orgID := uuid.MustParse("12340000-0000-4000-8000-000000000001").String()

	testData, err := os.ReadFile(testFile)
	if err != nil {
		b.Fatal(err)
	}
	segmentFile := filepath.Join(tmpDir, "segment.binpb.gz")
	if err := os.WriteFile(segmentFile, testData, 0644); err != nil {
		b.Fatal(err)
	}

	db, err := duckdbx.NewDB()
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	conn, release, err := db.GetConnection(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer release()

	if err := duckdbx.LoadOtelBinpbExtension(ctx, conn); err != nil {
		b.Fatal(err)
	}

	query := fmt.Sprintf(`
		SELECT chq_fingerprint
		FROM otel_traces_read('%s', customer_id='%s')
	`, segmentFile, orgID)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := conn.QueryContext(ctx, query)
		if err != nil {
			b.Fatal(err)
		}
		for rows.Next() {
			var fp string
			if err := rows.Scan(&fp); err != nil {
				b.Fatal(err)
			}
		}
		_ = rows.Close()
	}
}
