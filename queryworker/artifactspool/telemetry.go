// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package artifactspool

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	ttlCleanups metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryworker/artifactspool")

	var err error

	ttlCleanups, err = meter.Int64Counter(
		"lakerunner.query.artifact.ttl_cleanups",
		metric.WithDescription("Number of artifacts expired by TTL cleanup"),
	)
	if err != nil {
		log.Fatalf("failed to create artifact.ttl_cleanups counter: %v", err)
	}
}

func registerSpoolGauges(s *Spool) {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryworker/artifactspool")

	_, err := meter.Int64ObservableGauge(
		"lakerunner.query.artifact.spool_bytes",
		metric.WithDescription("Total bytes used by artifact spool"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(s.TotalBytes())
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("failed to create artifact.spool_bytes gauge: %v", err)
	}

	_, err = meter.Int64ObservableGauge(
		"lakerunner.query.artifact.spool_count",
		metric.WithDescription("Number of artifacts in spool"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(s.Count()))
			return nil
		}),
	)
	if err != nil {
		log.Fatalf("failed to create artifact.spool_count gauge: %v", err)
	}
}

func recordTTLCleanup() {
	ttlCleanups.Add(context.Background(), 1)
}
