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

package artifactfetcher

import (
	"context"
	"log"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	fetchErrors metric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/queryapi/artifactfetcher")

	var err error

	fetchErrors, err = meter.Int64Counter(
		"lakerunner.query.artifact.fetch_errors",
		metric.WithDescription("Number of artifact fetch errors"),
	)
	if err != nil {
		log.Fatalf("failed to create artifact.fetch_errors counter: %v", err)
	}
}

func recordFetchError(reason string) {
	fetchErrors.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("error_reason", reason),
	))
}
