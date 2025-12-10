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

package factories

import (
	"fmt"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
)

var (
	// StreamFieldMissingCounter counts log rows that are missing both
	// resource_customer_domain and resource_service_name fields.
	StreamFieldMissingCounter otelmetric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/parquetwriter/factories")

	var err error
	StreamFieldMissingCounter, err = meter.Int64Counter(
		"lakerunner.logs.stream_field_missing",
		otelmetric.WithDescription("Number of log rows without stream identification field (resource_customer_domain or resource_service_name)"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create stream_field_missing counter: %w", err))
	}
}
