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

package filereader

import (
	"fmt"

	"go.opentelemetry.io/otel"
	otelmetric "go.opentelemetry.io/otel/metric"
)

var (
	rowsInCounter      otelmetric.Int64Counter
	rowsOutCounter     otelmetric.Int64Counter
	rowsDroppedCounter otelmetric.Int64Counter
)

func init() {
	meter := otel.Meter("github.com/cardinalhq/lakerunner/internal/filereader")

	var err error
	rowsInCounter, err = meter.Int64Counter(
		"lakerunner.reader.rows.in",
		otelmetric.WithDescription("Number of rows read by readers from their input source"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create rows.in counter: %w", err))
	}

	rowsOutCounter, err = meter.Int64Counter(
		"lakerunner.reader.rows.out",
		otelmetric.WithDescription("Number of rows output by readers to downstream processing"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create rows.out counter: %w", err))
	}

	rowsDroppedCounter, err = meter.Int64Counter(
		"lakerunner.reader.rows.dropped",
		otelmetric.WithDescription("Number of rows dropped by readers due to invalid data"),
	)
	if err != nil {
		panic(fmt.Errorf("failed to create rows.dropped counter: %w", err))
	}
}
