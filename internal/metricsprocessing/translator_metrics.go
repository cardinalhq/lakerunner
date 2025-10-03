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
	"strings"

	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/fingerprinter"
	"github.com/cardinalhq/lakerunner/internal/pipeline"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

// MetricTranslator adds resource metadata to metric rows
type MetricTranslator struct {
	OrgID    string
	Bucket   string
	ObjectID string
}

// TranslateRow adds resource fields to each row
// Assumes all other metric fields (including sketches) are properly set by the proto reader
func (t *MetricTranslator) TranslateRow(_ context.Context, row *pipeline.Row) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}

	// Only set the specific required fields - assume all other fields are properly set
	(*row)[wkk.RowKeyCCustomerID] = t.OrgID
	(*row)[wkk.RowKeyCTelemetryType] = "metrics"

	// Validate required timestamp field - drop row if missing or invalid
	timestamp, ok := (*row)[wkk.RowKeyCTimestamp].(int64)
	if !ok {
		return fmt.Errorf("chq_timestamp field is missing or not int64")
	}

	// Truncate timestamp to nearest 10-second interval
	const tenSecondsMs = int64(10000)
	truncatedTimestamp := (timestamp / tenSecondsMs) * tenSecondsMs
	(*row)[wkk.RowKeyCTimestamp] = truncatedTimestamp

	// Compute and add TID field
	if _, nameOk := (*row)[wkk.RowKeyCName].(string); !nameOk {
		return fmt.Errorf("missing or invalid metric_name field for TID computation")
	}

	filterKeys(row)

	// Compute TID directly from wkk.RowKey map without conversion
	tid := fingerprinter.ComputeTID(*row)
	(*row)[wkk.RowKeyCTID] = tid

	return nil
}

var (
	keepkeys = map[string]bool{
		"resource_app":                  true,
		"resource_container_image_name": true,
		"resource_container_image_tag":  true,
		"resource_k8s_cluster_name":     true,
		"resource_k8s_daemonset_name":   true,
		"resource_k8s_deployment_name":  true,
		"resource_k8s_namespace_name":   true,
		"resource_k8s_pod_ip":           true,
		"resource_k8s_pod_name":         true,
		"resource_k8s_statefulset_name": true,
		"resource_service_name":         true,
		"resource_service_version":      true,
	}
)

func filterKeys(row *pipeline.Row) {
	for k := range *row {
		name := wkk.RowKeyValue(k)
		if !strings.HasPrefix(name, "resource_") {
			continue
		}

		if keepkeys[name] {
			continue
		}

		delete(*row, k)
	}
}
