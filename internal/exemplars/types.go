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

package exemplars

import (
	"strconv"

	"github.com/cespare/xxhash/v2"

	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

var (
	RowKeyResourceServiceName      = wkk.NewRowKey("resource_service_name")
	RowKeyResourceK8sClusterName   = wkk.NewRowKey("resource_k8s_cluster_name")
	RowKeyResourceK8sNamespaceName = wkk.NewRowKey("resource_k8s_namespace_name")
	RowKeyCardinalhqOldFingerprint = wkk.NewRowKey("chq_old_fingerprint")
	RowKeySpanName                 = wkk.NewRowKey("span_name")
	RowKeySpanKind                 = wkk.NewRowKey("span_kind")
)

// computeLogsTracesKey computes a xxhash for logs and traces exemplar deduplication
func computeLogsTracesKey(clusterName, namespaceName, serviceName string, fingerprint int64) uint64 {
	h := xxhash.New()
	_, _ = h.WriteString(clusterName)
	_, _ = h.WriteString("|")
	_, _ = h.WriteString(namespaceName)
	_, _ = h.WriteString("|")
	_, _ = h.WriteString(serviceName)
	_, _ = h.WriteString("|")
	_, _ = h.WriteString(strconv.FormatInt(fingerprint, 10))
	return h.Sum64()
}

// computeMetricsKey computes a xxhash for metrics exemplar deduplication
func computeMetricsKey(clusterName, namespaceName, serviceName, metricName, metricType string) uint64 {
	h := xxhash.New()
	_, _ = h.WriteString(clusterName)
	_, _ = h.WriteString("|")
	_, _ = h.WriteString(namespaceName)
	_, _ = h.WriteString("|")
	_, _ = h.WriteString(serviceName)
	_, _ = h.WriteString("|")
	_, _ = h.WriteString(metricName)
	_, _ = h.WriteString("|")
	_, _ = h.WriteString(metricType)
	return h.Sum64()
}
