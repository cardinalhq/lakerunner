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

package exemplarreceiver

import (
	"github.com/cardinalhq/lakerunner/pipeline"
)

// LogsBatchRequest represents a batch of logs exemplars from a single source
type LogsBatchRequest struct {
	Source    string         `json:"source"`
	Exemplars []LogsExemplar `json:"exemplars"`
}

// MetricsBatchRequest represents a batch of metrics exemplars from a single source
type MetricsBatchRequest struct {
	Source    string            `json:"source"`
	Exemplars []MetricsExemplar `json:"exemplars"`
}

// TracesBatchRequest represents a batch of traces exemplars from a single source
type TracesBatchRequest struct {
	Source    string           `json:"source"`
	Exemplars []TracesExemplar `json:"exemplars"`
}

// LogsExemplar represents a single logs exemplar with explicit fingerprinting fields
type LogsExemplar struct {
	ServiceName *string      `json:"service_name"`
	ClusterName *string      `json:"cluster_name"`
	Namespace   *string      `json:"namespace"`
	Message     string       `json:"message"`
	Level       string       `json:"level"`
	Attributes  pipeline.Row `json:"attributes"`
}

// MetricsExemplar represents a single metrics exemplar with explicit fingerprinting fields
type MetricsExemplar struct {
	ServiceName *string      `json:"service_name"`
	ClusterName *string      `json:"cluster_name"`
	Namespace   *string      `json:"namespace"`
	MetricName  string       `json:"metric_name"`
	MetricType  string       `json:"metric_type"`
	Attributes  pipeline.Row `json:"attributes"`
}

// TracesExemplar represents a single traces exemplar with explicit fingerprinting fields
type TracesExemplar struct {
	ServiceName *string      `json:"service_name"`
	ClusterName *string      `json:"cluster_name"`
	Namespace   *string      `json:"namespace"`
	SpanName    string       `json:"span_name"`
	SpanKind    string       `json:"span_kind"`
	Attributes  pipeline.Row `json:"attributes"`
}

// ExemplarBatchResponse represents the response from a batch upsert operation
type ExemplarBatchResponse struct {
	Status   string   `json:"status"`
	Accepted int      `json:"accepted"`
	Failed   int      `json:"failed,omitempty"`
	Errors   []string `json:"errors,omitempty"`
}
