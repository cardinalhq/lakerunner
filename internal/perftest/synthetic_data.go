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
	"fmt"
	"math/rand"
	"time"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// SyntheticDataGenerator creates realistic log data for benchmarking.
type SyntheticDataGenerator struct {
	rand *rand.Rand
}

// NewSyntheticDataGenerator creates a new generator with a fixed seed for reproducibility.
func NewSyntheticDataGenerator() *SyntheticDataGenerator {
	return &SyntheticDataGenerator{
		rand: rand.New(rand.NewSource(42)),
	}
}

// GenerateBatches creates batches of synthetic log data.
func (g *SyntheticDataGenerator) GenerateBatches(totalRows, batchSize int) []*pipeline.Batch {
	batches := make([]*pipeline.Batch, 0, (totalRows+batchSize-1)/batchSize)

	baseTime := time.Now().Add(-24 * time.Hour).UnixMilli()

	for rowsCreated := 0; rowsCreated < totalRows; {
		rowsInBatch := batchSize
		if rowsCreated+rowsInBatch > totalRows {
			rowsInBatch = totalRows - rowsCreated
		}

		batch := pipeline.GetBatch()
		for i := 0; i < rowsInBatch; i++ {
			row := batch.AddRow()
			g.populateLogRow(row, baseTime+int64(rowsCreated+i)*1000)
		}

		batches = append(batches, batch)
		rowsCreated += rowsInBatch
	}

	return batches
}

// populateLogRow fills a row with realistic log data.
func (g *SyntheticDataGenerator) populateLogRow(row pipeline.Row, timestamp int64) {

	// Core OTEL fields
	row[wkk.RowKeyCTimestamp] = timestamp
	row[wkk.RowKeyCMessage] = g.randomLogMessage()
	row[wkk.RowKeyCLevel] = g.randomSeverity()
	row[wkk.RowKeyCFingerprint] = g.rand.Int63()

	// Resource attributes (always present)
	row[wkk.NewRowKey("resource_service_name")] = g.randomServiceName()
	row[wkk.NewRowKey("resource_service_namespace")] = "production"
	row[wkk.NewRowKey("resource_service_version")] = g.randomVersion()
	row[wkk.NewRowKey("resource_host_name")] = g.randomHostname()
	row[wkk.NewRowKey("resource_host_id")] = g.randomUUID()
	row[wkk.NewRowKey("resource_cloud_provider")] = "aws"
	row[wkk.NewRowKey("resource_cloud_region")] = g.randomRegion()
	row[wkk.NewRowKey("resource_cloud_zone")] = g.randomZone()
	row[wkk.NewRowKey("resource_k8s_cluster_name")] = "prod-cluster-01"
	row[wkk.NewRowKey("resource_k8s_namespace_name")] = g.randomNamespace()
	row[wkk.NewRowKey("resource_k8s_pod_name")] = g.randomPodName()
	row[wkk.NewRowKey("resource_k8s_container_name")] = g.randomContainerName()

	// Scope attributes (always present)
	row[wkk.NewRowKey("scope_name")] = "github.com/cardinalhq/lakerunner"
	row[wkk.NewRowKey("scope_version")] = "1.0.0"

	// Log-specific attributes (sparse - only some logs have these)
	if g.rand.Float32() < 0.3 {
		row[wkk.NewRowKey("attr_http_method")] = g.randomHTTPMethod()
		row[wkk.NewRowKey("attr_http_status_code")] = g.randomHTTPStatus()
		row[wkk.NewRowKey("attr_http_url")] = g.randomURL()
		row[wkk.NewRowKey("attr_http_user_agent")] = g.randomUserAgent()
	}

	if g.rand.Float32() < 0.2 {
		row[wkk.NewRowKey("attr_db_system")] = "postgresql"
		row[wkk.NewRowKey("attr_db_name")] = g.randomDBName()
		row[wkk.NewRowKey("attr_db_operation")] = g.randomDBOperation()
		row[wkk.NewRowKey("attr_db_query_duration_ms")] = g.rand.Intn(5000)
	}

	if g.rand.Float32() < 0.15 {
		row[wkk.NewRowKey("attr_error")] = true
		row[wkk.NewRowKey("attr_error_type")] = g.randomErrorType()
		row[wkk.NewRowKey("attr_error_message")] = g.randomErrorMessage()
		row[wkk.NewRowKey("attr_error_stack")] = g.randomStackTrace()
	}

	if g.rand.Float32() < 0.25 {
		row[wkk.NewRowKey("attr_user_id")] = g.randomUserID()
		row[wkk.NewRowKey("attr_session_id")] = g.randomUUID()
		row[wkk.NewRowKey("attr_request_id")] = g.randomUUID()
	}

	// Numeric metrics (sparse)
	if g.rand.Float32() < 0.4 {
		row[wkk.NewRowKey("attr_duration_ms")] = g.rand.Intn(10000)
		row[wkk.NewRowKey("attr_bytes_sent")] = g.rand.Intn(1000000)
		row[wkk.NewRowKey("attr_bytes_received")] = g.rand.Intn(1000000)
	}

	// Add some completely sparse attributes (only 5% of rows)
	if g.rand.Float32() < 0.05 {
		row[wkk.NewRowKey("attr_cache_hit")] = g.rand.Float32() < 0.7
		row[wkk.NewRowKey("attr_cache_key")] = g.randomCacheKey()
	}

	if g.rand.Float32() < 0.05 {
		row[wkk.NewRowKey("attr_queue_name")] = g.randomQueueName()
		row[wkk.NewRowKey("attr_queue_size")] = g.rand.Intn(10000)
	}
}

// Random data generators
func (g *SyntheticDataGenerator) randomLogMessage() string {
	messages := []string{
		"Request processed successfully",
		"Database query executed",
		"Cache miss for key",
		"User authentication successful",
		"API request received",
		"Background job started",
		"Webhook delivered",
		"File uploaded",
		"Email sent",
		"Payment processed",
	}
	return messages[g.rand.Intn(len(messages))]
}

func (g *SyntheticDataGenerator) randomSeverity() string {
	severities := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	weights := []int{60, 20, 10, 10} // INFO most common

	total := 0
	for _, w := range weights {
		total += w
	}

	r := g.rand.Intn(total)
	cumulative := 0
	for i, w := range weights {
		cumulative += w
		if r < cumulative {
			return severities[i]
		}
	}
	return "INFO"
}

func (g *SyntheticDataGenerator) randomServiceName() string {
	services := []string{"api-gateway", "auth-service", "user-service", "payment-service", "notification-service"}
	return services[g.rand.Intn(len(services))]
}

func (g *SyntheticDataGenerator) randomVersion() string {
	return fmt.Sprintf("v1.%d.%d", g.rand.Intn(10), g.rand.Intn(20))
}

func (g *SyntheticDataGenerator) randomHostname() string {
	return fmt.Sprintf("ip-%d-%d-%d-%d", 10, g.rand.Intn(256), g.rand.Intn(256), g.rand.Intn(256))
}

func (g *SyntheticDataGenerator) randomUUID() string {
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		g.rand.Uint32(), g.rand.Intn(65536), g.rand.Intn(65536),
		g.rand.Intn(65536), g.rand.Int63n(281474976710656))
}

func (g *SyntheticDataGenerator) randomRegion() string {
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}
	return regions[g.rand.Intn(len(regions))]
}

func (g *SyntheticDataGenerator) randomZone() string {
	zones := []string{"a", "b", "c"}
	return zones[g.rand.Intn(len(zones))]
}

func (g *SyntheticDataGenerator) randomNamespace() string {
	namespaces := []string{"backend", "frontend", "jobs", "monitoring"}
	return namespaces[g.rand.Intn(len(namespaces))]
}

func (g *SyntheticDataGenerator) randomPodName() string {
	return fmt.Sprintf("pod-%s-%d", g.randomServiceName(), g.rand.Intn(10))
}

func (g *SyntheticDataGenerator) randomContainerName() string {
	return "app"
}

func (g *SyntheticDataGenerator) randomHTTPMethod() string {
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	return methods[g.rand.Intn(len(methods))]
}

func (g *SyntheticDataGenerator) randomHTTPStatus() int {
	statuses := []int{200, 201, 204, 400, 404, 500, 502}
	return statuses[g.rand.Intn(len(statuses))]
}

func (g *SyntheticDataGenerator) randomURL() string {
	return fmt.Sprintf("/api/v1/users/%d", g.rand.Intn(10000))
}

func (g *SyntheticDataGenerator) randomUserAgent() string {
	agents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
		"curl/7.68.0",
		"Go-http-client/1.1",
	}
	return agents[g.rand.Intn(len(agents))]
}

func (g *SyntheticDataGenerator) randomDBName() string {
	dbs := []string{"users_db", "orders_db", "products_db"}
	return dbs[g.rand.Intn(len(dbs))]
}

func (g *SyntheticDataGenerator) randomDBOperation() string {
	ops := []string{"SELECT", "INSERT", "UPDATE", "DELETE"}
	return ops[g.rand.Intn(len(ops))]
}

func (g *SyntheticDataGenerator) randomErrorType() string {
	types := []string{"ValidationError", "DatabaseError", "NetworkError", "TimeoutError"}
	return types[g.rand.Intn(len(types))]
}

func (g *SyntheticDataGenerator) randomErrorMessage() string {
	messages := []string{
		"Connection timeout",
		"Invalid input parameter",
		"Database constraint violation",
		"Resource not found",
	}
	return messages[g.rand.Intn(len(messages))]
}

func (g *SyntheticDataGenerator) randomStackTrace() string {
	return "at processRequest (app.js:123)\nat handleRoute (router.js:45)\nat server.js:89"
}

func (g *SyntheticDataGenerator) randomUserID() string {
	return fmt.Sprintf("user_%d", g.rand.Intn(100000))
}

func (g *SyntheticDataGenerator) randomCacheKey() string {
	return fmt.Sprintf("cache:user:%d:profile", g.rand.Intn(10000))
}

func (g *SyntheticDataGenerator) randomQueueName() string {
	queues := []string{"email-queue", "webhook-queue", "analytics-queue"}
	return queues[g.rand.Intn(len(queues))]
}
