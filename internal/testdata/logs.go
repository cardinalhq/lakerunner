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

package testdata

import (
	"fmt"
	"time"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

// GenerateLogBatch creates a batch of synthetic log data for testing
func GenerateLogBatch(count int, startOffset int) *pipeline.Batch {
	batch := pipeline.GetBatch()

	baseTime := time.Date(2025, 1, 15, 10, 0, 0, 0, time.UTC).UnixMilli()

	for i := 0; i < count; i++ {
		logNum := startOffset + i
		timestamp := baseTime + int64(logNum)*1000 // 1 second apart

		row := pipeline.Row{
			wkk.RowKeyCTimestamp:    timestamp,
			wkk.RowKeyCMessage:      generateLogMessage(logNum),
			wkk.RowKeyCLevel:        selectLogLevel(logNum),
			wkk.RowKeyCService:      selectService(logNum),
			wkk.RowKeyCHost:         fmt.Sprintf("host-%d", logNum%10),
			wkk.RowKeyCContainer:    fmt.Sprintf("container-%d", logNum%5),
			wkk.RowKeyCNamespace:    "default",
			wkk.RowKeyCPod:          fmt.Sprintf("pod-%d", logNum%20),
			wkk.RowKeyCCluster:      "test-cluster",
			wkk.RowKeyCEnvironment:  "production",
			wkk.RowKeyCFingerprint:  uint64(logNum % 100), // 100 unique fingerprints
		}

		batch.AppendRow(row)
	}

	return batch
}

// generateLogMessage creates realistic-looking log messages
func generateLogMessage(logNum int) string {
	patterns := []string{
		"Processing request for user_id=%d path=/api/v1/users duration=%dms",
		"Database query executed: SELECT * FROM users WHERE id=%d (took %dms)",
		"HTTP GET /api/metrics status=200 size=%d bytes latency=%dms",
		"Cache hit for key=user:%d ttl=%d",
		"Worker processed job_id=%d queue=default time=%dms",
		"gRPC call method=/api.Service/GetUser latency=%dms status=OK",
		"Kafka message consumed topic=events partition=%d offset=%d",
		"Redis operation SET key=session:%d value_size=%d ttl=3600",
		"Authentication successful user=user%d method=oauth2 provider=google",
		"Rate limit check passed key=api:%d current=%d limit=1000",
	}

	pattern := patterns[logNum%len(patterns)]
	return fmt.Sprintf(pattern, logNum, (logNum%1000)+10)
}

// selectLogLevel picks a log level with realistic distribution
func selectLogLevel(logNum int) string {
	// 60% INFO, 25% DEBUG, 10% WARN, 4% ERROR, 1% FATAL
	switch logNum % 100 {
	case 0:
		return "FATAL"
	case 1, 2, 3, 4:
		return "ERROR"
	case 5, 6, 7, 8, 9, 10, 11, 12, 13, 14:
		return "WARN"
	case 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39:
		return "DEBUG"
	default:
		return "INFO"
	}
}

// selectService picks a service name with realistic distribution
func selectService(logNum int) string {
	services := []string{
		"api-gateway",
		"auth-service",
		"user-service",
		"order-service",
		"payment-service",
		"notification-service",
		"analytics-service",
		"search-service",
	}
	return services[logNum%len(services)]
}
