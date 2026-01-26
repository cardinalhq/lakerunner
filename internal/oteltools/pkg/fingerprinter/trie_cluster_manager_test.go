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

package fingerprinter

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSimpleClustering(t *testing.T) {
	messages := []string{
		"Error reading file from path /var/logs/app.log",
		"Error reading file from path /usr/logs/app.log",
		"Failed to connect to database at db-server",
		"Connection error to database at db-server",
	}

	// threshold = 0.5
	clusterManager := NewTrieClusterManager(0.5)
	fpr := NewFingerprinter(WithMaxTokens(50))

	fps := make([]int64, len(messages))
	for i, msg := range messages {
		fp, _, err := fpr.Fingerprint(msg, clusterManager)
		assert.NoError(t, err)
		fps[i] = fp
	}

	// first two should share a cluster
	assert.Equal(t, fps[0], fps[1], "msgs 0&1 → same cluster")
	// last two share a different cluster
	assert.Equal(t, fps[2], fps[3], "msgs 2&3 → same cluster")
	// cross‐cluster fingerprints differ
	assert.NotEqual(t, fps[0], fps[2], "clusters 0/1 vs 2/3 differ")
}

func TestEnvoyAccessLogClustering(t *testing.T) {
	message1 := `[2025-01-15T01:37:14.008Z] "GET /search/tickets?account_id=11&page=&per_page=&query=test HTTP/1.1" 200 - via_upstream - "-" 0 2654 40 40 "54.162.8.237,172.25.31.44" "Typhoeus - https://github.com/typhoeus/typhoeus" "1e967019-52c7-410f-8d9c-cbb27e097f87" "search-service.freshstatus-sta91ng.io" "172.25.29.139:8181" outbound|80|BLUE|aiops-search.ams-aiops-search-staging.svc.cluster.local 172.25.27.204:33730 172.25.27.204:8080 172.25.31.44:9664 - -`
	message2 := `[2025-01-15T01:47:22.192Z] "POST /freshservice/bulk HTTP/1.1" 204 - via_upstream - "-" 1305 0 2 1 "-" "-" "b4d36036-5a5c-4444-a34e-8e628bfc869e" "internal-haystack-write-1499015305.us-east-1.elb.amazonaws.com:8080" "10.98.1.150:8080" PassthroughCluster 172.25.26.133:43784 10.98.1.150:8080 172.25.26.133:48440 - allow_any`
	message3 := `[2025-01-15T01:47:18.753Z] "GET /fcp/alb-health HTTP/1.1" 200 - via_upstream - "-" 0 0 0 0 "172.25.17.113" "ELB-HealthChecker/2.0" "98484ab4-2201-49e9-b99d-4af5427cc1c8" "172.25.27.204:8080" "172.25.27.204:15021" outbound|15021||istio-ingressgateway.istio-system.svc.cluster.local 172.25.27.204:44742 172.25.27.204:8080 172.25.17.113:28198 - -`
	message4 := `[2025-01-15T01:42:16.994Z] "GET /tickets HTTP/1.1" 200 - via_upstream - "-" 0 8443 1156 1155 "54.162.8.237,172.25.17.113" "Ruby" "59fb30b6-62e0-4ec3-940c-4c0eecda3c3e" "aiops-test10.freshstatus-sta91ng.io" "172.25.26.133:8181" inbound|8181|| 127.0.0.6:52821 172.25.26.133:8181 172.25.17.113:0 outbound_.80_.blue_.aiops-tickets.ams-aiops-tickets-staging.svc.cluster.local default`
	message5 := `[2025-01-15T01:47:27.309Z] "GET /metrics HTTP/1.1" 200 - via_upstream - "-" 0 2260 1 1 "-" "Prometheus/v0.18.2" "93068269-58a4-4258-a451-ff9ec522ab20" "172.25.26.133:9394" "172.25.26.133:9394" inbound|9394|| 127.0.0.6:54671 172.25.26.133:9394 172.25.26.165:36862 - default`
	message6 := `[2025-01-15T01:47:27.309Z] "GET /metrics HTTP/1.1" 200 - via_upstream - "-" 0 2260 1 1 "-" "Prometheus/v0.18.2" "93068269-58a4-4258-a451-ff9ec522ab20" "172.25.26.133:9394" "172.25.26.133:9394" inbound|9394|| 127.0.0.6:54671 172.25.26.133:9394 172.25.26.165:36862 - default`
	message7 := `[2025-01-15T01:47:27.309Z] "GET /metrics HTTP/1.1" 200 - via_upstream - "-" 0 2260 1 1 "-" "Prometheus/v0.18.2" "93068269-58a4-4258-a451-ff9ec522ab20" "172.25.26.133:9394" "172.25.26.133:9394" inbound|9394|| 127.0.0.6:54671 172.25.26.133:9394 172.25.26.165:36862 - default`

	clusterManager := NewTrieClusterManager(0.5)
	fpr := NewFingerprinter(WithMaxTokens(50))

	// Fingerprint the messages, and ensure they all have the same fingerprint.
	fps := make([]int64, 0)
	for _, msg := range []string{message1, message2, message3, message4, message5, message6, message7} {
		fp, _, err := fpr.Fingerprint(msg, clusterManager)
		assert.NoError(t, err)
		fps = append(fps, fp)
	}
	// All messages should have the same fingerprint
	assert.Equal(t, fps[0], fps[1], "msgs 0&1 → same cluster")
	assert.Equal(t, fps[0], fps[2], "msgs 0&2 → same cluster")
	assert.Equal(t, fps[0], fps[3], "msgs 0&3 → same cluster")
	assert.Equal(t, fps[0], fps[4], "msgs 0&4 → same cluster")
	assert.Equal(t, fps[0], fps[5], "msgs 0&5 → same cluster")
	assert.Equal(t, fps[0], fps[6], "msgs 0&6 → same cluster")
}

func TestClusteringOnReadingGlob(t *testing.T) {
	message1 := `[ceb1f20]Error in reading glob, sql = SELECT * FROM (SELECT * FROM read_parquet(['./db/415c9d63-5d29-4b7a-92ac-5c2c7ba0d672/chq-ccstats/20241219/metrics/21/tbl_15602004457141.parquet', './db/415c9d63-5d29-4b7a-92ac-5c2c7ba0d672/chq-ccstats/20241219/metrics/21/tbl_15301210694110.parquet'], union_by_name=True) WHERE "chq_timestamp" > 1734643100000 AND "chq_timestamp" <= 1734643840000) WHERE ((( "metric_name" = 'ruby.http.request.duration.seconds' and "resource_service_name" = 'api') and "metric_app" = 'aiops-ams') and "chq_telemetry_type" = 'metrics') java.sql.SQLException: Binder Error: Referenced column "metric_app" not found in FROM clause!`
	message2 := `[786a039]Error in reading glob, sql = SELECT "metric_app" as "metric_app", COUNT(*) AS count FROM read_parquet(['./db/415c9d63-5d29-4b7a-92ac-5c2c7ba0d672/chq-ccstats/20241219/metrics/21/tbl_14547073845256.parquet', './db/415c9d63-5d29-4b7a-92ac-5c2c7ba0d672/chq-ccstats/20241219/metrics/21/tbl_14641144825111.parquet'], union_by_name=True) WHERE ((" metric_name" = 'ruby.http.requests.total' and "chq_telemetry_type" = 'metrics') and "metric_app" IS NOT NULL) AND "chq_timestamp" > 1734642320000 AND "chq_timestamp" <= 1734643060000 GROUP BY "metric_app" java.sql.SQLException: Binder Error: Referenced column "metric_app" not found in FROM clause! Candidate bindings: "read_parquet.metric_data_type", "read_parquet.metric_transport", "read_parquet.metric_signal" LINE 1: SELECT "metric_app" as "metric_app", COUNT(*) ...`

	clusterManager := NewTrieClusterManager(0.5)
	fpr := NewFingerprinter(WithMaxTokens(50))

	// Fingerprint the messages, and ensure they all have the same fingerprint.
	fps := make([]int64, 0)
	for _, msg := range []string{message1, message2} {
		fp, _, err := fpr.Fingerprint(msg, clusterManager)
		assert.NoError(t, err)
		fps = append(fps, fp)
	}
	// All messages should have the same fingerprint
	assert.Equal(t, fps[0], fps[1], "msgs 0&1 → same cluster")
}

func TestClusteringOnLorenIpsum(t *testing.T) {
	message1 := "[de5515ba-98a0-4c1d-be32-ae61152cb0b8]   \033[1m\033[36mTicket Create (1.8ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Et dignissimos debitis voluptatum.', 'Omnis dolor error. Deleniti sint hic. Labore omnis id.', 585378, 11, '2025-01-13 17:42:43.050272', '2025-01-13 17:42:43.050272')\033[0m"
	message2 := "[5b3d31c9-7fc8-4b4b-a38f-b0bcf82434a6]   \033[1m\033[36mTicket Create (1.6ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Occaecati illum voluptas quibusdam.', 'Excepturi tenetur non. Ullam incidunt expedita. Explicabo earum reiciendis.', 584719, 11, '2025-01-13 07:03:52.694513', '2025-01-13 07:03:52.694513')\033[0m"
	message3 := "[5d8d83e3-d52c-461e-8c7c-4b10eab2a159]   \033[1m\033[36mTicket Create (1.7ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Est sit itaque illum.', 'Aliquam assumenda consequatur. Porro doloribus perspiciatis. Illum cumque voluptate.', 584482, 11, '2025-01-13 03:04:32.161775', '2025-01-13 03:04:32.161775')\033[0m"
	message4 := "[1a56410f-a24d-4a6b-aad9-dd4267069f20]   \033[1m\033[36mTicket Create (1.7ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Quis beatae enim iste.', 'Reprehenderit voluptas rem. Porro cupiditate amet. Atque recusandae eius.', 585360, 11, '2025-01-13 17:28:12.478078', '2025-01-13 17:28:12.478078')\033[0m"
	message5 := "[1a41a6cb-28e9-4806-a40b-822a38fb4630]   \033[1m\033[36mTicket Create (1.6ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Dignissimos repellendus et quam.', 'Minima laboriosam aut. Quas sapiente ut. Facilis ipsa animi.', 585165, 11, '2025-01-13 14:07:36.160576', '2025-01-13 14:07:36.160576')\033[0m"

	clusterManager := NewTrieClusterManager(0.5)
	fpr := NewFingerprinter(WithMaxTokens(50))

	// Fingerprint the messages, and ensure they all have the same fingerprint.
	fps := make([]int64, 0)
	for _, msg := range []string{message1, message2, message3, message4, message5} {
		fp, _, err := fpr.Fingerprint(msg, clusterManager)
		assert.NoError(t, err)
		fps = append(fps, fp)
	}
	// All messages should have the same fingerprint
	assert.Equal(t, fps[0], fps[1], "msgs 0&1 → same cluster")
	assert.Equal(t, fps[0], fps[2], "msgs 0&2 → same cluster")
	assert.Equal(t, fps[0], fps[3], "msgs 0&3 → same cluster")
	assert.Equal(t, fps[0], fps[4], "msgs 0&4 → same cluster")
}

func TestPartialPrefixDivergence(t *testing.T) {
	cm := NewTrieClusterManager(0.8)
	fpr := NewFingerprinter(WithMaxTokens(50))
	fp1, _, _ := fpr.Fingerprint("foo bar baz qux", cm)
	fp2, _, _ := fpr.Fingerprint("foo bar baz quux", cm)
	// share prefix “foo bar baz”
	assert.NotEqual(t, fp1, fp2, "should be distinct clusters under the same subtrie")
}
