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

package rowcodec

import (
	"bytes"
	"testing"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

func BenchmarkSpillCodec(b *testing.B) {
	codec := NewSpillCodec()
	row := benchmarkRow()
	dst := pipeline.Row{}
	buf := bytes.NewBuffer(make([]byte, 0, 2048))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		byteLen, err := codec.EncodeRowTo(buf, row)
		if err != nil {
			b.Fatal(err)
		}
		if int(byteLen) != buf.Len() {
			b.Fatalf("payload mismatch %d vs %d", byteLen, buf.Len())
		}
		if err := codec.DecodeRowFrom(bytes.NewReader(buf.Bytes()), dst); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkRow() pipeline.Row {
	return pipeline.Row{
		wkk.NewRowKey("byte"):     byte(7),
		wkk.NewRowKey("bytes"):    []byte{1, 2, 3, 4},
		wkk.NewRowKey("int64"):    int64(-123456789),
		wkk.NewRowKey("float64"):  float64(123.456),
		wkk.NewRowKey("strings"):  []string{"alpha", "beta", "gamma"},
		wkk.NewRowKey("bools"):    []bool{true, false, true, true},
		wkk.NewRowKey("float32s"): []float32{1.1, 2.2, 3.3, 4.4},
	}
}

// realisticLogRow simulates a typical OTEL log record with resource/scope attributes.
func realisticLogRow() pipeline.Row {
	return pipeline.Row{
		wkk.NewRowKey("timestamp"):                   int64(1702400000000000000),
		wkk.NewRowKey("observed_timestamp"):          int64(1702400000000000001),
		wkk.NewRowKey("severity_number"):             int32(9),
		wkk.NewRowKey("severity_text"):               "INFO",
		wkk.NewRowKey("body"):                        "Connection established to database server at 10.0.0.1:5432",
		wkk.NewRowKey("fingerprint"):                 int64(1234567890123456789),
		wkk.NewRowKey("resource_service_name"):       "payment-service",
		wkk.NewRowKey("resource_service_version"):    "1.2.3",
		wkk.NewRowKey("resource_host_name"):          "prod-payment-node-42",
		wkk.NewRowKey("resource_k8s_namespace"):      "production",
		wkk.NewRowKey("resource_k8s_pod_name"):       "payment-service-6b7c8d9e0f-abc12",
		wkk.NewRowKey("resource_k8s_container_name"): "payment",
		wkk.NewRowKey("resource_deployment_env"):     "production",
		wkk.NewRowKey("scope_name"):                  "github.com/org/payment-service/db",
		wkk.NewRowKey("scope_version"):               "1.0.0",
		wkk.NewRowKey("attr_db_system"):              "postgresql",
		wkk.NewRowKey("attr_db_name"):                "payments",
		wkk.NewRowKey("attr_db_connection_string"):   "host=10.0.0.1 port=5432 dbname=payments",
		wkk.NewRowKey("attr_net_peer_ip"):            "10.0.0.1",
		wkk.NewRowKey("attr_net_peer_port"):          int64(5432),
		wkk.NewRowKey("attr_thread_id"):              int64(12345),
		wkk.NewRowKey("attr_correlation_id"):         "550e8400-e29b-41d4-a716-446655440000",
	}
}

func BenchmarkSpillCodec_RealisticLog(b *testing.B) {
	codec := NewSpillCodec()
	row := realisticLogRow()
	dst := pipeline.Row{}
	buf := bytes.NewBuffer(make([]byte, 0, 4096))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_, err := codec.EncodeRowTo(buf, row)
		if err != nil {
			b.Fatal(err)
		}
		if err := codec.DecodeRowFrom(bytes.NewReader(buf.Bytes()), dst); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSpillCodec_EncodeOnly(b *testing.B) {
	codec := NewSpillCodec()
	row := realisticLogRow()
	buf := bytes.NewBuffer(make([]byte, 0, 4096))

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf.Reset()
		_, err := codec.EncodeRowTo(buf, row)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSpillCodec_DecodeOnly(b *testing.B) {
	codec := NewSpillCodec()
	row := realisticLogRow()
	buf := bytes.NewBuffer(make([]byte, 0, 4096))
	_, _ = codec.EncodeRowTo(buf, row)
	encoded := buf.Bytes()
	dst := pipeline.Row{}

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := codec.DecodeRowFrom(bytes.NewReader(encoded), dst); err != nil {
			b.Fatal(err)
		}
	}
}
