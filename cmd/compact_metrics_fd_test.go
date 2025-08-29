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

package cmd

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lockmgr"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type stubWorkItem struct {
	orgID       uuid.UUID
	instanceNum int16
	frequencyMs int32
	slotID      int32
	tsRange     pgtype.Range[pgtype.Timestamptz]
}

func (s *stubWorkItem) Complete() error                           { return nil }
func (s *stubWorkItem) Fail() error                               { return nil }
func (s *stubWorkItem) Delete() error                             { return nil }
func (s *stubWorkItem) ID() int64                                 { return 0 }
func (s *stubWorkItem) OrganizationID() uuid.UUID                 { return s.orgID }
func (s *stubWorkItem) InstanceNum() int16                        { return s.instanceNum }
func (s *stubWorkItem) Dateint() int32                            { return 0 }
func (s *stubWorkItem) FrequencyMs() int32                        { return s.frequencyMs }
func (s *stubWorkItem) Signal() lrdb.SignalEnum                   { return lrdb.SignalEnum("") }
func (s *stubWorkItem) Tries() int32                              { return 0 }
func (s *stubWorkItem) Action() lrdb.ActionEnum                   { return lrdb.ActionEnum("") }
func (s *stubWorkItem) TsRange() pgtype.Range[pgtype.Timestamptz] { return s.tsRange }
func (s *stubWorkItem) Priority() int32                           { return 0 }
func (s *stubWorkItem) SlotId() int32                             { return s.slotID }
func (s *stubWorkItem) AsMap() map[string]any                     { return nil }
func (s *stubWorkItem) RunnableAt() time.Time                     { return time.Time{} }

var _ lockmgr.Workable = (*stubWorkItem)(nil)

func countFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	return len(entries)
}

func TestCompactMetricIntervalClosesFiles(t *testing.T) {
	// Use noop counter to avoid telemetry setup
	fileSortedCounter, _ = noop.NewMeterProvider().Meter("test").Int64Counter("file_sorted")

	data, err := os.ReadFile(filepath.Join("..", "testdata", "metrics", "metrics-cooked-0001.parquet"))
	if err != nil {
		t.Fatalf("read parquet: %v", err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}))
	defer server.Close()

	httpClient := server.Client()

	cfg := aws.Config{
		Region:      "us-east-1",
		Credentials: credentials.NewStaticCredentialsProvider("a", "b", ""),
		HTTPClient:  httpClient,
		EndpointResolverWithOptions: aws.EndpointResolverWithOptionsFunc(
			func(service, region string, _ ...interface{}) (aws.Endpoint, error) {
				if service == s3.ServiceID {
					return aws.Endpoint{URL: server.URL, HostnameImmutable: true}, nil
				}
				return aws.Endpoint{}, fmt.Errorf("unknown service %s", service)
			},
		),
	}

	s3c := &awsclient.S3Client{
		Client: s3.NewFromConfig(cfg, func(o *s3.Options) {
			o.UsePathStyle = true
		}),
		Tracer: trace.NewNoopTracerProvider().Tracer("test"),
	}

	start := time.Now().UTC().Truncate(time.Hour)
	end := start.Add(time.Hour)
	tsRange := pgtype.Range[pgtype.Timestamptz]{
		Lower:     pgtype.Timestamptz{Time: start, Valid: true},
		Upper:     pgtype.Timestamptz{Time: end, Valid: true},
		LowerType: pgtype.Inclusive,
		UpperType: pgtype.Exclusive,
		Valid:     true,
	}
	inf := &stubWorkItem{
		orgID:       uuid.New(),
		instanceNum: 1,
		frequencyMs: 60000,
		slotID:      1,
		tsRange:     tsRange,
	}
	profile := storageprofile.StorageProfile{CollectorName: "collector", Bucket: "bucket"}
	rows := []lrdb.MetricSeg{{SegmentID: 1, SortVersion: lrdb.CurrentMetricSortVersion}}

	tmpdir := t.TempDir()
	ll := slog.New(slog.NewTextHandler(io.Discard, nil))

	base := countFDs(t)
	for i := 0; i < 3; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_ = compactMetricInterval(ctx, ll, nil, tmpdir, inf, profile, s3c, rows, 0)
		cancel()
		httpClient.CloseIdleConnections()
		server.CloseClientConnections()
		after := countFDs(t)
		if after != base {
			t.Fatalf("file descriptors leaked: before %d after %d", base, after)
		}
	}
}
