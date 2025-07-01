// Copyright 2025 CardinalHQ, Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pubsub

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/cmd/storageprofile"
	"github.com/cardinalhq/lakerunner/pkg/lrdb"
)

type pubsubCmd struct {
	sp       storageprofile.StorageProfileProvider
	workChan chan []byte
	tracer   trace.Tracer
	mdb      InqueueInserter
}

type InqueueInserter interface {
	PutInqueueWork(ctx context.Context, arg lrdb.PutInqueueWorkParams) error
}

func New() (*pubsubCmd, error) {
	sp, err := storageprofile.SetupStorageProfiles()
	if err != nil {
		slog.Error("Failed to setup storage profiles", slog.Any("error", err))
		return nil, fmt.Errorf("failed to setup storage profiles: %w", err)
	}

	mdb, err := dbopen.LRDBStore(context.Background())
	if err != nil {
		slog.Error("Failed to connect to lr database", slog.Any("error", err))
		return nil, fmt.Errorf("failed to connect to lr database: %w", err)
	}

	return &pubsubCmd{
		sp:       sp,
		mdb:      mdb,
		workChan: make(chan []byte, 100), // Buffered channel to handle incoming requests
		tracer:   otel.Tracer("github.com/cardinalhq/lakerunner/cmd/pubsub"),
	}, nil
}

func (ps *pubsubCmd) Run(doneCtx context.Context) error {
	slog.Info("Starting pubsub service")

	srv := &http.Server{
		Addr:    ":8080",
		Handler: ps,
	}

	go ps.Process(context.Background())

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Failed to start HTTP server", slog.Any("error", err))
		}
	}()

	<-doneCtx.Done()

	slog.Info("Shutting down pubsub service")
	if err := srv.Shutdown(context.Background()); err != nil {
		slog.Error("Failed to shutdown HTTP server", slog.Any("error", err))
		return fmt.Errorf("failed to shutdown HTTP server: %w", err)
	}

	close(ps.workChan)

	return nil
}

func (ps *pubsubCmd) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1024*1024)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesError *http.MaxBytesError
		if errors.As(err, &maxBytesError) {
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "Error reading request body", http.StatusInternalServerError)
		return
	}

	if len(body) > 0 {
		ps.workChan <- body
	}
	w.WriteHeader(http.StatusOK)
}

func (ps *pubsubCmd) Process(ctx context.Context) {
	slog.Info("Starting worker to process incoming messages")

	for msg := range ps.workChan {
		func() { // Use a closure to ensure the span is closed
			var span trace.Span
			ctx, span = ps.tracer.Start(ctx, "pubsubCmd.Process")
			defer span.End()

			if err := handleMessage(ctx, msg, ps.sp, ps.mdb); err != nil {
				slog.Error("Failed to handle message", slog.Any("error", err))
			}
		}()
	}
}

func handleMessage(ctx context.Context, msg []byte, sp storageprofile.StorageProfileProvider, mdb InqueueInserter) error {
	if len(msg) == 0 {
		return fmt.Errorf("empty message received")
	}

	items, err := parseS3LikeEvents(msg)
	if err != nil {
		return fmt.Errorf("failed to parse S3-like events: %w", err)
	}

	for _, item := range items {
		profile, err := sp.GetByCollectorName(ctx, item.OrganizationID, item.CollectorName)
		if err != nil {
			slog.Error("Failed to get storage profile", slog.Any("error", err), slog.Any("organization_id", item.OrganizationID), slog.String("collector_name", item.CollectorName))
			continue
		}
		item.InstanceNum = profile.InstanceNum
		slog.Info("Processing item", slog.String("bucket", profile.Bucket), slog.String("object_id", item.ObjectID), slog.String("telemetry_type", item.TelemetryType))

		err = mdb.PutInqueueWork(ctx, lrdb.PutInqueueWorkParams{
			OrganizationID: item.OrganizationID,
			CollectorName:  item.CollectorName,
			InstanceNum:    item.InstanceNum,
			Bucket:         profile.Bucket,
			ObjectID:       item.ObjectID,
			TelemetryType:  item.TelemetryType,
			Priority:       0,
		})
		if err != nil {
			return fmt.Errorf("failed to insert inqueue work: %w", err)
		}
	}

	return nil
}
