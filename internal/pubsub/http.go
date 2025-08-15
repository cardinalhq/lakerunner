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
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

type HTTPService struct {
	sp       storageprofile.StorageProfileProvider
	workChan chan []byte
	tracer   trace.Tracer
	mdb      InqueueInserter
}

func NewHTTPService() (*HTTPService, error) {
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

	return &HTTPService{
		sp:       sp,
		mdb:      mdb,
		workChan: make(chan []byte, 100), // Buffered channel to handle incoming requests
		tracer:   otel.Tracer("github.com/cardinalhq/lakerunner/internal/pubsub"),
	}, nil
}

func (ps *HTTPService) Run(doneCtx context.Context) error {
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

func (ps *HTTPService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

func (ps *HTTPService) Process(ctx context.Context) {
	slog.Info("Starting worker to process incoming messages")

	for msg := range ps.workChan {
		func() { // Use a closure to ensure the span is closed
			var span trace.Span
			ctx, span = ps.tracer.Start(ctx, "HTTPService.Process")
			defer span.End()

			if err := handleMessage(ctx, msg, ps.sp, ps.mdb); err != nil {
				slog.Error("Failed to handle message", slog.Any("error", err))
			}
		}()
	}
}
