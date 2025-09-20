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

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/configdb"
	"github.com/cardinalhq/lakerunner/internal/constants"
	"github.com/cardinalhq/lakerunner/internal/fly"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
	"github.com/cardinalhq/lakerunner/lrdb"
)

type HTTPService struct {
	sp           storageprofile.StorageProfileProvider
	workChan     chan []byte
	tracer       trace.Tracer
	kafkaHandler *KafkaHandler
	deduplicator *Deduplicator
}

func NewHTTPService(ctx context.Context, cfg *config.Config, kafkaFactory *fly.Factory) (*HTTPService, error) {
	cdb, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		slog.Error("Failed to connect to configdb", slog.Any("error", err))
		return nil, fmt.Errorf("failed to connect to configdb: %w", err)
	}
	sp := storageprofile.NewStorageProfileProvider(cdb)

	lrdbStore, err := lrdb.LRDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to lrdb for deduplication: %w", err)
	}
	deduplicator := NewDeduplicator(lrdbStore)

	kafkaHandler, err := NewKafkaHandler(ctx, cfg, kafkaFactory, "http", sp, deduplicator)
	if err != nil {
		return nil, fmt.Errorf("failed to create Kafka handler: %w", err)
	}

	service := &HTTPService{
		sp:           sp,
		workChan:     make(chan []byte, 100), // Buffered channel to handle incoming requests
		tracer:       otel.Tracer("github.com/cardinalhq/lakerunner/internal/pubsub"),
		kafkaHandler: kafkaHandler,
		deduplicator: deduplicator,
	}

	slog.Info("HTTP pubsub service initialized with Kafka support")
	return service, nil
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

	// Close Kafka handler if it exists
	if ps.kafkaHandler != nil {
		if err := ps.kafkaHandler.Close(); err != nil {
			slog.Error("Failed to close Kafka handler", slog.Any("error", err))
		}
	}

	return nil
}

func (ps *HTTPService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, constants.HTTPBodyLimitBytes)
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

			if err := ps.kafkaHandler.HandleMessage(ctx, msg); err != nil {
				slog.Error("Failed to handle message with Kafka", slog.Any("error", err))
			}
		}()
	}
}
