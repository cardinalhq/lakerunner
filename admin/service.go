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

package admin

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"google.golang.org/grpc"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/internal/adminconfig"
)

type Service struct {
	adminproto.UnimplementedAdminServiceServer
	server   *grpc.Server
	listener net.Listener
	addr     string
	serverID string
}

func NewService(addr string) (*Service, error) {
	if addr == "" {
		addr = ":9091"
	}

	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
	}

	// Setup admin configuration
	configProvider, err := adminconfig.SetupAdminConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to setup admin config: %w", err)
	}

	// Create auth interceptor
	authInterceptor := newAuthInterceptor(configProvider)

	// Create GRPC server with auth interceptor
	server := grpc.NewServer(
		grpc.UnaryInterceptor(authInterceptor.unaryInterceptor),
	)

	hostname, _ := os.Hostname()
	serverID := fmt.Sprintf("%s-%d", hostname, time.Now().Unix())

	service := &Service{
		server:   server,
		listener: listener,
		addr:     addr,
		serverID: serverID,
	}

	adminproto.RegisterAdminServiceServer(server, service)

	return service, nil
}

func (s *Service) Run(ctx context.Context) error {
	slog.Info("Starting admin service", slog.String("addr", s.listener.Addr().String()))

	errChan := make(chan error, 1)

	go func() {
		if err := s.server.Serve(s.listener); err != nil {
			errChan <- fmt.Errorf("GRPC server failed: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		slog.Info("Shutting down admin service")
		s.server.GracefulStop()
		return nil
	case err := <-errChan:
		return err
	}
}

func (s *Service) Ping(ctx context.Context, req *adminproto.PingRequest) (*adminproto.PingResponse, error) {
	slog.Debug("Received ping request", slog.String("message", req.Message))

	response := fmt.Sprintf("pong: %s", req.Message)
	if req.Message == "" {
		response = "pong"
	}

	return &adminproto.PingResponse{
		Message:   response,
		Timestamp: time.Now().Unix(),
		ServerId:  s.serverID,
	}, nil
}

func (s *Service) WorkQueueStatus(ctx context.Context, req *adminproto.WorkQueueStatusRequest) (*adminproto.WorkQueueStatusResponse, error) {
	slog.Debug("Received workqueue status request")

	store, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to lrdb: %w", err)
	}

	results, err := store.WorkQueueSummary(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query work queue summary: %w", err)
	}

	items := make([]*adminproto.WorkQueueItem, len(results))
	for i, result := range results {
		items[i] = &adminproto.WorkQueueItem{
			Count:  result.Count,
			Signal: string(result.Signal),
			Action: string(result.Action),
		}
	}

	return &adminproto.WorkQueueStatusResponse{
		Items: items,
	}, nil
}

func (s *Service) InQueueStatus(ctx context.Context, req *adminproto.InQueueStatusRequest) (*adminproto.InQueueStatusResponse, error) {
	slog.Debug("Received inqueue status request")

	store, err := dbopen.LRDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to lrdb: %w", err)
	}

	results, err := store.InqueueSummary(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query inqueue summary: %w", err)
	}

	items := make([]*adminproto.InQueueItem, len(results))
	for i, result := range results {
		items[i] = &adminproto.InQueueItem{
			Count:         result.Count,
			TelemetryType: result.TelemetryType,
		}
	}

	return &adminproto.InQueueStatusResponse{
		Items: items,
	}, nil
}
