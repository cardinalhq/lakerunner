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

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/cmd/dbopen"
	"github.com/cardinalhq/lakerunner/configdb"
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

	// Return empty response for backward compatibility
	items := make([]*adminproto.InQueueItem, 0)

	return &adminproto.InQueueStatusResponse{
		Items: items,
	}, nil
}

func (s *Service) ListOrganizations(ctx context.Context, _ *adminproto.ListOrganizationsRequest) (*adminproto.ListOrganizationsResponse, error) {
	slog.Debug("Received list organizations request")

	store, err := dbopen.ConfigDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to configdb: %w", err)
	}

	orgs, err := store.ListOrganizations(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list organizations: %w", err)
	}

	respOrgs := make([]*adminproto.Organization, len(orgs))
	for i, o := range orgs {
		respOrgs[i] = &adminproto.Organization{Id: o.ID.String(), Name: o.Name, Enabled: o.Enabled}
	}

	return &adminproto.ListOrganizationsResponse{Organizations: respOrgs}, nil
}

func (s *Service) CreateOrganization(ctx context.Context, req *adminproto.CreateOrganizationRequest) (*adminproto.CreateOrganizationResponse, error) {
	slog.Debug("Received create organization request", slog.String("name", req.Name))

	store, err := dbopen.ConfigDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to configdb: %w", err)
	}

	org, err := store.UpsertOrganization(ctx, configdb.UpsertOrganizationParams{ID: uuid.New(), Name: req.Name, Enabled: req.Enabled})
	if err != nil {
		return nil, fmt.Errorf("failed to create organization: %w", err)
	}

	return &adminproto.CreateOrganizationResponse{Organization: &adminproto.Organization{Id: org.ID.String(), Name: org.Name, Enabled: org.Enabled}}, nil
}

func (s *Service) UpdateOrganization(ctx context.Context, req *adminproto.UpdateOrganizationRequest) (*adminproto.UpdateOrganizationResponse, error) {
	slog.Debug("Received update organization request", slog.String("id", req.Id))

	store, err := dbopen.ConfigDBStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to configdb: %w", err)
	}

	orgID, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, fmt.Errorf("invalid organization ID: %w", err)
	}

	org, err := store.GetOrganization(ctx, orgID)
	if err != nil {
		return nil, fmt.Errorf("failed to get organization: %w", err)
	}

	if req.Name != nil {
		org.Name = req.Name.Value
	}
	if req.Enabled != nil {
		org.Enabled = req.Enabled.Value
	}

	updated, err := store.UpsertOrganization(ctx, configdb.UpsertOrganizationParams{ID: orgID, Name: org.Name, Enabled: org.Enabled})
	if err != nil {
		return nil, fmt.Errorf("failed to update organization: %w", err)
	}

	return &adminproto.UpdateOrganizationResponse{Organization: &adminproto.Organization{Id: updated.ID.String(), Name: updated.Name, Enabled: updated.Enabled}}, nil
}
