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

package healthcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"
)

type Status int32

const (
	StatusStarting Status = iota
	StatusHealthy
	StatusUnhealthy
)

func (s Status) String() string {
	switch s {
	case StatusStarting:
		return "starting"
	case StatusHealthy:
		return "healthy"
	case StatusUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

type Response struct {
	Healthy bool `json:"healthy"`
}

type Server struct {
	port   int
	status atomic.Int32
	server *http.Server
}

type Config struct {
	Port int
}

func GetConfigFromEnv() Config {
	port := 8090
	if portStr := os.Getenv("HEALTH_CHECK_PORT"); portStr != "" {
		if p, err := strconv.Atoi(portStr); err == nil && p > 0 && p < 65536 {
			port = p
		}
	}

	return Config{
		Port: port,
	}
}

func NewServer(config Config) *Server {
	if config.Port == 0 {
		config.Port = 8090
	}

	return &Server{
		port: config.Port,
	}
}

func (s *Server) SetStatus(status Status) {
	s.status.Store(int32(status))
	slog.Debug("Health check status updated", slog.String("status", status.String()))
}

func (s *Server) GetStatus() Status {
	return Status(s.status.Load())
}

func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.healthzHandler)
	mux.HandleFunc("/readyz", s.readyzHandler)
	mux.HandleFunc("/livez", s.livezHandler)

	s.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: mux,
	}

	s.SetStatus(StatusStarting)
	slog.Info("Starting health check server", slog.Int("port", s.port))

	go func() {
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Health check server error", slog.Any("error", err))
		}
	}()

	<-ctx.Done()
	return s.Stop()
}

func (s *Server) Stop() error {
	if s.server == nil {
		return nil
	}

	slog.Info("Stopping health check server")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	return s.server.Shutdown(ctx)
}

func (s *Server) healthzHandler(w http.ResponseWriter, r *http.Request) {
	status := s.GetStatus()
	isHealthy := status == StatusHealthy
	response := Response{Healthy: isHealthy}

	w.Header().Set("Content-Type", "application/json")

	if isHealthy {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode health check response", slog.Any("error", err))
	}
}

func (s *Server) readyzHandler(w http.ResponseWriter, r *http.Request) {
	status := s.GetStatus()
	isHealthy := status == StatusHealthy
	response := Response{Healthy: isHealthy}

	w.Header().Set("Content-Type", "application/json")

	if isHealthy {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode health check response", slog.Any("error", err))
	}
}

func (s *Server) livezHandler(w http.ResponseWriter, r *http.Request) {
	status := s.GetStatus()
	isAlive := status != StatusUnhealthy
	response := Response{Healthy: isAlive}

	w.Header().Set("Content-Type", "application/json")

	if isAlive {
		w.WriteHeader(http.StatusOK)
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		slog.Error("Failed to encode health check response", slog.Any("error", err))
	}
}
