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

package debugging

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
)

const DefaultPprofPort = 6060

func RunPprof(ctx context.Context) {
	port := getPprofPort()
	if port <= 0 {
		return
	}

	addr := fmt.Sprintf(":%d", port)
	server := &http.Server{
		Addr: addr,
	}

	go func() {
		slog.Info("Starting pprof server", slog.String("address", addr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Pprof server error", slog.Any("error", err))
		}
	}()

	go func() {
		<-ctx.Done()
		slog.Info("Shutting down pprof server")
		if err := server.Shutdown(context.Background()); err != nil {
			slog.Error("Error shutting down pprof server", slog.Any("error", err))
		}
	}()
}

func getPprofPort() int {
	envPort := os.Getenv("PPROF_PORT")
	if envPort == "" {
		return DefaultPprofPort
	}

	if envPort == "0" || envPort == "false" || envPort == "off" {
		return 0
	}

	port, err := strconv.Atoi(envPort)
	if err != nil {
		slog.Warn("Invalid PPROF_PORT value, using default", slog.String("value", envPort), slog.Int("default", DefaultPprofPort))
		return DefaultPprofPort
	}

	return port
}
