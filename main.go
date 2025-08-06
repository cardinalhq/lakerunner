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

package main

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime/debug"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	gomaxecs "github.com/rdforte/gomaxecs/maxprocs"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/cardinalhq/lakerunner/cmd"
)

func simpleLogger(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
}

func init() {
	time.Local = time.UTC // Ensure all time operations are in UTC

	if gomaxecs.IsECS() {
		_, err := gomaxecs.Set(gomaxecs.WithLogger(simpleLogger))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to set maxprocs package github.com/rdforte/gomaxecs/maxprocs: %v\n", err)
		}
	} else {
		_, err := maxprocs.Set(maxprocs.Logger(simpleLogger))
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to set maxprocs using package go.uber.org/automaxprocs/maxprocs: %v\n", err)
		}
	}
	_, err := memlimit.SetGoMemLimitWithOpts(
		memlimit.WithRatio(0.8),
		memlimit.WithLogger(slog.Default()),
		memlimit.WithProvider(
			memlimit.ApplyFallback(
				memlimit.FromCgroup,
				memlimit.FromSystem,
			),
		),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to set memory limit using package github.com/KimMachineGun/automemlimit/memlimit: %v\n", err)
	}

	if os.Getenv("GOGC") == "" {
		fmt.Fprintf(os.Stderr, "GOGC is not set, setting it to 50%%\n")
		debug.SetGCPercent(50)
		os.Setenv("GOGC", "50")
	}
}

func main() {
	tmp := os.TempDir()
	tmp = filepath.Join(tmp, "lakerunner")
	if err := os.MkdirAll(tmp, 0755); err != nil {
		slog.Error("Failed to create temp dir path (ignoring)", slog.String("path", tmp), slog.Any("error", err))
	} else {
		slog.Info("Created temp dir path", slog.String("path", tmp))
	}
	if err := os.Setenv("TMPDIR", tmp); err != nil {
		slog.Error("Failed to set TMPDIR environment variable", slog.String("path", tmp), slog.Any("error", err))
	} else {
		slog.Info("Set TMPDIR environment variable", slog.String("path", tmp))
	}

	slog.Info("Using temp dir", "path", os.TempDir())

	cmd.Execute()
}
