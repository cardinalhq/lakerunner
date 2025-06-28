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

func init() {
	time.Local = time.UTC // Ensure all time operations are in UTC
}

func simpleLogger(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
}

func init() {
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
	_, err := memlimit.SetGoMemLimitWithOpts(memlimit.WithRatio(0.8), memlimit.WithLogger(slog.Default()))
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
	_ = CleanTempDir()

	cmd.Execute()
}

func CleanTempDir() error {
	temp := os.TempDir()

	entries, err := os.ReadDir(temp)
	if err != nil {
		return fmt.Errorf("reading temp dir: %w", err)
	}

	for _, entry := range entries {
		path := filepath.Join(temp, entry.Name())
		_ = os.RemoveAll(path)
	}

	return nil
}
