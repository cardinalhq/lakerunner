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
	"os"
	"runtime/debug"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	gomaxecs "github.com/rdforte/gomaxecs/maxprocs"
	"go.uber.org/automaxprocs/maxprocs"

	"github.com/cardinalhq/lakerunner/cmd"
)

var debugEnabled = os.Getenv("DEBUG") != "" || os.Getenv("LAKERUNNER_DEBUG") != ""

func simpleLogger(msg string, args ...any) {
	// Only log if debug logging is enabled
	if debugEnabled {
		fmt.Fprintf(os.Stderr, msg+"\n", args...)
	}
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
		if debugEnabled {
			fmt.Fprintf(os.Stderr, "GOGC is not set, setting it to 50%%\n")
		}
		debug.SetGCPercent(50)
		_ = os.Setenv("GOGC", "50")
	}
}

func main() {
	cmd.Execute()
}
