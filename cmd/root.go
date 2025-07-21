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

package cmd

import (
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/cardinalhq/lakerunner/cmd/fileconvcmd"
)

const (
	workSleepTime  = 2 * time.Second
	maxWorkRetries = 100
	targetFileSize = 1_100_000
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "lakerunner",
	Short: "Process s3 signal storage",
	Long:  `Read and process signals written to s3 by the CardinalHQ Open Telemetry Collector's S3 exporter.`,
}

func init() {
	rootCmd.AddCommand(fileconvcmd.Cmd)
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
