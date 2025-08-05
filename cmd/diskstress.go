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
	"fmt"
	"log"
	"os"

	"github.com/spf13/cobra"
)

func init() {
	cmd := &cobra.Command{
		Use:   "diskstress",
		Short: "Run disk stress tests",
		RunE: func(_ *cobra.Command, _ []string) error {
			return stressTestTempFiles()
		},
	}

	rootCmd.AddCommand(cmd)
}

func stressTestTempFiles() error {
	fmt.Printf("Running disk stress test, tmpdir is %q\n", os.TempDir())

	for loop := range 1000 {
		log.Printf("Iteration %d...", loop+1)
		var filePaths []string

		for i := range 1000 {
			f, err := os.CreateTemp("", "stress-test-*")
			if err != nil {
				return fmt.Errorf("failed to create temp file #%d: %w", i, err)
			}

			_, err = f.WriteString("hello world")
			if err != nil {
				f.Close()
				return fmt.Errorf("failed to write to file %s: %w", f.Name(), err)
			}

			filePaths = append(filePaths, f.Name())
			f.Close()
		}

		for i, path := range filePaths {
			if i == 0 {
				fmt.Printf("  loop sample path: %s\n", path)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read back file #%d (%s): %w", i, path, err)
			}
			if string(data) != "hello world" {
				return fmt.Errorf("unexpected content in file %s: %q", path, data)
			}
		}

		for _, path := range filePaths {
			_ = os.Remove(path)
		}
	}
	return nil
}
