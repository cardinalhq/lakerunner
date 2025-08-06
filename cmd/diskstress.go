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
	for loop := range 1000 {
		log.Printf("Iteration %d...", loop+1)
		var filePaths []string

		for i := range 1000 {
			fname := fmt.Sprintf("stress-test-%d-%d", loop+1, i+1)
			f, err := os.Create(fname)
			if err != nil {
				return fmt.Errorf("failed to create temp file #%d: %w", i, err)
			}

			n, err := f.WriteString("hello world")
			if err != nil {
				if err2 := f.Close(); err2 != nil {
					log.Printf("warning: failed to close file %s: %v", f.Name(), err2)
				}
				return fmt.Errorf("failed to write to file %s: %w", f.Name(), err)
			}
			if n != len("hello world") {
				if err2 := f.Close(); err2 != nil {
					log.Printf("warning: failed to close file %s: %v", f.Name(), err2)
				}
				return fmt.Errorf("short write to file %s: wrote %d bytes, expected %d", f.Name(), n, len("hello world"))
			}

			filePaths = append(filePaths, f.Name())
			if err2 := f.Close(); err2 != nil {
				log.Printf("warning: failed to close file %s: %v", f.Name(), err2)
			}
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
			if err := os.Remove(path); err != nil {
				log.Printf("warning: failed to remove file %s: %v", path, err)
			}
		}
	}
	return nil
}
