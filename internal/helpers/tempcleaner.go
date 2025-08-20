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

package helpers

import (
	"log/slog"
	"os"
	"path/filepath"
)

// SetupTempDir creates a lakerunner-specific temp directory, sets TMPDIR, and cleans it
func SetupTempDir() {
	tmp := os.TempDir()
	tmp = filepath.Join(tmp, "lakerunner")
	if err := os.MkdirAll(tmp, 0755); err != nil {
		slog.Error("Failed to create temp dir path (ignoring)", slog.String("path", tmp), slog.Any("error", err))
	} else {
		slog.Debug("Created temp dir path", slog.String("path", tmp))
	}
	if err := os.Setenv("TMPDIR", tmp); err != nil {
		slog.Error("Failed to set TMPDIR environment variable", slog.String("path", tmp), slog.Any("error", err))
	} else {
		slog.Debug("Set TMPDIR environment variable", slog.String("path", tmp))
	}

	slog.Debug("Using temp dir", "path", os.TempDir())

	// Clean the temp directory
	slog.Debug("Cleaning temp dir", "path", os.TempDir())
	temp := os.TempDir()
	entries, err := os.ReadDir(temp)
	if err != nil {
		slog.Debug("Failed to read temp dir (ignoring)", slog.String("path", temp), slog.Any("error", err))
		return
	}

	for _, entry := range entries {
		path := filepath.Join(temp, entry.Name())
		_ = os.RemoveAll(path)
	}
}
