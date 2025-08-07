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

func CleanTempDir() {
	slog.Info("Cleaning temp dir", "path", os.TempDir())
	temp := os.TempDir()
	entries, err := os.ReadDir(temp)
	if err != nil {
		slog.Info("Failed to read temp dir (ignoring)", slog.String("path", temp), slog.Any("error", err))
		return
	}

	for _, entry := range entries {
		path := filepath.Join(temp, entry.Name())
		_ = os.RemoveAll(path)
	}
}
