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
