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

package ingestlogs

import (
	"errors"
	"fmt"
	"log/slog"
	"path"
	"regexp"
	"strings"

	"github.com/cardinalhq/lakerunner/fileconv/rawparquet"
	"github.com/cardinalhq/lakerunner/fileconv/translate"
	"github.com/cardinalhq/lakerunner/internal/buffet"
)

func ConvertRawParquet(sourcefile, tmpdir, bucket, objectID string) ([]string, error) {
	r, err := rawparquet.NewRawParquetReader(sourcefile, translate.NewMapper(), nil)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	nodes, err := r.Nodes()
	if err != nil {
		return nil, fmt.Errorf("failed to get nodes: %w", err)
	}
	slog.Info("nodes", slog.Any("nodes", nodes))

	// add our new nodes to the list of nodes we will write out
	nmb := buffet.NewNodeMapBuilder()
	if err := nmb.AddNodes(nodes); err != nil {
		return nil, fmt.Errorf("failed to add nodes: %w", err)
	}
	if err := nmb.Add(map[string]any{
		"resource.bucket.name": "bucket",
		"resource.file.name":   "object",
		"resource.file.type":   "filetype",
		"resource.file":        "file",
	}); err != nil {
		return nil, fmt.Errorf("failed to add resource nodes: %w", err)
	}

	w, err := buffet.NewWriter("fileconv", tmpdir, nmb.Build(), 0, 0)
	if err != nil {
		return nil, fmt.Errorf("Failed to create writer: %w", err)
	}
	defer func() {
		_, err := w.Close()
		if errors.Is(err, buffet.ErrAlreadyClosed) {
			if err != nil {
				slog.Error("Failed to close writer", slog.Any("error", err))
			}
		}
	}()

	baseitems := map[string]string{
		"resource.bucket.name": bucket,
		"resource.file.name":   "./" + objectID,
		"resource.file.type":   GetFileType(objectID),
		"resource.file":        getResourceFile(objectID),
	}

	for {
		row, done, err := r.GetRow()
		if err != nil {
			return nil, err
		}
		if done {
			break
		}
		for k, v := range baseitems {
			row[k] = v
		}
		if err := w.Write(row); err != nil {
			return nil, fmt.Errorf("failed to write row: %w", err)
		}
	}

	result, err := w.Close()
	if err != nil {
		return nil, fmt.Errorf("failed to close writer: %w", err)
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("no records written to file")
	}

	var fnames []string
	for _, res := range result {
		fnames = append(fnames, res.FileName)
	}
	return fnames, nil
}

func getResourceFile(objectID string) string {
	// find the /Support path element, and return the next element
	parts := strings.Split(objectID, "/")
	for i, part := range parts {
		if part == "Support" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

var nonLetter = regexp.MustCompile(`[^a-zA-Z]`)

// getFileType extracts the “base” of the filename (everything before the last dot),
// then strips out any non‑letter characters.
func GetFileType(p string) string {
	fileName := path.Base(p)

	// find last “.”; if none, use whole filename
	if idx := strings.LastIndex(fileName, "."); idx != -1 {
		fileName = fileName[:idx]
	}

	// strip out anything that isn’t A–Z or a–z
	return nonLetter.ReplaceAllString(fileName, "")
}
