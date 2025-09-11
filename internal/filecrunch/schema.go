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

package filecrunch

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

// FileHandle represents a handle to a parquet file for schema inspection
type FileHandle struct {
	Schema *parquet.Schema
	file   *os.File
}

// LoadSchemaForFile loads the schema from a parquet file
func LoadSchemaForFile(filename string) (*FileHandle, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", filename, err)
	}

	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file %s: %w", filename, err)
	}

	pf, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to open parquet file: %w", err)
	}

	schema := pf.Schema()

	return &FileHandle{
		Schema: schema,
		file:   file,
	}, nil
}

// Close closes the file handle
func (fh *FileHandle) Close() error {
	if fh.file != nil {
		return fh.file.Close()
	}
	return nil
}
