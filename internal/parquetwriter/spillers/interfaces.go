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

package spillers


// SpillFile represents a temporary file containing spilled sorted data.
type SpillFile struct {
	// Path is the filesystem path to the spill file
	Path string
	
	// RowCount is the number of rows written to this spill file
	RowCount int64
	
	// Metadata can contain spiller-specific information
	Metadata any
}

// SpillReader provides an interface for reading rows back from a spill file.
type SpillReader interface {
	// Next reads the next row from the spill file.
	// Returns io.EOF when no more rows are available.
	Next() (map[string]any, error)
	
	// Close closes the spill reader and cleans up resources.
	Close() error
}

// Spiller handles writing sorted data to temporary files and reading it back.
type Spiller interface {
	// WriteSpillFile writes a sorted slice of rows to a temporary file.
	// Returns a SpillFile descriptor that can be used to read the data back.
	WriteSpillFile(tmpDir string, rows []map[string]any, keyFunc func(map[string]any) any) (*SpillFile, error)
	
	// OpenSpillFile opens a spill file for reading.
	// The returned SpillReader will read rows in the same order they were written.
	OpenSpillFile(spillFile *SpillFile, keyFunc func(map[string]any) any) (SpillReader, error)
	
	// CleanupSpillFile removes the spill file from disk.
	CleanupSpillFile(spillFile *SpillFile) error
}