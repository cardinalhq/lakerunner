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

package filereader

import "io"

// NewCookedMetricParquetReader creates a reader for cooked metric parquet files.
// It wraps a ParquetRawReader with CookedMetricTranslatingReader to apply
// metric-specific filtering and transformations.
func NewCookedMetricParquetReader(r io.ReaderAt, size int64, batchSize int) (Reader, error) {
	raw, err := NewParquetRawReader(r, size, batchSize)
	if err != nil {
		return nil, err
	}
	return NewCookedMetricTranslatingReader(raw), nil
}

// NewCookedLogParquetReader creates a reader for cooked log parquet files.
// It wraps a ParquetRawReader with CookedLogTranslatingReader to apply
// log-specific filtering and transformations.
func NewCookedLogParquetReader(r io.ReaderAt, size int64, batchSize int) (Reader, error) {
	raw, err := NewParquetRawReader(r, size, batchSize)
	if err != nil {
		return nil, err
	}
	return NewCookedLogTranslatingReader(raw), nil
}
