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
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/parquet-go/parquet-go/format"
)

type FileHandle struct {
	File        *os.File
	Size        int64
	Schema      *parquet.Schema
	ParquetFile *parquet.File
	Nodes       map[string]parquet.Node
}

func (fh *FileHandle) Close() error {
	if err := fh.File.Close(); err != nil {
		return err
	}
	return nil
}

func LoadSchemaForFile(filename string) (*FileHandle, error) {
	fh, err := openfile(filename)
	if err != nil {
		return nil, err
	}

	if err := loadSchema(fh); err != nil {
		_ = fh.File.Close()
		return nil, err
	}

	return fh, nil
}

func openfile(file string) (*FileHandle, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &FileHandle{
		File: f,
		Size: stat.Size(),
	}, nil
}

func loadSchema(fh *FileHandle) error {
	f, err := parquet.OpenFile(fh.File, fh.Size)
	if err != nil {
		return err
	}
	fh.ParquetFile = f

	md := f.Metadata()
	fh.Nodes = map[string]parquet.Node{}
	for _, schema := range md.Schema {
		if schema.Type == nil {
			continue
		}
		typ := schema.Type.String()
		logicalType := ""
		if schema.LogicalType != nil {
			logicalType = schema.LogicalType.String()
		}

		stype, err := schemaTypeToNode(typ, logicalType)
		if err != nil {
			return err
		}
		if currentNode, ok := fh.Nodes[schema.Name]; ok {
			if !parquet.EqualNodes(currentNode, stype) {
				return fmt.Errorf("schema mismatch: %s (%s vs %s)", schema.Name, currentNode.String(), stype.String())
			}
		} else {
			fh.Nodes[schema.Name] = stype
		}
	}

	fh.Schema = parquet.NewSchema(fh.File.Name(), parquet.Group(fh.Nodes))

	return nil
}

func wrapP(n parquet.Node) parquet.Node {
	n = parquet.Compressed(n, enc)
	if n.Leaf() {
		n = parquet.Encoded(n, parquet.LookupEncoding(format.Encoding(format.RLEDictionary)))
	}
	n = parquet.Optional(n)
	return n
}

var (
	enc         = &zstd.Codec{Level: zstd.SpeedBetterCompression}
	NodeTypeMap = map[string]parquet.Node{
		"INT8":       wrapP(parquet.Int(8)),
		"INT16":      wrapP(parquet.Int(16)),
		"INT32":      wrapP(parquet.Int(32)),
		"INT64":      wrapP(parquet.Int(64)),
		"DOUBLE":     wrapP(parquet.Leaf(parquet.DoubleType)),
		"BOOLEAN":    wrapP(parquet.Leaf(parquet.BooleanType)),
		"BYTE_ARRAY": wrapP(parquet.Leaf(parquet.ByteArrayType)),
	}
	logicalNodes = map[string]parquet.Node{
		"STRING": wrapP(parquet.String()),
	}
)

func schemaTypeToNode(typ, logical string) (parquet.Node, error) {
	if node, ok := logicalNodes[logical]; ok {
		return node, nil
	}
	if node, ok := NodeTypeMap[typ]; ok {
		return node, nil
	}
	return nil, fmt.Errorf("unsupported type: %s, logical %s", typ, logical)
}
