package fileconv

type Reader interface {
	// Returns the next row of data.  It will return (nil, true, nil) when there are no more rows.
	// row and done are only valid if err is nil.
	GetRow() (row map[string]any, done bool, err error)
	Close() error
}

type Writer interface {
	// WriteRow writes a row of data.  It returns an error if the write fails.
	WriteRow(row map[string]any) error
	Close() error
}
