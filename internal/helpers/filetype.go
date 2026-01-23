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
	"io"
	"os"
	"path"
	"regexp"
	"strings"
)

var (
	nonLetter = regexp.MustCompile("[^A-Za-z]+")
	// Match a domain at the start of the string, immediately followed by "-" or "_".
	// Capture only the domain part.
	domainWithSepRe = regexp.MustCompile(`^((?:[A-Za-z0-9-]+\.)+[A-Za-z]+)[-_]`)
	// Match a string that is *only* a domain (no separator or suffix).
	domainOnlyRe = regexp.MustCompile(`^((?:[A-Za-z0-9-]+\.)+[A-Za-z]+)$`)
)

// GetFileType extracts a normalized file type from a file path.
// It removes the extension and non-letter characters from the filename.
func GetFileType(p string) string {
	fileName := path.Base(p)

	// find last "."; if none, use whole filename
	if idx := strings.LastIndex(fileName, "."); idx != -1 {
		fileName = fileName[:idx]
	}

	// strip out anything that isn't A–Z or a–z
	return nonLetter.ReplaceAllString(fileName, "")
}

// ExtractCustomerDomain attempts to extract a customer domain from a resource file name.
// It expects patterns like: domain.com-rest-of-stuff or foo-bar.example.com_rest_of_stuff
// The domain is assumed to be at the start of the string.
func ExtractCustomerDomain(resourceFile string) string {
	if resourceFile == "" {
		return ""
	}

	// Special case for just a dot
	if resourceFile == "." {
		return "."
	}

	// 1) Domain followed by "-" or "_" (take the last such match, conceptually)
	if matches := domainWithSepRe.FindAllStringSubmatchIndex(resourceFile, -1); len(matches) > 0 {
		last := matches[len(matches)-1]
		// last[2], last[3] are the start/end of the first capturing group
		start, end := last[2], last[3]
		return resourceFile[start:end]
	}

	// 2) Entire string is just a domain (no separator)
	if m := domainOnlyRe.FindStringSubmatch(resourceFile); m != nil {
		return m[1]
	}

	// 3) No recognizable domain pattern
	return ""
}

// IsGzipFile checks if a file is actually gzip compressed by examining magic bytes.
// GCP Cloud Storage may transparently decompress .gz files during download,
// resulting in files that have a .gz extension but contain uncompressed data.
// This function checks the file content, not the extension.
func IsGzipFile(filename string) (bool, error) {
	f, err := os.Open(filename)
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()
	return IsGzipReader(f)
}

// IsGzipReader checks if a reader contains gzip compressed data.
// It reads the first 2 bytes to check for the gzip magic number (0x1f 0x8b).
// The reader should be at position 0; after calling, the reader position
// will have advanced by up to 2 bytes.
func IsGzipReader(r io.Reader) (bool, error) {
	magic := make([]byte, 2)
	n, err := io.ReadFull(r, magic)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil // File too short to be gzip
		}
		return false, err
	}
	if n < 2 {
		return false, nil
	}
	// Gzip magic number: 0x1f 0x8b
	return magic[0] == 0x1f && magic[1] == 0x8b, nil
}
