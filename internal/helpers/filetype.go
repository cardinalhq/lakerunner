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
	"path"
	"regexp"
	"strings"
)

var nonLetter = regexp.MustCompile("[^A-Za-z]+")

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
// Returns the domain part by finding the first separator (hyphen or underscore) after the
// last dot (TLD). This handles hyphenated domains correctly.
// If no separator found after the TLD, returns the whole string.
func ExtractCustomerDomain(resourceFile string) string {
	if resourceFile == "" {
		return ""
	}

	// Find the last dot (should be before the TLD)
	lastDotIdx := strings.LastIndex(resourceFile, ".")
	if lastDotIdx == -1 {
		// No dot found - not a domain
		return ""
	}

	// Look for the first separator (hyphen or underscore) after the last dot
	afterDot := resourceFile[lastDotIdx+1:]
	sepIdx := strings.IndexAny(afterDot, "-_")
	if sepIdx == -1 {
		// No separator after the last dot, return the whole string
		return resourceFile
	}

	// Return everything up to (but not including) the separator
	return resourceFile[:lastDotIdx+1+sepIdx]
}
