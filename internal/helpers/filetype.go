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
// It expects patterns like: domain.com-rest-of-stuff or domain.com_rest_of_stuff
// Returns the domain part (before first separator) if it contains a dot.
// If no separator found but string contains a dot, returns the whole string.
func ExtractCustomerDomain(resourceFile string) string {
	if resourceFile == "" {
		return ""
	}

	// Check if it looks like a domain (contains at least one dot)
	if !strings.Contains(resourceFile, ".") {
		return ""
	}

	// Find the first separator (hyphen or underscore)
	hyphenIdx := strings.IndexAny(resourceFile, "-_")
	if hyphenIdx == -1 {
		// No separator found - return the whole string if it has a dot
		return resourceFile
	}

	// Extract the part before the first separator
	return resourceFile[:hyphenIdx]
}
