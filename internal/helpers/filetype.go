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
// It expects the pattern: domain.com-rest-of-stuff
// Returns the domain part (before first hyphen) if it contains a dot, empty string otherwise.
func ExtractCustomerDomain(resourceFile string) string {
	if resourceFile == "" {
		return ""
	}

	// Find the first hyphen
	hyphenIdx := strings.Index(resourceFile, "-")
	if hyphenIdx == -1 {
		// No hyphen found - pattern doesn't match
		return ""
	}

	// Extract the part before the first hyphen
	potentialDomain := resourceFile[:hyphenIdx]

	// Check if it looks like a domain (contains at least one dot)
	if !strings.Contains(potentialDomain, ".") {
		return ""
	}

	return potentialDomain
}
