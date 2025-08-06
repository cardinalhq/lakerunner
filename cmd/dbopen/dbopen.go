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

package dbopen

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
)

// getDatabaseURLFromEnv constructs a PostgreSQL URL from environment
// variables named PREFIX_HOST, PREFIX_PORT, PREFIX_USER, PREFIX_PASSWORD,
// PREFIX_DBNAME, and optionally PREFIX_SSLMODE. If PREFIX does not end in
// "_", it will be added automatically.
//
// It requires at minimum HOST and DBNAME, and will default PORT to 5432.
// Returns an error listing any missing required variables.
func getDatabaseURLFromEnv(prefix string) (string, error) {
	if !strings.HasSuffix(prefix, "_") {
		prefix += "_"
	}

	// First check to see if prefix_URL is set.  If so, return it directly.
	if urlStr := os.Getenv(prefix + "URL"); urlStr != "" {
		return urlStr, nil
	}

	// required
	host := os.Getenv(prefix + "HOST")
	dbname := os.Getenv(prefix + "DBNAME")

	var missing []string
	if host == "" {
		missing = append(missing, prefix+"HOST")
	}
	if dbname == "" {
		missing = append(missing, prefix+"DBNAME")
	}
	if len(missing) > 0 {
		return "", fmt.Errorf(
			"missing required environment variable(s): %s",
			strings.Join(missing, ", "),
		)
	}

	// optional with defaults
	port := os.Getenv(prefix + "PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv(prefix + "USER")
	pass := os.Getenv(prefix + "PASSWORD")

	sslmode := os.Getenv(prefix + "SSLMODE") // e.g. "require", "disable"

	u := &url.URL{
		Scheme: "postgresql",
		Host:   host + ":" + port,
		Path:   dbname,
	}

	if user != "" {
		if pass != "" {
			u.User = url.UserPassword(user, pass)
		} else {
			u.User = url.User(user)
		}
	}

	// add sslmode or any other query params
	q := u.Query()
	if sslmode != "" {
		q.Set("sslmode", sslmode)
	}
	u.RawQuery = q.Encode()

	return u.String(), nil
}

var ErrDatabaseNotConfigured = errors.New("database connection configuration is unavailable")
