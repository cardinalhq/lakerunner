// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package metricsprocessing

import "fmt"

// ConfigMismatchError represents an error when message metadata doesn't match gatherer configuration
type ConfigMismatchError struct {
	Field    string // "topic" or "consumer_group"
	Expected string
	Got      string
}

func (e *ConfigMismatchError) Error() string {
	return fmt.Sprintf("unexpected %s: got %q, expected %q", e.Field, e.Got, e.Expected)
}

// GroupValidationError represents an error when message group validation fails
type GroupValidationError struct {
	Field    string      // field that failed validation
	Expected interface{} // expected value
	Got      interface{} // actual value
	Message  string      // description of the validation failure
}

func (e *GroupValidationError) Error() string {
	return fmt.Sprintf("group validation failed for %s: %s", e.Field, e.Message)
}
