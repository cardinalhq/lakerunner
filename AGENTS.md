# For our robotic overlords

Lakerunner is a batch processing system that reads files from an object store
and writes them back in a consistent format.  It also indexes them.

Currently telemetry types of logs and metrics are processed, but the code base
may be extended to include traces/spans as well as profile data.

The incoming format is typically Parquet.  This is often written by CardinaHQ's
Open Telemetry collector exporter in a specific format (these are files under the
bucket in otel-raw/ path prefix) but we can use some heuristics to handle arbitrary
Parquet files, specific json.gz format archives for logs, and some other formats.

## Coding Guidelines

* Use modern go.  We use at least 1.24, so things like ranging on an integer is valid syntax.  Do not convert these into for loops, as it is not needed.
* When making changes to large functions, it is acceptable and often preferred to break the larger function into smaller, testable helper methods.  If this is done, tests should be included.
* If tests already exist for a function that is being changed, the tests should be updated to address the specific change.
* For simple functions, a table driven test approach is preferred.  When complex setup or complex inspection is required, it is OK to write many individual tests.
* For public methods added to classes, the Go docs should be updated to match the change.  If no docs are present, add one.  Do not add simplistic docs though, like adding "NewThing creates a new thing" if the function name is clear and unambigious.

## Good Pull Requests

* Good pull requests should be focused on a specific, testable, and reviewable change.
* PRs must leave the code in a working state.
* "main" branch must always end up as a releasable branch at all times.

## Unit Testing

* New files added for Go or other formats that are considered source code must have a license header.  That license header must be AGPL v3, and would look generally like the license shown below.
* "make test" is a good command to run the full test suite, which will also rebuild any generate files.
* "make check" needs to pass prior to a PR being made.  "make check" will run a linter as well, which can take some significant time to run.  If "make check" consistently fails to complete in the time allowed, fall back to using "make test license-check" instead.

## Build Tools

* Protobuf generation relies on the Buf CLI. Install it with `make bin/buf`, which places the binary in `./bin` for use by `make generate` and other targets.

## Licensing

This is a common header our source and source-like files must use

```go
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
```
