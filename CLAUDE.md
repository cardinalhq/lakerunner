# Claude Development Guide

This document provides information for Claude Code when working on the lakerunner project.

## Build Process

**Always use `make` for building the project. Do NOT use `go build` directly.**

### Standard Build Commands

- **Development build**: `make local`
  - Runs `go generate ./...`
  - Runs tests
  - Produces binary at `./bin/lakerunner`

- **Full test suite**: `make test`
  - Runs `go generate ./...`
  - Runs `go test -race ./...`

- **Complete check**: `make check`
  - Runs tests
  - Runs license checks
  - Runs linter

### Binary Location

The proper build process creates the binary at:

```sh
./bin/lakerunner
```

**Do NOT use** `go build -o lakerunner .` - this bypasses important build steps.

## Project Structure

- `promql/` - PromQL query processing and worker discovery
- `cmd/` - Command-line interface and subcommands
  - `cmd/debug/` - Debug subcommands for troubleshooting
- `bin/` - Built binaries (created by make)

## Testing

- Use `make test` for running tests
- The project includes race detection (`-race` flag)
- Tests must pass before any binary is built

## Environment Variables

The project uses specific environment variable naming conventions. See AGENTS.md for worker discovery configuration.
