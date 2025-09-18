#!/bin/bash
# Copyright (C) 2025 CardinalHQ, Inc
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, version 3.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

set -euo pipefail

# Dev tool versions - update these to change versions across the project
GOLANGCI_LINT_VERSION="v2.4.0"
GOIMPORTS_VERSION="latest"

# Project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$PROJECT_ROOT/bin"

echo "Installing development tools to $BIN_DIR..."

# Create bin directory if it doesn't exist
mkdir -p "$BIN_DIR"

# Install tools with pinned versions to project-local bin directory
echo "Installing golangci-lint $GOLANGCI_LINT_VERSION..."
GOBIN="$BIN_DIR" go install "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$GOLANGCI_LINT_VERSION" || echo "Failed to install golangci-lint"

echo "Installing goimports $GOIMPORTS_VERSION..."
GOBIN="$BIN_DIR" go install "golang.org/x/tools/cmd/goimports@$GOIMPORTS_VERSION" || echo "Failed to install goimports"

echo ""
echo "Installation complete. Installed tools:"
ls -la "$BIN_DIR" | grep -E "golangci-lint|goimports" || echo "No tools found"