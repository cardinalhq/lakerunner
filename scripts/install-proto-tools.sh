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

# Proto tool versions - update these to change versions across the project
BUF_VERSION="v1.56.0"
PROTOC_GEN_GO_VERSION="v1.36.10"
PROTOC_GEN_GO_GRPC_VERSION="v1.6.0"

# Project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$PROJECT_ROOT/bin"

echo "Installing protobuf tools to $BIN_DIR..."

# Create bin directory if it doesn't exist
mkdir -p "$BIN_DIR"

# Install tools with pinned versions to project-local bin directory
GOBIN="$BIN_DIR" go install "github.com/bufbuild/buf/cmd/buf@$BUF_VERSION"
GOBIN="$BIN_DIR" go install "google.golang.org/protobuf/cmd/protoc-gen-go@$PROTOC_GEN_GO_VERSION"
GOBIN="$BIN_DIR" go install "google.golang.org/grpc/cmd/protoc-gen-go-grpc@$PROTOC_GEN_GO_GRPC_VERSION"

echo "Protobuf tools installed successfully:"
echo "  buf: $BUF_VERSION"
echo "  protoc-gen-go: $PROTOC_GEN_GO_VERSION"
echo "  protoc-gen-go-grpc: $PROTOC_GEN_GO_GRPC_VERSION"
