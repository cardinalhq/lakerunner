#!/bin/bash
# Copyright (C) 2025-2026 CardinalHQ, Inc
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

set -e

echo "GoReleaser entrypoint starting..."
echo "Debug: Current user: $(whoami) ($(id))"
echo "Debug: Home directory: $HOME"

# Test write permissions
echo "Debug: Testing write permissions..."
touch /workspace/test-write && echo "Debug: Write test successful" && rm /workspace/test-write

# Show workspace ownership
echo "Debug: Workspace ownership:"
ls -la /workspace | head -3

# Configure git to trust the workspace directory
echo "Configuring git safe directory..."
git config --global --add safe.directory /workspace
git config --global --add safe.directory '*'

# Check git config was written
echo "Debug: Git config written:"
cat ~/.gitconfig 2>/dev/null || echo "No .gitconfig found"

# Verify git is working
echo "Testing git status..."
git status

# Login to ECR if AWS credentials are available
if [ -n "$AWS_ACCESS_KEY_ID" ] && [ -n "$AWS_SECRET_ACCESS_KEY" ]; then
    echo "Logging in to ECR..."
    aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws
fi

# Run GoReleaser with provided arguments
echo "Running GoReleaser with args: $@"
exec goreleaser "$@"