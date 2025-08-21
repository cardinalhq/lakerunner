#!/usr/bin/env bash

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

# Check migration file integrity against branch point
# Allows comment and whitespace changes, but blocks substantive changes

# Find the merge-base (branch point) automatically
# First try to find the remote tracking branch, fall back to main/master
if [ -n "${1:-}" ]; then
    # Use provided base branch
    BASE_REF="$1"
elif git rev-parse --verify @{upstream} >/dev/null 2>&1; then
    # Use upstream tracking branch
    BASE_REF="@{upstream}"
elif git rev-parse --verify origin/main >/dev/null 2>&1; then
    # Fall back to origin/main
    BASE_REF="origin/main"
elif git rev-parse --verify origin/master >/dev/null 2>&1; then
    # Fall back to origin/master
    BASE_REF="origin/master"
else
    echo "Error: Cannot determine base branch. Please specify one as an argument."
    exit 1
fi

# Find the actual merge-base commit
MERGE_BASE=$(git merge-base HEAD "$BASE_REF")

# Silently check migration file integrity against merge-base
# Only output on violations

# No need to fetch - we're working with the merge-base

# Get list of existing migrations at merge-base (these are the "protected" migrations)
EXISTING_MIGRATIONS=$(git ls-tree -r --name-only "$MERGE_BASE" -- lrdb/migrations/ configdb/migrations/ | grep '\.sql$' || true)

# Get list of new migrations created in current branch (these can be freely modified)
NEW_MIGRATIONS=$(git diff --name-only --diff-filter=A "$MERGE_BASE..HEAD" -- lrdb/migrations/ configdb/migrations/ | grep '\.sql$' || true)

if [ -z "$EXISTING_MIGRATIONS" ]; then
    # No existing migrations to check - silently succeed
    exit 0
fi

# Function to normalize SQL content for comparison
# Removes comments and normalizes whitespace
normalize_sql() {
    local content="$1"
    
    # Remove SQL comments (-- style)
    # Remove empty lines
    # Normalize whitespace (collapse multiple spaces/tabs to single space)
    # Remove leading/trailing whitespace from lines
    # Convert to lowercase for case-insensitive comparison
    echo "$content" | \
        sed 's/--.*$//' | \
        sed '/^[[:space:]]*$/d' | \
        sed -E 's/[[:space:]]+/ /g' | \
        sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | \
        tr '[:upper:]' '[:lower:]'
}

VIOLATIONS_FOUND=false

while IFS= read -r migration; do
    # Skip if this migration is new in the current branch (can be freely modified)
    if echo "$NEW_MIGRATIONS" | grep -q "^$migration$"; then
        continue
    fi
    
    if [ -f "$migration" ]; then
        # Get normalized content from both versions
        BASE_CONTENT=$(git show "$MERGE_BASE:$migration")
        CURRENT_CONTENT=$(cat "$migration")
        
        BASE_NORMALIZED=$(normalize_sql "$BASE_CONTENT")
        CURRENT_NORMALIZED=$(normalize_sql "$CURRENT_CONTENT")
        
        if [ "$BASE_NORMALIZED" != "$CURRENT_NORMALIZED" ]; then
            echo "VIOLATION: $migration has substantive changes beyond comments/whitespace"
            echo "   This migration existed before your branch and cannot be substantially modified."
            echo ""
            echo "   Diff of normalized content:"
            # Show the diff of normalized content to help identify the issue
            diff -u <(echo "$BASE_NORMALIZED") <(echo "$CURRENT_NORMALIZED") || true
            echo ""
            VIOLATIONS_FOUND=true
        fi
    else
        # File was deleted
        echo "VIOLATION: $migration has been deleted"
        echo "   This migration existed before your branch and cannot be deleted."
        VIOLATIONS_FOUND=true
    fi
done <<< "$EXISTING_MIGRATIONS"

if [ "$VIOLATIONS_FOUND" = true ]; then
    echo ""
    echo "Migration integrity check FAILED!"
    echo "   Existing migration files must not have substantive changes."
    echo "   Only comment and whitespace changes are allowed."
    echo "   This prevents database schema inconsistencies in deployed environments."
    echo "   Please create a new migration file instead."
    exit 1
fi

echo "Migration integrity check passed."
