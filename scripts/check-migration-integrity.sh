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

# Check if we're on a tag - if so, skip migration integrity check
if git describe --exact-match --tags HEAD >/dev/null 2>&1; then
    echo "Detected tag checkout - skipping migration integrity check for tagged release."
    echo "Migration integrity should have been validated during development."
    exit 0
fi

# Find the merge-base (branch point) automatically
# Prioritize main/master over upstream tracking to ensure we're comparing against the main branch
if [ -n "${1:-}" ]; then
    # Use provided base branch
    BASE_REF="$1"
elif git rev-parse --verify origin/main >/dev/null 2>&1; then
    # Prefer origin/main
    BASE_REF="origin/main"
elif git rev-parse --verify origin/master >/dev/null 2>&1; then
    # Fall back to origin/master
    BASE_REF="origin/master"
elif git rev-parse --verify @{upstream} >/dev/null 2>&1; then
    # Last resort: use upstream tracking branch
    BASE_REF="@{upstream}"
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
# Only consider properly named .up.sql and .down.sql files
EXISTING_MIGRATIONS=$(git ls-tree -r --name-only "$MERGE_BASE" -- lrdb/migrations/ configdb/migrations/ | grep -E '\.(up|down)\.sql$' || true)

# Get list of new migrations created in current branch (these can be freely modified)
# Only consider properly named .up.sql and .down.sql files
# Include both committed and uncommitted new files
COMMITTED_NEW=$(git diff --name-only --diff-filter=A "$MERGE_BASE..HEAD" -- lrdb/migrations/ configdb/migrations/ | grep -E '\.(up|down)\.sql$' || true)
UNCOMMITTED_NEW=$(git ls-files --others --exclude-standard -- lrdb/migrations/ configdb/migrations/ | grep -E '\.(up|down)\.sql$' || true)
NEW_MIGRATIONS=$(echo -e "$COMMITTED_NEW\n$UNCOMMITTED_NEW" | sort -u | grep -v '^$' || true)

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

# Function to extract timestamp from migration filename
extract_timestamp() {
    local filename="$1"
    basename "$filename" | sed -E 's/^([0-9]+)_.*/\1/'
}

# Function to extract migration name (without .up/.down.sql)
extract_migration_name() {
    local filename="$1"
    basename "$filename" | sed -E 's/^([0-9]+_[^.]+)\.(up|down)\.sql$/\1/'
}

# Check for up/down pair completeness for ALL migrations (existing + new)
echo "Checking migration up/down pairs..."

# Use find with -exec to check pairs directly, avoiding multiple passes
# This is the most efficient approach - single filesystem traversal
for dir in lrdb/migrations configdb/migrations; do
    if [ -d "$dir" ]; then
        for up_file in "$dir"/*.up.sql; do
            if [ -f "$up_file" ]; then
                down_file="${up_file%.up.sql}.down.sql"
                if [ ! -f "$down_file" ]; then
                    migration_name=$(extract_migration_name "$up_file")
                    echo "VIOLATION: Missing down migration for $migration_name"
                    echo "   Found up file: $up_file"
                    echo "   Expected: $down_file"
                    VIOLATIONS_FOUND=true
                fi
            fi
        done
        
        for down_file in "$dir"/*.down.sql; do
            if [ -f "$down_file" ]; then
                up_file="${down_file%.down.sql}.up.sql"
                if [ ! -f "$up_file" ]; then
                    migration_name=$(extract_migration_name "$down_file")
                    echo "VIOLATION: Missing up migration for $migration_name"
                    echo "   Found down file: $down_file"
                    echo "   Expected: $up_file"
                    VIOLATIONS_FOUND=true
                fi
            fi
        done
    fi
done

# Check sequential ordering of new migrations
echo "Checking migration timestamp ordering..."
if [ -n "$NEW_MIGRATIONS" ]; then
    # Get the highest timestamp from existing migrations
    HIGHEST_EXISTING_TIMESTAMP=0
    if [ -n "$EXISTING_MIGRATIONS" ]; then
        while IFS= read -r migration; do
            if [ -n "$migration" ]; then
                timestamp=$(extract_timestamp "$migration")
                if [ "$timestamp" -gt "$HIGHEST_EXISTING_TIMESTAMP" ]; then
                    HIGHEST_EXISTING_TIMESTAMP="$timestamp"
                fi
            fi
        done <<< "$EXISTING_MIGRATIONS"
    fi

    # Check that all new migrations have timestamps higher than existing ones
    while IFS= read -r migration; do
        if [ -n "$migration" ]; then
            timestamp=$(extract_timestamp "$migration")
            if [ "$timestamp" -le "$HIGHEST_EXISTING_TIMESTAMP" ]; then
                echo "VIOLATION: New migration $migration has timestamp $timestamp"
                echo "   This is not greater than the highest existing timestamp: $HIGHEST_EXISTING_TIMESTAMP"
                echo "   New migrations must have timestamps after all existing migrations"
                VIOLATIONS_FOUND=true
            fi
        fi
    done <<< "$NEW_MIGRATIONS"
fi

echo "Checking existing migration integrity..."

# Use git diff-tree to efficiently find what changed
# This gets us only the files that actually differ from merge base
CHANGED_FILES=$(git diff-tree -r --name-only --diff-filter=AMD "$MERGE_BASE" HEAD -- 'lrdb/migrations/*.sql' 'configdb/migrations/*.sql' | grep -E '\.(up|down)\.sql$' || true)

# Also check uncommitted changes
UNCOMMITTED=$(git status --porcelain -- 'lrdb/migrations/*.sql' 'configdb/migrations/*.sql' | awk '{print $2}' | grep -E '\.(up|down)\.sql$' || true)

# Combine and deduplicate
ALL_CHANGED=$(echo -e "$CHANGED_FILES\n$UNCOMMITTED" | sort -u | grep -v '^$' || true)

if [ -n "$ALL_CHANGED" ]; then
    while IFS= read -r migration; do
        # Skip if this migration is new (doesn't exist at merge base)
        if ! git ls-tree "$MERGE_BASE" -- "$migration" | grep -q .; then
            continue
        fi
        
        # Check if file was deleted
        if [ ! -f "$migration" ]; then
            echo "VIOLATION: $migration has been deleted"
            echo "   This migration existed before your branch and cannot be deleted."
            VIOLATIONS_FOUND=true
            continue
        fi
        
        # File exists and existed before - compare content
        BASE_CONTENT=$(git show "$MERGE_BASE:$migration")
        CURRENT_CONTENT=$(cat "$migration")
        
        # Quick hash check first
        BASE_HASH=$(echo "$BASE_CONTENT" | shasum -a 256 | cut -d' ' -f1)
        CURRENT_HASH=$(echo "$CURRENT_CONTENT" | shasum -a 256 | cut -d' ' -f1)
        
        if [ "$BASE_HASH" = "$CURRENT_HASH" ]; then
            continue
        fi
        
        # Hashes differ - check if it's just whitespace/comments
        BASE_NORMALIZED=$(normalize_sql "$BASE_CONTENT")
        CURRENT_NORMALIZED=$(normalize_sql "$CURRENT_CONTENT")
        
        if [ "$BASE_NORMALIZED" != "$CURRENT_NORMALIZED" ]; then
            echo "VIOLATION: $migration has substantive changes beyond comments/whitespace"
            echo "   This migration existed before your branch and cannot be substantially modified."
            echo ""
            echo "   Diff of normalized content:"
            diff -u <(echo "$BASE_NORMALIZED") <(echo "$CURRENT_NORMALIZED") || true
            echo ""
            VIOLATIONS_FOUND=true
        fi
    done <<< "$ALL_CHANGED"
fi

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
