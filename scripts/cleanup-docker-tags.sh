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

REPOSITORY_NAME="lakerunner"
AWS_REGION="us-east-1"

# Default to dry-run mode
DRY_RUN=true

print_usage() {
    echo "Usage: $0 [--dry-run|--execute]"
    echo ""
    echo "Finds Docker tags that might need cleanup and optionally deletes them."
    echo ""
    echo "Tags that are KEPT:"
    echo "  - latest, latest-dev, stable"
    echo "  - v123 or 123 (major version tags)"
    echo "  - v123.123 or 123.123 (minor version tags)"
    echo "  - v123.123.123 or 123.123.123 (release tags)"
    echo "  - v123.123.123-rc72 or 123.123.123-rc72 (release candidate tags)"
    echo "  - v0.0.0-pr123-* (PR tags with open PRs)"
    echo ""
    echo "Tags that are DELETED:"
    echo "  - v0.0.0-pr123-* (PR tags with closed/merged PRs)"
    echo "  - v0.0.0-branch-* (old branch-based tags)"
    echo "  - Any other non-standard tags"
    echo ""
    echo "Options:"
    echo "  --dry-run    Show what would be deleted (default)"
    echo "  --execute    Actually delete the tags"
    echo ""
}

# Parse command line arguments
case "${1:-}" in
    --dry-run)
        DRY_RUN=true
        ;;
    --execute)
        DRY_RUN=false
        ;;
    --help|-h)
        print_usage
        exit 0
        ;;
    "")
        DRY_RUN=true
        ;;
    *)
        echo "Error: Unknown option $1"
        print_usage
        exit 1
        ;;
esac

echo "Docker tag cleanup script"
if [ "$DRY_RUN" = true ]; then
    echo "Mode: DRY RUN (use --execute to actually delete tags)"
else
    echo "Mode: EXECUTE (will actually delete tags)"
fi
echo ""

# Function to check if a tag should be kept
should_keep_tag() {
    local tag="$1"

    # Keep special tags
    case "$tag" in
        latest|latest-dev|stable)
            return 0
            ;;
    esac

    # Keep version tags (both with and without v prefix for helm compatibility)
    # Major version (v1 or 1)
    if [[ "$tag" =~ ^v?[0-9]+$ ]]; then
        return 0
    fi

    # Minor version (v1.2 or 1.2)
    if [[ "$tag" =~ ^v?[0-9]+\.[0-9]+$ ]]; then
        return 0
    fi

    # Release version (v1.2.3 or 1.2.3)
    if [[ "$tag" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
        return 0
    fi

    # RC tags (v1.2.3-rc1 or 1.2.3-rc1)
    if [[ "$tag" =~ ^v?[0-9]+\.[0-9]+\.[0-9]+-rc[0-9]+$ ]]; then
        return 0
    fi

    # For PR tags, check if the PR is still open
    if [[ "$tag" =~ ^v0\.0\.0-pr([0-9]+)- ]]; then
        local pr_number="${BASH_REMATCH[1]}"

        # Check if PR is still open
        if gh pr view "$pr_number" --json state --jq '.state' 2>/dev/null | grep -q "OPEN"; then
            return 0  # Keep tags for open PRs
        else
            return 1  # Delete tags for closed/merged PRs
        fi
    fi

    # Delete everything else (old branch tags, malformed tags, etc.)
    return 1
}

# Function to get all tags from ECR
get_all_tags() {
    aws ecr-public describe-images \
        --repository-name "$REPOSITORY_NAME" \
        --region "$AWS_REGION" \
        --query 'imageDetails[*].imageTags[*]' \
        --output text | tr '\t' '\n' | grep -v '^$' | sort -u
}

# Function to delete a tag
delete_tag() {
    local tag="$1"

    # Final safety check - never delete in dry run mode
    if [ "$DRY_RUN" = true ]; then
        return
    fi

    echo "Deleting tag: $tag..."
    aws ecr-public batch-delete-image \
        --repository-name "$REPOSITORY_NAME" \
        --region "$AWS_REGION" \
        --image-ids imageTag="$tag" \
        --output text >/dev/null
}

# Main logic
echo "Fetching all Docker tags from ECR..."
ALL_TAGS=$(get_all_tags)

if [ -z "$ALL_TAGS" ]; then
    echo "No tags found in repository."
    exit 0
fi

echo "Found $(echo "$ALL_TAGS" | wc -l) total tags."
echo ""

TAGS_TO_DELETE=()
TAGS_TO_KEEP=()

echo "Analyzing tags..."
while IFS= read -r tag; do
    if [ -n "$tag" ]; then
        if should_keep_tag "$tag"; then
            TAGS_TO_KEEP+=("$tag")
        else
            TAGS_TO_DELETE+=("$tag")
        fi
    fi
done <<< "$ALL_TAGS"

echo ""
echo "=== SUMMARY ==="
echo "Tags to keep: ${#TAGS_TO_KEEP[@]}"
echo "Tags to delete: ${#TAGS_TO_DELETE[@]}"
echo ""

if [ ${#TAGS_TO_KEEP[@]} -gt 0 ]; then
    echo "Tags that will be KEPT:"
    printf '  %s\n' "${TAGS_TO_KEEP[@]}" | head -10
    if [ ${#TAGS_TO_KEEP[@]} -gt 10 ]; then
        echo "  ... and $((${#TAGS_TO_KEEP[@]} - 10)) more"
    fi
    echo ""
fi

if [ ${#TAGS_TO_DELETE[@]} -gt 0 ]; then
    echo "Tags that will be DELETED:"
    printf '  %s\n' "${TAGS_TO_DELETE[@]}"
    echo ""

    if [ "$DRY_RUN" = false ]; then
        echo "Proceeding with deletion in 5 seconds... (Ctrl+C to cancel)"
        sleep 5
    fi

    if [ "$DRY_RUN" = false ]; then
        echo "Processing deletions..."
        for tag in "${TAGS_TO_DELETE[@]}"; do
            delete_tag "$tag"
        done
    fi

    if [ "$DRY_RUN" = false ]; then
        echo ""
        echo "✓ Cleanup completed successfully!"
        echo "  Deleted: ${#TAGS_TO_DELETE[@]} tags"
        echo "  Kept: ${#TAGS_TO_KEEP[@]} tags"
    fi
else
    echo "No tags need cleanup - all tags follow the expected patterns!"
fi

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo "This was a dry run. Use --execute to actually delete the tags."
fi
