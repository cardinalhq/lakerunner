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
#
# Downloads performance test data from S3 (test account data only)
# Usage: ./scripts/download-perf-testdata.sh {raw|cooked} [count]

set -euo pipefail

BUCKET="chq-saas-us-east-2-4d79e03f"
ORG_ID="65928f26-224b-4acb-8e57-9ee628164694"
DEST_DIR="/tmp/lakerunner-perftest"

# Parse arguments
DATA_TYPE="${1:-raw}"
FILE_COUNT="${2:-10}"

if [[ "$DATA_TYPE" != "raw" && "$DATA_TYPE" != "cooked" ]]; then
    echo "Error: First argument must be 'raw' or 'cooked'"
    echo "Usage: $0 {raw|cooked} [count]"
    exit 1
fi

# Set S3 prefix based on data type
if [[ "$DATA_TYPE" == "raw" ]]; then
    S3_PREFIX="s3://${BUCKET}/otel-raw/${ORG_ID}/"
    LOCAL_DIR="${DEST_DIR}/raw"
else
    S3_PREFIX="s3://${BUCKET}/db/${ORG_ID}/"
    LOCAL_DIR="${DEST_DIR}/cooked"
fi

echo "==> Downloading ${FILE_COUNT} ${DATA_TYPE} files from S3..."
echo "    Source: ${S3_PREFIX}"
echo "    Dest:   ${LOCAL_DIR}"

# Create destination directory
mkdir -p "${LOCAL_DIR}"

# List files and download first N
echo "==> Listing available files..."
aws s3 ls "${S3_PREFIX}" --recursive | head -n "${FILE_COUNT}" | while read -r line; do
    # Extract the file path from ls output (4th column)
    file_path=$(echo "$line" | awk '{print $4}')
    file_size=$(echo "$line" | awk '{print $3}')

    if [[ -z "$file_path" ]]; then
        continue
    fi

    # Download the file
    s3_url="s3://${BUCKET}/${file_path}"
    local_file="${LOCAL_DIR}/$(basename "${file_path}")"

    echo "  Downloading: $(basename "${file_path}") (${file_size} bytes)"
    aws s3 cp "${s3_url}" "${local_file}" --quiet
done

echo "==> Download complete!"
echo "    Files stored in: ${LOCAL_DIR}"
echo ""
echo "To use in tests:"
echo "  export PERFTEST_DATA_DIR=${LOCAL_DIR}"
echo ""
echo "To clean up:"
echo "  rm -rf ${DEST_DIR}"
