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

# Build arguments
ARG TARGETARCH
ARG DUCKDB_VERSION=v1.3.2

# ========= Extension Download Stage =========
FROM debian:bookworm-slim AS extensions

ARG TARGETARCH
ARG DUCKDB_VERSION

# Install curl and copy all its runtime dependencies
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/* && \
    mkdir -p /runtime-deps && \
    ldd /usr/bin/curl | grep "=> /" | awk '{print $3}' | while read lib; do \
        if [ -f "$lib" ]; then \
            DEST_DIR="/runtime-deps$(dirname "$lib")" && \
            mkdir -p "$DEST_DIR" && \
            cp "$lib" "$DEST_DIR/"; \
        fi; \
    done

# Download httpfs, aws, and azure extensions
RUN mkdir -p /app/extensions && \
    curl -fsSL "https://extensions.duckdb.org/${DUCKDB_VERSION}/linux_${TARGETARCH}/httpfs.duckdb_extension.gz" \
    | gunzip -c > /app/extensions/httpfs.duckdb_extension && \
    curl -fsSL "https://extensions.duckdb.org/${DUCKDB_VERSION}/linux_${TARGETARCH}/aws.duckdb_extension.gz" \
    | gunzip -c > /app/extensions/aws.duckdb_extension && \
    curl -fsSL "https://extensions.duckdb.org/${DUCKDB_VERSION}/linux_${TARGETARCH}/azure.duckdb_extension.gz" \
    | gunzip -c > /app/extensions/azure.duckdb_extension

# ========= Runtime Image =========
FROM gcr.io/distroless/cc-debian12:nonroot

# Copy the pre-built binary from goreleaser
COPY --chmod=755 lakerunner /app/bin/lakerunner

# Copy httpfs, aws, and azure extensions
COPY --from=extensions /app/extensions/httpfs.duckdb_extension /app/extensions/httpfs.duckdb_extension
COPY --from=extensions /app/extensions/aws.duckdb_extension /app/extensions/aws.duckdb_extension
COPY --from=extensions /app/extensions/azure.duckdb_extension /app/extensions/azure.duckdb_extension

# Copy curl binary and its runtime dependencies
COPY --from=extensions /usr/bin/curl /usr/bin/curl
COPY --from=extensions /runtime-deps/ /

# Set environment variables for DuckDB extension configuration
ENV LAKERUNNER_DUCKDB_EXTENSIONS_PATH=/app/extensions
ENV LAKERUNNER_DUCKDB_HTTPFS_EXTENSION=/app/extensions/httpfs.duckdb_extension
ENV LAKERUNNER_DUCKDB_AWS_EXTENSION=/app/extensions/aws.duckdb_extension
ENV LAKERUNNER_DUCKDB_AZURE_EXTENSION=/app/extensions/azure.duckdb_extension

CMD ["/app/bin/lakerunner"]
