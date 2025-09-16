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

# ========= Extensions Stage (copy pre-downloaded extensions) =========
FROM debian:bookworm-slim AS extensions

ARG TARGETARCH

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

# ========= Runtime Image =========
FROM gcr.io/distroless/cc-debian12:nonroot

ARG TARGETARCH

# Copy the pre-built binary from goreleaser
COPY --chmod=755 lakerunner /app/bin/lakerunner
COPY --chmod=755 lakectl /app/bin/lakectl

# Copy pre-downloaded DuckDB extensions (already decompressed by make)
# These are platform-specific and decompressed at build time
COPY docker/duckdb-extensions/linux_${TARGETARCH}/httpfs.duckdb_extension /app/extensions/httpfs.duckdb_extension
COPY docker/duckdb-extensions/linux_${TARGETARCH}/aws.duckdb_extension /app/extensions/aws.duckdb_extension
COPY docker/duckdb-extensions/linux_${TARGETARCH}/azure.duckdb_extension /app/extensions/azure.duckdb_extension

# Copy curl binary and its runtime dependencies
COPY --from=extensions /usr/bin/curl /usr/bin/curl
COPY --from=extensions /runtime-deps/ /

# Set environment variable for DuckDB extension path
ENV LAKERUNNER_EXTENSIONS_PATH=/app/extensions

CMD ["/app/bin/lakerunner"]
