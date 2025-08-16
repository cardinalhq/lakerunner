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
ARG USER_UID=2000
ARG DUCKDB_VERSION=v1.3.2

# ========= Extension Download Stage =========
FROM debian:bookworm-slim AS extensions

ARG TARGETARCH
ARG DUCKDB_VERSION

# Install curl for downloading extensions
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Download httpfs extension
RUN mkdir -p /app/extensions && \
    curl -fsSL "https://extensions.duckdb.org/${DUCKDB_VERSION}/linux_${TARGETARCH}/httpfs.duckdb_extension.gz" \
    | gunzip -c > /app/extensions/httpfs.duckdb_extension

# ========= Runtime Image =========
FROM gcr.io/distroless/cc-debian12:nonroot

# Copy the pre-built binary from goreleaser
COPY lakerunner /app/bin/lakerunner

# Copy httpfs extension
COPY --from=extensions /app/extensions/httpfs.duckdb_extension /app/extensions/httpfs.duckdb_extension

# Set environment variable for extension location
ENV LAKERUNNER_HTTPFS_EXTENSION=/app/extensions/httpfs.duckdb_extension

# Run as specified user
USER ${USER_UID}:${USER_UID}

CMD ["/app/bin/lakerunner"]
