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
ARG DUCKDB_SDK_VERSION=latest
ARG TARGETARCH

# ========= Stage 1: Extract DuckDB SDK =========
FROM public.ecr.aws/cardinalhq.io/duckdb-sdk:${DUCKDB_SDK_VERSION}-${TARGETARCH} AS duckdb-sdk

# ========= Stage 2: Build Go Application =========
FROM golang:1.24-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy DuckDB SDK files
COPY --from=duckdb-sdk /usr/local /usr/local
COPY --from=duckdb-sdk /lib /lib
COPY --from=duckdb-sdk /etc/ssl /etc/ssl

# Update library cache
RUN ldconfig

# Set working directory
WORKDIR /workspace

# Copy Go modules and download dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the binary with CGO enabled and custom DuckDB
ENV CGO_ENABLED=1
ENV PKG_CONFIG_PATH=/usr/local/lib/pkgconfig

# Build binary (goreleaser will override this during real builds)
ARG LDFLAGS=""
RUN go build -ldflags="${LDFLAGS}" -o lakerunner main.go

# ========= Stage 3: Runtime Image =========
FROM gcr.io/distroless/cc-debian12:nonroot

# Copy SSL certificates for HTTPS support (statically linked binary needs no DuckDB libs)
COPY --from=duckdb-sdk /etc/ssl /etc/ssl

# Copy the binary
COPY --from=builder /workspace/lakerunner /app/bin/lakerunner

# No library path needed for statically linked binary

CMD ["/app/bin/lakerunner"]
