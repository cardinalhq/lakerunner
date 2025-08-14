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

# ========= Stage 1: Build DuckDB SDK =========
FROM debian:bookworm-slim AS build

# Adding "ninja-build" here speeds up the comilation significantly, but uses a lot more memory
# during the build.  If you want to try it out, add "ninja-build" to the apt-get install line below,
# and GEN=ninja to the make command below.

RUN apt-get update && apt-get install -y \
    git g++ cmake libssl-dev python3 ca-certificates \
 && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /workdir && chown 1000:1000 /workdir
WORKDIR /workdir
USER 1000:1000

ARG DUCKDB_VERSION=v1.3.2
RUN git clone --branch ${DUCKDB_VERSION} https://github.com/duckdb/duckdb.git
WORKDIR /workdir/duckdb

RUN BUILD_SHELL=0 CORE_EXTENSIONS='parquet;httpfs;json' GEN= make

FROM build AS collect
USER root

ENV INSTALL_PREFIX=/usr/local
RUN mkdir -p ${INSTALL_PREFIX}/include ${INSTALL_PREFIX}/lib

# Copy SDK headers and libs
RUN cp -r /workdir/duckdb/src/include/duckdb ${INSTALL_PREFIX}/include/
RUN cp /workdir/duckdb/build/release/src/libduckdb.so \
       /workdir/duckdb/build/release/src/libduckdb_static.a \
       ${INSTALL_PREFIX}/lib/

# pkg-config file for easier linking
RUN mkdir -p ${INSTALL_PREFIX}/lib/pkgconfig && \
    printf 'prefix=%s\nexec_prefix=${prefix}\nlibdir=${exec_prefix}/lib\nincludedir=${prefix}/include\n\nName: duckdb\nDescription: DuckDB\nVersion: %s\nLibs: -L${libdir} -lduckdb\nCflags: -I${includedir}\n' \
      "${INSTALL_PREFIX}" "${DUCKDB_VERSION}" > ${INSTALL_PREFIX}/lib/pkgconfig/duckdb.pc

# Collect runtime shared libs & dynamic loader into /rootfs with original absolute paths
RUN set -eux; \
    mkdir -p /rootfs; \
    so=${INSTALL_PREFIX}/lib/libduckdb.so; \
    ldd "$so" | awk '/=> \//{print $3} /ld-linux/{print $1}' \
      | xargs -I '{}' cp -v --parents '{}' /rootfs; \
    # also bring CA bundle for httpfs/SSL
    if [ -f /etc/ssl/certs/ca-certificates.crt ]; then \
      mkdir -p /rootfs/etc/ssl/certs; \
      cp -v /etc/ssl/certs/ca-certificates.crt /rootfs/etc/ssl/certs/; \
    fi

FROM scratch AS duckdb-sdk

COPY --from=collect /usr/local/include /usr/local/include
COPY --from=collect /usr/local/lib     /usr/local/lib

# Runtime shared libs + loader + CA bundle
COPY --from=collect /rootfs /

# This image is just a SDK/runtime; no entrypoint
# Multi-arch build targets same final image
