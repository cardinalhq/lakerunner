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

FROM alpine:latest AS certs
#RUN apk --update add ca-certificates

# FROM scratch

# RUN mkdir -p /apps/libs && \
#     curl -fsSL "https://extensions.duckdb.org/v1.3.2/${TARGETOS}_${TARGETARCH}/httpfs.duckdb_extension.gz" \
#     | gunzip -c > /app/libs/httpfs.duckdb_extension

ARG USER_UID=2000
USER ${USER_UID}:${USER_UID}

#COPY --from=certs /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY lakerunner /app/bin/lakerunner
CMD ["/app/bin/lakerunner"]
