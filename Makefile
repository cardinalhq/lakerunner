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

TARGETS=test local
PLATFORM=linux/amd64,linux/arm64
BUILDX=docker buildx build --pull --platform ${PLATFORM}
IMAGE_PREFIX=public.ecr.aws/cardinalhq.io/
IMAGE_TAG=latest-dev

#
# Build targets.  Adding to these will cause magic to occur.
#

# These are targets for "make local"
BINARIES = lakerunner

# These are the targets for Docker images, used both for the multi-arch and
# single (local) Docker builds.
# Dockerfiles should have a target that ends in -image, e.g. agent-image.
IMAGE_TARGETS = lakerunner

#
# Below here lies magic...
#

# Due to the way we build, we will make the universe no matter which files
# actually change.  With the many targets, this is just so much easier,
# and it also ensures the Docker images have identical timestamp-based tags.
all_deps := $(shell find . -name '*.go' | grep -v _test) Makefile

#
# Default target.
#

.PHONY: all
all: ${TARGETS}

# name of the buildx builder we’ll always (re)create
BUILDER := lakerunner-builder

# convenience recipe to always start with a clean builder
define with_builder
	@docker buildx rm $(BUILDER) >/dev/null 2>&1 || true; \
	docker buildx create --name $(BUILDER) --driver docker-container --use --bootstrap; \
	trap 'docker buildx rm $(BUILDER) >/dev/null 2>&1' EXIT; \
	$(1)
endef

#
# Generate all the things.
#
.PHONY: generate
generate:
	go generate ./...

#
# Run pre-commit checks
#
check: test license-check lint

license-check:
	go tool license-eye header check

lint:
	go tool golangci-lint run --timeout 15m --config .golangci.yaml

#
# Build locally, mostly for development speed.
#
.PHONY: local
local: $(addprefix bin/,$(BINARIES))

bin/lakerunner: ${all_deps}
	@[ -d bin ] || mkdir bin
	go build -o $@ main.go

#
# Multi-architecture image builds
#
.PHONY: images
images: test-only
	$(call with_builder, go tool goreleaser release --clean)

#
# Test targets
#

.PHONY: test
test: generate test-only

.PHONY: test-only
test-only:
	go test -race ./...


#
# promode to prod
#

.PHONY: promote-to-prod
promote-to-prod:
	crane cp ${IMAGE_PREFIX}lakerunner:${IMAGE_TAG} ${IMAGE_PREFIX}lakerunner:latest

#
# Clean the world.
#

.PHONY: clean
clean:
	rm -f bin/*

.PHONY: really-clean
really-clean: clean

new-migration:
	@if [ -z "$$name" ]; then echo "Usage: make new-migration name=migration_name"; exit 1; fi; \
	ts=$$(date +%s); \
	up_file="pkg/lrdb/migrations/$${ts}_$${name}.up.sql"; \
	down_file="pkg/lrdb/migrations/$${ts}_$${name}.down.sql"; \
	touch "$$up_file" "$$down_file"; \
	echo "-- $${ts}_$${name}.up.sql" > "$$up_file"; \
	echo "-- $${ts}_$${name}.down.sql" > "$$down_file"; \
	echo "Created: $$up_file $$down_file"
