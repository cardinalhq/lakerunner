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
IMAGE_TAG=dev-$(shell date +%Y%m%d-%H%M%S)-$(shell git rev-parse --short HEAD)

#
# Build targets.  Adding to these will cause magic to occur.
#

# These are targets for "make local"
BINARIES = lakerunner lakectl

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
all: duckdb-extensions-decompress gofmt ${TARGETS}

#
# Help target - display available make targets
#
.PHONY: help
help:
	@echo "Lakerunner Makefile Targets"
	@echo "============================"
	@echo ""
	@echo "Building:"
	@echo "  make              - Run tests and build locally (default)"
	@echo "  make local        - Build all binaries locally"
	@echo "  make docker       - Build Docker images"
	@echo "  make docker-multi - Build multi-arch Docker images"
	@echo ""
	@echo "Testing:"
	@echo "  make test         - Run all tests with code generation"
	@echo "  make test-only    - Run tests without code generation"
	@echo "  make test-integration - Run integration tests (requires test databases)"
	@echo "  make test-kafka   - Run Kafka tests (with containerized Kafka)"
	@echo "  make test-ci      - Run all CI tests"
	@echo "  make coverage     - Generate test coverage report"
	@echo "  make coverage-html - Generate HTML coverage report and open in browser"
	@echo ""
	@echo "Code Quality:"
	@echo "  make check        - Run all pre-commit checks (concise output)"
	@echo "  make check-verbose - Run all pre-commit checks (verbose output)"
	@echo "  make lint         - Run golangci-lint"
	@echo "  make fmt          - Format code with gofmt and goimports"
	@echo "  make gofmt        - Format code with gofmt only"
	@echo "  make imports-fix  - Fix import organization"
	@echo "  make license-check - Check license headers"
	@echo ""
	@echo "Code Generation:"
	@echo "  make generate     - Generate all code (SQL, protobuf, etc.)"
	@echo ""
	@echo "Database:"
	@echo "  make new-lrdb-migration name=<name> - Create new LRDB migration"
	@echo "  make new-configdb-migration name=<name> - Create new ConfigDB migration"
	@echo "  make check-migration-integrity - Verify migration files"
	@echo ""
	@echo "Utilities:"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make help         - Show this help message"

# name of the buildx builder we'll always (re)create
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
generate: bin/buf
	go generate ./...

bin/buf:
	./scripts/install-proto-tools.sh

bin/golangci-lint bin/goimports bin/license-eye:
	./scripts/install-dev-tools.sh

#
# Download otel_binpb extension from GitHub releases
#
OTEL_BINPB_VERSION=v0.3.0
OTEL_BINPB_BASE_URL=https://github.com/cardinalhq/duckdb-binpb/releases/download/${OTEL_BINPB_VERSION}

.PHONY: otel-binpb-extension-download
otel-binpb-extension-download:
	@echo "Downloading otel_binpb extension (${OTEL_BINPB_VERSION}) for all platforms..."
	@mkdir -p docker/duckdb-extensions/linux_amd64
	@mkdir -p docker/duckdb-extensions/linux_arm64
	@mkdir -p docker/duckdb-extensions/osx_amd64
	@mkdir -p docker/duckdb-extensions/osx_arm64
	curl -L -o docker/duckdb-extensions/linux_amd64/otel_binpb.duckdb_extension "${OTEL_BINPB_BASE_URL}/otel_binpb-linux_amd64.duckdb_extension"
	curl -L -o docker/duckdb-extensions/linux_arm64/otel_binpb.duckdb_extension "${OTEL_BINPB_BASE_URL}/otel_binpb-linux_arm64.duckdb_extension"
	curl -L -o docker/duckdb-extensions/osx_amd64/otel_binpb.duckdb_extension "${OTEL_BINPB_BASE_URL}/otel_binpb-osx_amd64.duckdb_extension"
	curl -L -o docker/duckdb-extensions/osx_arm64/otel_binpb.duckdb_extension "${OTEL_BINPB_BASE_URL}/otel_binpb-osx_arm64.duckdb_extension"
	@echo "Compressing extensions..."
	@for f in docker/duckdb-extensions/*/otel_binpb.duckdb_extension; do gzip -kf "$$f"; done
	@echo "otel_binpb extension downloaded and compressed"

#
# Decompress DuckDB extensions (if compressed versions exist)
#
.PHONY: duckdb-extensions-decompress
duckdb-extensions-decompress:
	@if [ -d docker/duckdb-extensions ]; then \
		for dir in docker/duckdb-extensions/*/; do \
			for gz in $$dir*.duckdb_extension.gz; do \
				if [ -f "$$gz" ]; then \
					target=$${gz%.gz}; \
					if [ ! -f "$$target" ] || [ "$$gz" -nt "$$target" ]; then \
						echo "Decompressing $$gz..."; \
						gunzip -k -f "$$gz"; \
					fi; \
				fi; \
			done; \
		done; \
	fi

#
# Run pre-commit checks
#
check: bin/golangci-lint
	@./scripts/check-concise.sh

check-verbose: bin/golangci-lint
	@VERBOSE=1 ./scripts/check-concise.sh

license-check: bin/license-eye
	./bin/license-eye header check

imports-check: bin/goimports
	@echo "Checking import organization..."
	@./bin/goimports -local github.com/cardinalhq/lakerunner -l . | grep -v '\.pb\.go' | tee /tmp/goimports-check.out
	@if [ -s /tmp/goimports-check.out ]; then \
		echo "Import organization check failed. Files with incorrect imports:"; \
		cat /tmp/goimports-check.out; \
		echo "Run 'make imports-fix' to fix import organization"; \
		exit 1; \
	fi
	@echo "Import organization check passed"

fmt: gofmt imports-fix

imports-fix: bin/goimports
	@echo "Fixing import organization (excluding *.pb.go files)..."
	@find . -name '*.go' -not -name '*.pb.go' -exec ./bin/goimports -local github.com/cardinalhq/lakerunner -w {} \;

gofmt:
	@echo "Running gofmt to fix formatting..."
	@gofmt -w -s .

lint: bin/golangci-lint
	./bin/golangci-lint run --timeout 15m --config .golangci.yaml

#
# Build locally, mostly for development speed.
#
.PHONY: local
local: $(addprefix bin/,$(BINARIES))

bin/lakerunner: ${all_deps}
	@[ -d bin ] || mkdir bin
	go build -o $@ main.go

bin/lakectl: ${all_deps}
	@[ -d bin ] || mkdir bin
	go build -o $@ ./lakectl

#
# Test targets
#

.PHONY: test
test: generate test-only

.PHONY: test-only
test-only: duckdb-extensions-decompress
	go test -race ./...

.PHONY: test-integration
test-integration: duckdb-extensions-decompress bin/lakerunner
	@echo "Running integration tests (requires test databases)..."
	@echo "Running database migrations..."
	LRDB_HOST=$${LRDB_HOST:-localhost} \
	LRDB_USER=$${LRDB_USER:-$${USER}} \
	LRDB_PASSWORD=$${LRDB_PASSWORD} \
	LRDB_DBNAME=$${LRDB_DBNAME:-testing_lrdb} \
		./bin/lakerunner migrate --databases=lrdb
	CONFIGDB_HOST=$${CONFIGDB_HOST:-localhost} \
	CONFIGDB_USER=$${CONFIGDB_USER:-$${USER}} \
	CONFIGDB_PASSWORD=$${CONFIGDB_PASSWORD} \
	CONFIGDB_DBNAME=$${CONFIGDB_DBNAME:-testing_configdb} \
		./bin/lakerunner migrate --databases=configdb
	@echo "Running tests..."
	LRDB_HOST=$${LRDB_HOST:-localhost} LRDB_DBNAME=$${LRDB_DBNAME:-testing_lrdb} \
	CONFIGDB_HOST=$${CONFIGDB_HOST:-localhost} CONFIGDB_DBNAME=$${CONFIGDB_DBNAME:-testing_configdb} \
	go test -race -tags=integration ./...

# Run Kafka integration tests (with containerized Kafka via Gnomock)
.PHONY: test-kafka
test-kafka:
	@echo "Running Kafka integration tests (with containerized Kafka)..."
	go test -race -tags=kafkatest ./internal/fly -timeout=10m

.PHONY: test-ci
test-ci: test test-integration

#
# Coverage targets
#

.PHONY: coverage
coverage: generate
	@echo "Generating test coverage report..."
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	@echo "Total coverage:"
	go tool cover -func=coverage.out | grep total

.PHONY: coverage-html
coverage-html: generate
	@echo "Generating HTML test coverage report..."
	go test -race -coverprofile=coverage.out -covermode=atomic ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"
	@echo "Open in browser: open coverage.html"

.PHONY: coverage-serve
coverage-serve: coverage-html
	@echo "Starting HTTP server for coverage report..."
	@echo "Open http://localhost:8080/coverage.html in your browser"
	@echo "Press Ctrl+C to stop the server"
	@python3 -m http.server 8080 2>/dev/null || python -m SimpleHTTPServer 8080

#
# Clean the world.
#

.PHONY: clean
clean:
	rm -f bin/*
	@# Remove decompressed DuckDB extensions (keep .gz files)
	@find docker/duckdb-extensions -name "*.duckdb_extension" -type f -delete 2>/dev/null || true

.PHONY: really-clean
really-clean: clean

new-lrdb-migration:
	@if [ -z "$$name" ]; then echo "Usage: make new-lrdb-migration name=migration_name"; exit 1; fi; \
	ts=$$(date +%s); \
	up_file="lrdb/migrations/$${ts}_$${name}.up.sql"; \
	down_file="lrdb/migrations/$${ts}_$${name}.down.sql"; \
	touch "$$up_file" "$$down_file"; \
	echo "-- $${ts}_$${name}.up.sql" > "$$up_file"; \
	echo "" >> "$$up_file"; \
	echo "-- Code generated by database migration. DO NOT EDIT." >> "$$up_file"; \
	echo "" >> "$$up_file"; \
	echo "-- $${ts}_$${name}.down.sql" > "$$down_file"; \
	echo "" >> "$$down_file"; \
	echo "-- Code generated by database migration. DO NOT EDIT." >> "$$down_file"; \
	echo "" >> "$$down_file"; \
	echo "Created: $$up_file $$down_file"

new-configdb-migration:
	@if [ -z "$$name" ]; then echo "Usage: make new-configdb-migration name=migration_name"; exit 1; fi; \
	ts=$$(date +%s); \
	up_file="configdb/migrations/$${ts}_$${name}.up.sql"; \
	down_file="configdb/migrations/$${ts}_$${name}.down.sql"; \
	touch "$$up_file" "$$down_file"; \
	echo "-- $${ts}_$${name}.up.sql" > "$$up_file"; \
	echo "" >> "$$up_file"; \
	echo "-- Code generated by database migration. DO NOT EDIT." >> "$$up_file"; \
	echo "" >> "$$up_file"; \
	echo "-- $${ts}_$${name}.down.sql" > "$$down_file"; \
	echo "" >> "$$down_file"; \
	echo "-- Code generated by database migration. DO NOT EDIT." >> "$$down_file"; \
	echo "" >> "$$down_file"; \
	echo "Created: $$up_file $$down_file"

check-migration-integrity:
	@./scripts/check-migration-integrity.sh
