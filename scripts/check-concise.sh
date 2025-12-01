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

# Concise output for "make check" - shows only failures or a simple success message.
# For verbose output, use "make check-verbose"

set -o pipefail

VERBOSE=${VERBOSE:-0}
EXIT_CODE=0

# Color output helpers
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Temp files for capturing output
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

print_pass() {
    echo -e "${GREEN}✓${NC} $1"
}

print_fail() {
    echo -e "${RED}✗${NC} $1"
}

print_section() {
    echo ""
    echo -e "${YELLOW}=== $1 ===${NC}"
}

# Run tests
run_tests() {
    local output_file="$TMPDIR/test.out"

    if [ "$VERBOSE" = "1" ]; then
        go test -race ./...
        return $?
    fi

    go test -race ./... >"$output_file" 2>&1
    local result=$?

    if [ $result -eq 0 ]; then
        # Show summary: count passing packages, hide "[no test files]"
        local pass_count=$(grep -c "^ok" "$output_file" || true)
        print_pass "Tests passed ($pass_count packages)"
    else
        print_section "Test Failures"
        # Show compilation errors, test failures, and panics
        # Remove linker warnings about LC_DYSYMTAB
        grep -v "ld: warning:.*LC_DYSYMTAB" "$output_file" | \
            grep -E "^(FAIL|# |.*\.(go|sql):[0-9]+:|--- FAIL:|panic:)" || true
        print_fail "Tests failed"
    fi

    return $result
}

# Run license check
run_license_check() {
    local output_file="$TMPDIR/license.out"

    if [ "$VERBOSE" = "1" ]; then
        go tool license-eye header check
        return $?
    fi

    go tool license-eye header check >"$output_file" 2>&1
    local result=$?

    if [ $result -eq 0 ]; then
        print_pass "License check passed"
    else
        print_section "License Check Failures"
        cat "$output_file"
        print_fail "License check failed"
    fi

    return $result
}

# Run gofmt check
run_gofmt() {
    local output_file="$TMPDIR/gofmt.out"

    # gofmt -l shows files that need formatting
    gofmt -l . 2>&1 | grep -v "\.pb\.go" > "$output_file" || true

    if [ ! -s "$output_file" ]; then
        print_pass "Format check passed"
        return 0
    else
        if [ "$VERBOSE" = "1" ]; then
            print_section "Format Check Failures"
            cat "$output_file"
        fi
        print_fail "Format check failed - run 'make gofmt'"
        return 1
    fi
}

# Run lint
run_lint() {
    local output_file="$TMPDIR/lint.out"

    if [ "$VERBOSE" = "1" ]; then
        ./bin/golangci-lint run --timeout 15m --config .golangci.yaml
        return $?
    fi

    ./bin/golangci-lint run --timeout 15m --config .golangci.yaml >"$output_file" 2>&1
    local result=$?

    if [ $result -eq 0 ]; then
        print_pass "Lint passed"
    else
        print_section "Lint Failures"
        # golangci-lint already provides concise output
        cat "$output_file"
        print_fail "Lint failed"
    fi

    return $result
}

# Run migration integrity check
run_migration_check() {
    local output_file="$TMPDIR/migration.out"

    if [ "$VERBOSE" = "1" ]; then
        ./scripts/check-migration-integrity.sh
        return $?
    fi

    ./scripts/check-migration-integrity.sh >"$output_file" 2>&1
    local result=$?

    if [ $result -eq 0 ]; then
        print_pass "Migration integrity check passed"
    else
        print_section "Migration Integrity Check Failures"
        cat "$output_file"
        print_fail "Migration integrity check failed"
    fi

    return $result
}

# Main execution
main() {
    echo "Running pre-commit checks..."
    echo ""

    # Run each check and track failures
    run_tests || EXIT_CODE=1
    run_license_check || EXIT_CODE=1
    run_gofmt || EXIT_CODE=1
    run_lint || EXIT_CODE=1
    run_migration_check || EXIT_CODE=1

    echo ""
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}All checks passed!${NC}"
    else
        echo -e "${RED}Some checks failed. See details above.${NC}"
    fi

    if [ "$VERBOSE" != "1" ]; then
        echo ""
        echo "For verbose output, run: make check-verbose"
    fi

    return $EXIT_CODE
}

main
