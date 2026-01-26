//go:build integration

// Copyright (C) 2025-2026 CardinalHQ, Inc
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, version 3.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

package testhelpers

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/cardinalhq/lakerunner/configdb"
	configdbmigrations "github.com/cardinalhq/lakerunner/configdb/migrations"
	"github.com/cardinalhq/lakerunner/lrdb"
	lrdbmigrations "github.com/cardinalhq/lakerunner/lrdb/migrations"
)

// SetupTestLRDB creates a clean test lrdb database with migrations applied.
// Returns a connection pool and registers cleanup with t.Cleanup.
func SetupTestLRDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()
	dbName := fmt.Sprintf("test_lrdb_%d_%d", time.Now().Unix(), rand.Intn(10000))

	// Get connection details from environment
	host := getEnvOrDefault("LRDB_HOST", "localhost")
	port := getEnvOrDefault("LRDB_PORT", "5432")
	user := getEnvOrDefault("LRDB_USER", os.Getenv("USER"))
	baseDB := getEnvOrDefault("LRDB_DBNAME", "testing_lrdb")

	// Connect to base database to create test database
	password := os.Getenv("LRDB_PASSWORD")
	var baseConnStr string
	if password != "" {
		baseConnStr = fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, baseDB)
	} else {
		baseConnStr = fmt.Sprintf("postgresql://%s@%s:%s/%s", user, host, port, baseDB)
	}
	basePool, err := pgxpool.New(ctx, baseConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to base database: %v", err)
	}

	// Create test database
	_, err = basePool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create test database %s: %v", dbName, err)
	}

	// Connect to test database
	var testConnStr string
	if password != "" {
		testConnStr = fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)
	} else {
		testConnStr = fmt.Sprintf("postgresql://%s@%s:%s/%s", user, host, port, dbName)
	}
	testPool, err := pgxpool.New(ctx, testConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	// Run migrations
	err = lrdbmigrations.RunMigrationsUp(ctx, testPool)
	if err != nil {
		testPool.Close()
		t.Fatalf("Failed to run lrdb migrations: %v", err)
	}

	// Register cleanup
	t.Cleanup(func() {
		testPool.Close()

		// Drop test database
		_, err := basePool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			slog.Error("Failed to drop test database", slog.String("dbName", dbName), slog.Any("error", err))
		}

		// Close base pool after cleanup
		basePool.Close()
	})

	return testPool
}

// SetupTestConfigDB creates a clean test configdb database with migrations applied.
// Returns a connection pool and registers cleanup with t.Cleanup.
func SetupTestConfigDB(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()
	dbName := fmt.Sprintf("test_configdb_%d_%d", time.Now().Unix(), rand.Intn(10000))

	// Get connection details from environment
	host := getEnvOrDefault("CONFIGDB_HOST", "localhost")
	port := getEnvOrDefault("CONFIGDB_PORT", "5432")
	user := getEnvOrDefault("CONFIGDB_USER", os.Getenv("USER"))
	baseDB := getEnvOrDefault("CONFIGDB_DBNAME", "testing_configdb")

	// Connect to base database to create test database
	password := os.Getenv("CONFIGDB_PASSWORD")
	var baseConnStr string
	if password != "" {
		baseConnStr = fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, baseDB)
	} else {
		baseConnStr = fmt.Sprintf("postgresql://%s@%s:%s/%s", user, host, port, baseDB)
	}
	basePool, err := pgxpool.New(ctx, baseConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to base configdb: %v", err)
	}

	// Create test database
	_, err = basePool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s", dbName))
	if err != nil {
		t.Fatalf("Failed to create test configdb %s: %v", dbName, err)
	}

	// Connect to test database
	var testConnStr string
	if password != "" {
		testConnStr = fmt.Sprintf("postgresql://%s:%s@%s:%s/%s?sslmode=disable", user, password, host, port, dbName)
	} else {
		testConnStr = fmt.Sprintf("postgresql://%s@%s:%s/%s", user, host, port, dbName)
	}
	testPool, err := pgxpool.New(ctx, testConnStr)
	if err != nil {
		t.Fatalf("Failed to connect to test configdb: %v", err)
	}

	// Run migrations
	err = configdbmigrations.RunMigrationsUp(ctx, testPool)
	if err != nil {
		testPool.Close()
		t.Fatalf("Failed to run configdb migrations: %v", err)
	}

	// Register cleanup
	t.Cleanup(func() {
		testPool.Close()

		// Drop test database
		_, err := basePool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))
		if err != nil {
			slog.Error("Failed to drop test configdb", slog.String("dbName", dbName), slog.Any("error", err))
		}

		// Close base pool after cleanup
		basePool.Close()
	})

	return testPool
}

// NewTestLRDBStore creates a new lrdb store connected to a test database.
func NewTestLRDBStore(t *testing.T) lrdb.StoreFull {
	pool := SetupTestLRDB(t)
	store := lrdb.NewStore(pool)
	t.Cleanup(func() {
		store.Close()
	})
	return store
}

// NewTestConfigDBStore creates a new configdb store connected to a test database.
func NewTestConfigDBStore(t *testing.T) configdb.QuerierFull {
	pool := SetupTestConfigDB(t)
	return configdb.NewStore(pool)
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
