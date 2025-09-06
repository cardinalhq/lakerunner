# Viper configuration migration

This document outlines the steps to migrate all environment variable based configuration to the central `config` package backed by [Viper](https://github.com/spf13/viper).

## Current state

- `config.Load()` initializes Viper with the prefix `LAKERUNNER` and unmarshals into a top level `config.Config` struct.
- Only the `internal/fly` package currently defines a `Config` struct and is loaded via the central loader.
- Many other packages still read from `os.Getenv` or helper functions.

## Migration plan

1. **Inventory existing env vars**
   - `rg os.Getenv` to list all direct environment lookups.
   - Identify helper packages like `internal/helpers/envar.go` that wrap `os.Getenv`.

2. **Define per–package Config types**
   - For each package that owns configuration, create a `Config` struct in that package with defaults and `mapstructure` tags.
   - Example:
     ```go
     package pubsub

     type Config struct {
         Endpoint string `mapstructure:"endpoint"`
     }

     func DefaultConfig() *Config { return &Config{Endpoint: ""} }
     ```

3. **Extend the central loader**
   - Add fields to `config.Config` for each new package and initialize them in `Load()` with defaults.
   - Use `viper.Sub("<pkg>").Unmarshal(&cfg.<Pkg>)` to populate each sub-config.

4. **Replace `os.Getenv` usage**
   - Remove direct environment parsing in packages.
   - Update constructors to accept the package `Config` and plumb the loaded values from `main()` or other composition roots.

5. **Drop legacy helpers**
   - Delete helpers like `internal/helpers/envar.go` once all lookups go through Viper.

6. **Standardize env variable names**
   - All keys use the prefix `LAKERUNNER` and dots translate to underscores: `query.timeout` → `LAKERUNNER_QUERY_TIMEOUT`.
   - Document these names in `docs/configuration.md`.

7. **Test coverage**
   - For each package, add tests verifying that Viper defaults load and that env vars override correctly.
   - Maintain existing behaviour by comparing old env names to new ones where appropriate.

## Rollout

- Convert packages incrementally to keep changes reviewable.
- After migration, remove deprecated env vars and document the final configuration surface.

