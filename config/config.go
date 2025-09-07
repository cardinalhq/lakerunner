// Copyright (C) 2025 CardinalHQ, Inc
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

package config

import (
	"os"
	"reflect"
	"strings"

	"github.com/spf13/viper"

	"github.com/cardinalhq/lakerunner/internal/fly"
	ingestion "github.com/cardinalhq/lakerunner/internal/metricsprocessing/ingestion"
)

// Config aggregates configuration for the application.
// Each field is owned by its respective package.
type Config struct {
	Debug   bool          `mapstructure:"debug"`
	Fly     fly.Config    `mapstructure:"fly"`
	Metrics MetricsConfig `mapstructure:"metrics"`
	Batch   BatchConfig   `mapstructure:"batch"`
	DuckDB  DuckDBConfig  `mapstructure:"duckdb"`
	Logs    LogsConfig    `mapstructure:"logs"`
	Traces  TracesConfig  `mapstructure:"traces"`
	Admin   AdminConfig   `mapstructure:"admin"`
}

type MetricsConfig struct {
	Ingestion  ingestion.Config `mapstructure:"ingestion"`
	Compaction CompactionConfig `mapstructure:"compaction"`
	Rollup     RollupConfig     `mapstructure:"rollup"`
}

type BatchConfig struct {
	TargetSizeBytes int64 `mapstructure:"target_size_bytes"`
	MaxBatchSize    int   `mapstructure:"max_batch_size"`
	MaxTotalSize    int64 `mapstructure:"max_total_size"`
	MaxAgeSeconds   int   `mapstructure:"max_age_seconds"`
	MinBatchSize    int   `mapstructure:"min_batch_size"`
}

type DuckDBConfig struct {
	ExtensionsPath  string `mapstructure:"extensions_path"`
	HTTPFSExtension string `mapstructure:"httpfs_extension"`
}

type LogsConfig struct {
	Partitions int `mapstructure:"partitions"`
}

type TracesConfig struct {
	Partitions int `mapstructure:"partitions"`
}

type AdminConfig struct {
	InitialAPIKey string `mapstructure:"initial_api_key"`
}

type CompactionConfig struct {
	OverFactor   float64 `mapstructure:"over_factor"`
	BatchLimit   int     `mapstructure:"batch_limit"`
	GraceMinutes int     `mapstructure:"grace_minutes"`
	DeferSeconds int     `mapstructure:"defer_seconds"`
	MaxAttempts  int     `mapstructure:"max_attempts"`
}

type RollupConfig struct {
	BatchLimit int `mapstructure:"batch_limit"`
}

// Load reads configuration from files and environment variables.
// Environment variables use the prefix "LAKERUNNER" and the dot character
// in keys is replaced by an underscore. For example, "fly.brokers" becomes
// "LAKERUNNER_FLY_BROKERS".
func Load() (*Config, error) {
	cfg := &Config{
		Debug: false,
		Fly:   *fly.DefaultConfig(),
		Metrics: MetricsConfig{
			Ingestion: ingestion.DefaultConfig(),
			Compaction: CompactionConfig{
				OverFactor:   2.0,
				BatchLimit:   100,
				GraceMinutes: 5,
				DeferSeconds: 0,
				MaxAttempts:  3,
			},
			Rollup: RollupConfig{
				BatchLimit: 100,
			},
		},
		Batch: BatchConfig{
			TargetSizeBytes: 100 * 1024 * 1024, // 100MB
			MaxBatchSize:    100,
			MaxTotalSize:    1024 * 1024 * 1024, // 1GB
			MaxAgeSeconds:   300,                // 5 minutes
			MinBatchSize:    1,
		},
		DuckDB: DuckDBConfig{
			ExtensionsPath:  "",
			HTTPFSExtension: "",
		},
		Logs: LogsConfig{
			Partitions: 128,
		},
		Traces: TracesConfig{
			Partitions: 128,
		},
		Admin: AdminConfig{
			InitialAPIKey: "",
		},
	}

	v := viper.New()
	v.SetConfigName("config")
	v.AddConfigPath(".")
	v.SetEnvPrefix("LAKERUNNER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	bindEnvs(v, cfg)
	_ = v.ReadInConfig()

	if err := v.Unmarshal(cfg); err != nil {
		return nil, err
	}
	if b := v.GetString("fly.brokers"); b != "" {
		cfg.Fly.Brokers = strings.Split(b, ",")
	}

	// Also check DEBUG environment variable (without prefix)
	if os.Getenv("DEBUG") != "" {
		cfg.Debug = true
	}

	return cfg, nil
}

// bindEnvs registers all keys within cfg so that viper will look up
// corresponding environment variables when unmarshalling.
func bindEnvs(v *viper.Viper, cfg any, parts ...string) {
	val := reflect.ValueOf(cfg)
	typ := reflect.TypeOf(cfg)
	if typ.Kind() == reflect.Pointer {
		val = val.Elem()
		typ = typ.Elem()
	}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := f.Tag.Get("mapstructure")
		if tag == "" {
			tag = strings.ToLower(f.Name)
		}
		key := append(parts, tag)
		if f.Type.Kind() == reflect.Struct {
			bindEnvs(v, val.Field(i).Interface(), key...)
			continue
		}
		_ = v.BindEnv(strings.Join(key, "."))
	}
}
