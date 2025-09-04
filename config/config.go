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
	"reflect"
	"strings"

	"github.com/spf13/viper"

	"github.com/cardinalhq/lakerunner/internal/fly"
	ingestion "github.com/cardinalhq/lakerunner/internal/metricsprocessing/ingestion"
)

// Config aggregates configuration for the application.
// Each field is owned by its respective package.
type Config struct {
	Fly     fly.Config    `mapstructure:"fly"`
	Metrics MetricsConfig `mapstructure:"metrics"`
}

type MetricsConfig struct {
	Ingestion ingestion.Config `mapstructure:"ingestion"`
}

// Load reads configuration from files and environment variables.
// Environment variables use the prefix "LAKERUNNER" and the dot character
// in keys is replaced by an underscore. For example, "fly.brokers" becomes
// "LAKERUNNER_FLY_BROKERS".
func Load() (*Config, error) {
	cfg := &Config{
		Fly: *fly.DefaultConfig(),
		Metrics: MetricsConfig{
			Ingestion: ingestion.DefaultConfig(),
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
	cfg.Fly.Enabled = v.GetBool("fly.enabled")
	return cfg, nil
}

// bindEnvs registers all keys within cfg so that viper will look up
// corresponding environment variables when unmarshalling.
func bindEnvs(v *viper.Viper, cfg any, parts ...string) {
	val := reflect.ValueOf(cfg)
	typ := reflect.TypeOf(cfg)
	if typ.Kind() == reflect.Ptr {
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
