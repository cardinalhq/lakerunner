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

package cloudprovider

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// LoadConfigFromEnv loads provider configuration from environment variables
func LoadConfigFromEnv() (*ProviderConfig, error) {
	// First migrate any legacy environment variables
	migrateLegacyEnvVars()

	// Determine provider type
	providerType := ProviderType(getEnvOrDefault("LAKERUNNER_PROVIDER_TYPE", "aws"))

	config := &ProviderConfig{
		Type:     providerType,
		Settings: make(map[string]string),
	}

	// Load provider-specific settings
	prefix := fmt.Sprintf("LAKERUNNER_%s_", strings.ToUpper(string(providerType)))

	for _, env := range os.Environ() {
		if strings.HasPrefix(env, prefix) {
			parts := strings.SplitN(env, "=", 2)
			if len(parts) == 2 {
				key := strings.TrimPrefix(parts[0], prefix)
				config.Settings[strings.ToLower(key)] = parts[1]
			}
		}
	}

	// Load object store configuration
	config.ObjectStore = loadObjectStoreConfig(providerType, config.Settings)

	// Load pubsub configuration (if overridden)
	config.PubSub = loadPubSubConfig()

	return config, nil
}

// loadObjectStoreConfig loads object store configuration for the given provider
func loadObjectStoreConfig(providerType ProviderType, settings map[string]string) ObjectStoreConfig {
	config := ObjectStoreConfig{
		ProviderSettings: make(map[string]any),
	}

	// Load common settings
	config.Bucket = getEnvOrDefault("LAKERUNNER_BUCKET", os.Getenv("S3_BUCKET"))
	config.Region = getEnvOrDefault("LAKERUNNER_REGION", settings["region"])
	config.Endpoint = settings["endpoint"]
	config.UsePathStyle = parseBool(settings["use_path_style"])
	config.InsecureTLS = parseBool(settings["insecure_tls"])
	config.AccessKey = settings["access_key"]
	config.SecretKey = settings["secret_key"]
	config.Role = settings["role"]

	// Provider-specific defaults
	switch providerType {
	case ProviderAWS:
		if config.Region == "" {
			config.Region = getEnvOrDefault("AWS_REGION", "us-east-1")
		}
		// Store AWS-specific settings
		if useIRSA := settings["use_irsa"]; useIRSA != "" {
			config.ProviderSettings["use_irsa"] = parseBool(useIRSA)
		}
		if roleArn := settings["role_arn"]; roleArn != "" {
			config.ProviderSettings["role_arn"] = roleArn
		}

	case ProviderGCP:
		if project := settings["project"]; project != "" {
			config.ProviderSettings["project"] = project
		}
		if useWorkloadIdentity := settings["use_workload_identity"]; useWorkloadIdentity != "" {
			config.ProviderSettings["use_workload_identity"] = parseBool(useWorkloadIdentity)
		}
		if serviceAccount := settings["service_account"]; serviceAccount != "" {
			config.ProviderSettings["service_account"] = serviceAccount
		}

	case ProviderAzure:
		if tenantID := settings["tenant_id"]; tenantID != "" {
			config.ProviderSettings["tenant_id"] = tenantID
		}
		if useManagedIdentity := settings["use_managed_identity"]; useManagedIdentity != "" {
			config.ProviderSettings["use_managed_identity"] = parseBool(useManagedIdentity)
		}
		if storageAccount := settings["storage_account"]; storageAccount != "" {
			config.ProviderSettings["storage_account"] = storageAccount
		}

	case ProviderLocal:
		basePath := settings["base_path"]
		if basePath == "" {
			basePath = "/tmp/lakerunner-dev"
		}
		config.ProviderSettings["base_path"] = basePath

		if persistData := settings["persist_data"]; persistData != "" {
			config.ProviderSettings["persist_data"] = parseBool(persistData)
		}

	}

	return config
}

// loadPubSubConfig loads pubsub configuration if overridden
func loadPubSubConfig() PubSubConfig {
	config := PubSubConfig{
		ProviderSettings: make(map[string]any),
	}

	// Check for pubsub override
	pubsubType := os.Getenv("LAKERUNNER_PUBSUB_TYPE")
	if pubsubType == "" {
		return config // Use provider default
	}

	config.Type = pubsubType

	// Load type-specific settings
	switch pubsubType {
	case "http":
		config.Endpoint = os.Getenv("LAKERUNNER_PUBSUB_HTTP_ENDPOINT")

	case "sqs":
		if queueURL := os.Getenv("LAKERUNNER_PUBSUB_SQS_QUEUE_URL"); queueURL != "" {
			config.ProviderSettings["queue_url"] = queueURL
		}

	case "gcp_pubsub":
		if project := os.Getenv("LAKERUNNER_PUBSUB_GCP_PROJECT"); project != "" {
			config.ProviderSettings["project"] = project
		}
		if subscription := os.Getenv("LAKERUNNER_PUBSUB_GCP_SUBSCRIPTION"); subscription != "" {
			config.ProviderSettings["subscription"] = subscription
		}

	case "azure_servicebus":
		if connectionString := os.Getenv("LAKERUNNER_PUBSUB_AZURE_CONNECTION_STRING"); connectionString != "" {
			config.ProviderSettings["connection_string"] = connectionString
		}
		if queueName := os.Getenv("LAKERUNNER_PUBSUB_AZURE_QUEUE_NAME"); queueName != "" {
			config.ProviderSettings["queue_name"] = queueName
		}

	case "local":
		if basePath := os.Getenv("LAKERUNNER_PUBSUB_LOCAL_BASE_PATH"); basePath != "" {
			config.ProviderSettings["base_path"] = basePath
		}
	}

	return config
}

// migrateLegacyEnvVars migrates old environment variables to new format
func migrateLegacyEnvVars() {
	migrations := map[string]string{
		"AWS_REGION":     "LAKERUNNER_AWS_REGION",
		"S3_BUCKET":      "LAKERUNNER_BUCKET",
		"AWS_ACCESS_KEY": "LAKERUNNER_S3_ACCESS_KEY",
		"AWS_SECRET_KEY": "LAKERUNNER_S3_SECRET_KEY",
	}

	for old, new := range migrations {
		if oldVal := os.Getenv(old); oldVal != "" && os.Getenv(new) == "" {
			os.Setenv(new, oldVal)
		}
	}

	// Special case: S3_PROVIDER → LAKERUNNER_PROVIDER_TYPE
	if s3Provider := os.Getenv("S3_PROVIDER"); s3Provider != "" && os.Getenv("LAKERUNNER_PROVIDER_TYPE") == "" {
		os.Setenv("LAKERUNNER_PROVIDER_TYPE", s3Provider)
	}
}

// getEnvOrDefault returns environment variable value or default
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// parseBool safely parses boolean values from strings
func parseBool(value string) bool {
	if value == "" {
		return false
	}

	b, err := strconv.ParseBool(value)
	if err != nil {
		return false
	}

	return b
}
