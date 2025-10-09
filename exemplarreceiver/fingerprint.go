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

package exemplarreceiver

import (
	"strings"

	"github.com/cespare/xxhash/v2"
)

// computeTracesFingerprint calculates a fingerprint for traces based on incoming JSON data.
// This follows the same logic as fingerprinter.CalculateSpanFingerprintFromRow but works with map[string]any.
func computeTracesFingerprint(data map[string]any) int64 {
	fingerprintAttributes := make([]string, 0)

	// Extract resource-level attributes
	clusterName := getStringFromMap(data, "cluster_name")
	if clusterName == "" {
		clusterName = "unknown"
	}
	namespaceName := getStringFromMap(data, "namespace")
	if namespaceName == "" {
		namespaceName = "unknown"
	}
	serviceName := getStringFromMap(data, "service_name")
	if serviceName == "" {
		serviceName = "unknown"
	}

	// Extract span kind (string or int32)
	spanKindStr := getStringFromMap(data, "span_kind")
	if spanKindStr == "" {
		// Try to get as int32 and convert
		spanKind := getInt32FromMap(data, "span_kind")
		spanKindStr = spanKindToString(spanKind)
	}

	fingerprintAttributes = append(fingerprintAttributes, clusterName, namespaceName, serviceName, spanKindStr)

	// Check messaging system first
	messagingSystem := getStringFromMap(data, "messaging_system")
	if messagingSystem != "" {
		messagingOperationType := getStringFromMap(data, "messaging_operation_type")
		messagingDestinationName := getStringFromMap(data, "messaging_destination_name")
		fingerprintAttributes = append(fingerprintAttributes, messagingSystem, messagingOperationType, messagingDestinationName)
		return toHash(fingerprintAttributes)
	}

	// Check database system
	dbSystem := getStringFromMap(data, "db_system")
	if dbSystem != "" {
		dbNamespace := getStringFromMap(data, "db_namespace")
		dbOperationName := getStringFromMap(data, "db_operation_name")
		serverAddress := getStringFromMap(data, "server_address")
		collectionName := getStringFromMap(data, "db_collection_name")
		spanName := getStringFromMap(data, "span_name")

		fingerprintAttributes = append(fingerprintAttributes, spanName, dbSystem, dbNamespace, dbOperationName, serverAddress, collectionName)
		return toHash(fingerprintAttributes)
	}

	// Check HTTP request method
	httpRequestMethod := getStringFromMap(data, "http_request_method")
	if httpRequestMethod != "" {
		httpUrlTemplate := getStringFromMap(data, "url_template")
		fingerprintAttributes = append(fingerprintAttributes, httpRequestMethod, httpUrlTemplate)
		return toHash(fingerprintAttributes)
	}

	// Default: use span name
	sanitizedName := getStringFromMap(data, "span_name")
	fingerprintAttributes = append(fingerprintAttributes, sanitizedName)

	return toHash(fingerprintAttributes)
}

// computeLogsFingerprint calculates a simple fingerprint for logs based on the message.
// For more sophisticated fingerprinting (clustering), the client should send the fingerprint.
func computeLogsFingerprint(data map[string]any) int64 {
	// Get the log message
	message := getStringFromMap(data, "message")
	if message == "" {
		message = getStringFromMap(data, "body")
	}

	// Include service context in fingerprint
	clusterName := getStringFromMap(data, "cluster_name")
	if clusterName == "" {
		clusterName = "unknown"
	}
	namespaceName := getStringFromMap(data, "namespace")
	if namespaceName == "" {
		namespaceName = "unknown"
	}
	serviceName := getStringFromMap(data, "service_name")
	if serviceName == "" {
		serviceName = "unknown"
	}

	// Create fingerprint from service context + message
	fingerprintAttributes := []string{clusterName, namespaceName, serviceName, message}
	return toHash(fingerprintAttributes)
}

// toHash converts a slice of strings into an int64 hash
func toHash(attributes []string) int64 {
	return int64(xxhash.Sum64String(strings.Join(attributes, "##")))
}

// spanKindToString converts a span kind integer to its string representation
func spanKindToString(kind int32) string {
	switch kind {
	case 0:
		return "SPAN_KIND_UNSPECIFIED"
	case 1:
		return "SPAN_KIND_INTERNAL"
	case 2:
		return "SPAN_KIND_SERVER"
	case 3:
		return "SPAN_KIND_CLIENT"
	case 4:
		return "SPAN_KIND_PRODUCER"
	case 5:
		return "SPAN_KIND_CONSUMER"
	default:
		return "SPAN_KIND_UNSPECIFIED"
	}
}
