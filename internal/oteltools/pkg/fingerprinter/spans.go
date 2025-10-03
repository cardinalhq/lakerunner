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

package fingerprinter

import (
	"strings"

	"github.com/cardinalhq/oteltools/hashutils"
	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"

	"github.com/cardinalhq/lakerunner/internal/oteltools/pkg/translate"
	"github.com/cardinalhq/lakerunner/internal/pipeline/wkk"
)

const (
	serviceNameKey   = string(semconv.ServiceNameKey)
	clusterNameKey   = string(semconv.K8SClusterNameKey)
	namespaceNameKey = string(semconv.K8SNamespaceNameKey)
)

var (
	// RowKey constants for span fingerprinting
	rowKeyResourceClusterName   = wkk.NewRowKey("resource_k8s_cluster_name")
	rowKeyResourceNamespace     = wkk.NewRowKey("resource_k8s_namespace_name")
	rowKeyResourceServiceName   = wkk.NewRowKey("resource_service_name")
	rowKeySpanKind              = wkk.NewRowKey("span_kind")
	rowKeySpanName              = wkk.NewRowKey("span_name")
	rowKeyAttrDBSystemName      = wkk.NewRowKey("attr_db_system_name")
	rowKeyAttrMessagingSystem   = wkk.NewRowKey("attr_messaging_system")
	rowKeyAttrHTTPRequestMethod = wkk.NewRowKey("attr_http_request_method")
	rowKeyAttrMessagingOpType   = wkk.NewRowKey("attr_messaging_operation_type")
	rowKeyAttrMessagingDestName = wkk.NewRowKey("attr_messaging_destination_name")
	rowKeyAttrDBNamespace       = wkk.NewRowKey("attr_db_namespace")
	rowKeyAttrDBOperationName   = wkk.NewRowKey("attr_db_operation_name")
	rowKeyAttrServerAddress     = wkk.NewRowKey("attr_server_address")
	rowKeyAttrDBCollectionName  = wkk.NewRowKey("attr_db_collection_name")
	rowKeyAttrURLTemplate       = wkk.NewRowKey("attr_url_template")
)

func ComputeExemplarKey(rl pcommon.Resource, extraKeys []string) ([]string, int64) {
	keys := []string{
		clusterNameKey, GetFromResource(rl.Attributes(), clusterNameKey),
		namespaceNameKey, GetFromResource(rl.Attributes(), namespaceNameKey),
		serviceNameKey, GetFromResource(rl.Attributes(), serviceNameKey),
	}
	keys = append(keys, extraKeys...)
	return keys, int64(hashutils.HashStrings(nil, keys...))
}

func GetFromResource(attr pcommon.Map, key string) string {
	clusterVal, clusterFound := attr.Get(key)
	if !clusterFound {
		return "unknown"
	}
	return clusterVal.AsString()
}

func GetFingerprintAttribute(l pcommon.Map) int64 {
	fnk := translate.CardinalFieldFingerprint
	if fingerprintField, found := l.Get(fnk); found {
		return fingerprintField.Int()
	}
	return 0
}

func GetExceptionMessage(sr ptrace.Span) string {
	var exceptionMessage string
	for i := 0; i < sr.Events().Len(); i++ {
		event := sr.Events().At(i)
		if event.Name() == semconv.ExceptionEventName {
			if exType, found := event.Attributes().Get(string(semconv.ExceptionTypeKey)); found {
				exceptionMessage = exceptionMessage + exType.AsString()
			}
			if exMsg, found := event.Attributes().Get(string(semconv.ExceptionMessageKey)); found {
				if exceptionMessage != "" {
					exceptionMessage = exceptionMessage + " " + exMsg.AsString()
				} else {
					exceptionMessage = exMsg.AsString()
				}
			}
			if exStack, found := event.Attributes().Get(string(semconv.ExceptionStacktraceKey)); found {
				if exceptionMessage != "" {
					exceptionMessage = exceptionMessage + "\n" + exStack.AsString()
				} else {
					exceptionMessage = exStack.AsString()
				}
			}
			break
		}
	}
	return exceptionMessage
}

func GetStringAttribute(sr ptrace.Span, key string) string {
	attrValue, found := sr.Attributes().Get(key)
	if !found {
		return ""
	}
	return attrValue.AsString()
}

func CalculateSpanFingerprint(res pcommon.Resource, sr ptrace.Span) int64 {
	fingerprintAttributes := make([]string, 0)
	clusterName := GetFromResource(res.Attributes(), clusterNameKey)
	namespaceName := GetFromResource(res.Attributes(), namespaceNameKey)
	serviceName := GetFromResource(res.Attributes(), serviceNameKey)
	spanKindStr := sr.Kind().String()
	fingerprintAttributes = append(fingerprintAttributes, clusterName, namespaceName, serviceName, spanKindStr)

	dbSystem := GetStringAttribute(sr, string(semconv.DBSystemNameKey))
	messagingSystem := GetStringAttribute(sr, string(semconv.MessagingSystemKey))
	httpRequestMethod := GetStringAttribute(sr, string(semconv.HTTPRequestMethodKey))

	if messagingSystem != "" {
		messagingOperationType := GetStringAttribute(sr, string(semconv.MessagingOperationTypeKey))
		messagingDestinationName := GetStringAttribute(sr, string(semconv.MessagingDestinationNameKey))
		fingerprintAttributes = append(fingerprintAttributes, messagingSystem, messagingOperationType, messagingDestinationName)
		return toHash(fingerprintAttributes)
	}
	if dbSystem != "" {
		dbNamespace := GetStringAttribute(sr, string(semconv.DBNamespaceKey))
		dbOperationName := GetStringAttribute(sr, string(semconv.DBOperationNameKey))
		serverAddress := GetStringAttribute(sr, string(semconv.ServerAddressKey))
		collectionName := GetStringAttribute(sr, string(semconv.DBCollectionNameKey))
		fingerprintAttributes = append(fingerprintAttributes, sr.Name(), dbSystem, dbNamespace, dbOperationName, serverAddress, collectionName)
		return toHash(fingerprintAttributes)
	}
	if httpRequestMethod != "" {
		httpUrlTemplate := GetStringAttribute(sr, string(semconv.URLTemplateKey))
		fingerprintAttributes = append(fingerprintAttributes, httpRequestMethod, httpUrlTemplate)
		return toHash(fingerprintAttributes)
	}

	sanitizedName := sr.Name()
	fingerprintAttributes = append(fingerprintAttributes, sanitizedName)

	return toHash(fingerprintAttributes)
}

func toHash(fingerprintAttributes []string) int64 {
	return int64(xxhash.Sum64String(strings.Join(fingerprintAttributes, "##")))
}

// GetStringFromRow retrieves a string value from a row map by RowKey.
// Returns empty string if the key is not found or the value is not a string.
func GetStringFromRow(row map[wkk.RowKey]any, key wkk.RowKey) string {
	val, exists := row[key]
	if !exists {
		return ""
	}
	if str, ok := val.(string); ok {
		return str
	}
	return ""
}

// CalculateSpanFingerprintFromRow calculates a span fingerprint from a row map.
// The row should contain resource attributes and span attributes with appropriate prefixes.
func CalculateSpanFingerprintFromRow(row map[wkk.RowKey]any) int64 {
	fingerprintAttributes := make([]string, 0)

	// Extract resource-level attributes
	clusterName := GetStringFromRow(row, rowKeyResourceClusterName)
	if clusterName == "" {
		clusterName = "unknown"
	}
	namespaceName := GetStringFromRow(row, rowKeyResourceNamespace)
	if namespaceName == "" {
		namespaceName = "unknown"
	}
	serviceName := GetStringFromRow(row, rowKeyResourceServiceName)
	if serviceName == "" {
		serviceName = "unknown"
	}

	// Extract span kind
	spanKindStr := GetStringFromRow(row, rowKeySpanKind)

	fingerprintAttributes = append(fingerprintAttributes, clusterName, namespaceName, serviceName, spanKindStr)

	// Check messaging system first
	messagingSystem := GetStringFromRow(row, rowKeyAttrMessagingSystem)
	if messagingSystem != "" {
		messagingOperationType := GetStringFromRow(row, rowKeyAttrMessagingOpType)
		messagingDestinationName := GetStringFromRow(row, rowKeyAttrMessagingDestName)
		fingerprintAttributes = append(fingerprintAttributes, messagingSystem, messagingOperationType, messagingDestinationName)
		return toHash(fingerprintAttributes)
	}

	// Check database system
	dbSystem := GetStringFromRow(row, rowKeyAttrDBSystemName)
	if dbSystem != "" {
		dbNamespace := GetStringFromRow(row, rowKeyAttrDBNamespace)
		dbOperationName := GetStringFromRow(row, rowKeyAttrDBOperationName)
		serverAddress := GetStringFromRow(row, rowKeyAttrServerAddress)
		collectionName := GetStringFromRow(row, rowKeyAttrDBCollectionName)
		spanName := GetStringFromRow(row, rowKeySpanName)

		fingerprintAttributes = append(fingerprintAttributes, spanName, dbSystem, dbNamespace, dbOperationName, serverAddress, collectionName)
		return toHash(fingerprintAttributes)
	}

	// Check HTTP request method
	httpRequestMethod := GetStringFromRow(row, rowKeyAttrHTTPRequestMethod)
	if httpRequestMethod != "" {
		httpUrlTemplate := GetStringFromRow(row, rowKeyAttrURLTemplate)
		fingerprintAttributes = append(fingerprintAttributes, httpRequestMethod, httpUrlTemplate)
		return toHash(fingerprintAttributes)
	}

	// Default: use span name
	sanitizedName := GetStringFromRow(row, rowKeySpanName)
	fingerprintAttributes = append(fingerprintAttributes, sanitizedName)

	return toHash(fingerprintAttributes)
}
