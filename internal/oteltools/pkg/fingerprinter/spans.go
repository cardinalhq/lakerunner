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

	"github.com/cespare/xxhash/v2"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"

	"github.com/cardinalhq/lakerunner/pipeline"
	"github.com/cardinalhq/lakerunner/pipeline/wkk"
)

const (
	serviceNameKey   = string(semconv.ServiceNameKey)
	clusterNameKey   = string(semconv.K8SClusterNameKey)
	namespaceNameKey = string(semconv.K8SNamespaceNameKey)
)

func getFromResource(attr pcommon.Map, key string) string {
	clusterVal, clusterFound := attr.Get(key)
	if !clusterFound {
		return "unknown"
	}
	return clusterVal.AsString()
}

func getStringAttribute(sr ptrace.Span, key string) string {
	attrValue, found := sr.Attributes().Get(key)
	if !found {
		return ""
	}
	return attrValue.AsString()
}

func CalculateSpanFingerprint(res pcommon.Resource, sr ptrace.Span) int64 {
	fingerprintAttributes := make([]string, 0)
	clusterName := getFromResource(res.Attributes(), clusterNameKey)
	namespaceName := getFromResource(res.Attributes(), namespaceNameKey)
	serviceName := getFromResource(res.Attributes(), serviceNameKey)
	spanKindStr := sr.Kind().String()
	fingerprintAttributes = append(fingerprintAttributes, clusterName, namespaceName, serviceName, spanKindStr)

	dbSystem := getStringAttribute(sr, string(semconv.DBSystemNameKey))
	messagingSystem := getStringAttribute(sr, string(semconv.MessagingSystemKey))
	httpRequestMethod := getStringAttribute(sr, string(semconv.HTTPRequestMethodKey))

	if messagingSystem != "" {
		messagingOperationType := getStringAttribute(sr, string(semconv.MessagingOperationTypeKey))
		messagingDestinationName := getStringAttribute(sr, string(semconv.MessagingDestinationNameKey))
		fingerprintAttributes = append(fingerprintAttributes, messagingSystem, messagingOperationType, messagingDestinationName)
		return toHash(fingerprintAttributes)
	}
	if dbSystem != "" {
		dbNamespace := getStringAttribute(sr, string(semconv.DBNamespaceKey))
		dbOperationName := getStringAttribute(sr, string(semconv.DBOperationNameKey))
		serverAddress := getStringAttribute(sr, string(semconv.ServerAddressKey))
		collectionName := getStringAttribute(sr, string(semconv.DBCollectionNameKey))
		fingerprintAttributes = append(fingerprintAttributes, sr.Name(), dbSystem, dbNamespace, dbOperationName, serverAddress, collectionName)
		return toHash(fingerprintAttributes)
	}
	if httpRequestMethod != "" {
		httpUrlTemplate := getStringAttribute(sr, string(semconv.URLTemplateKey))
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

// CalculateSpanFingerprintFromRow calculates a span fingerprint from a row.
// The row should contain resource attributes and span attributes with appropriate prefixes.
func CalculateSpanFingerprintFromRow(row pipeline.Row) int64 {
	fingerprintAttributes := make([]string, 0)

	// Extract resource-level attributes
	clusterName := row.GetString(wkk.RowKeyResourceK8sClusterName)
	if clusterName == "" {
		clusterName = "unknown"
	}
	namespaceName := row.GetString(wkk.RowKeyResourceK8sNamespaceName)
	if namespaceName == "" {
		namespaceName = "unknown"
	}
	serviceName := row.GetString(wkk.RowKeyResourceServiceName)
	if serviceName == "" {
		serviceName = "unknown"
	}

	// Extract span kind
	spanKindStr := row.GetString(wkk.RowKeySpanKind)

	fingerprintAttributes = append(fingerprintAttributes, clusterName, namespaceName, serviceName, spanKindStr)

	// Check messaging system first
	messagingSystem := row.GetString(wkk.RowKeyAttrMessagingSystem)
	if messagingSystem != "" {
		messagingOperationType := row.GetString(wkk.RowKeyAttrMessagingOperationType)
		messagingDestinationName := row.GetString(wkk.RowKeyAttrMessagingDestinationName)
		fingerprintAttributes = append(fingerprintAttributes, messagingSystem, messagingOperationType, messagingDestinationName)
		return toHash(fingerprintAttributes)
	}

	// Check database system
	dbSystem := row.GetString(wkk.RowKeyAttrDBSystemName)
	if dbSystem != "" {
		dbNamespace := row.GetString(wkk.RowKeyAttrDBNamespace)
		dbOperationName := row.GetString(wkk.RowKeyAttrDBOperationName)
		serverAddress := row.GetString(wkk.RowKeyAttrServerAddress)
		collectionName := row.GetString(wkk.RowKeyAttrDBCollectionName)
		spanName := row.GetString(wkk.RowKeySpanName)

		fingerprintAttributes = append(fingerprintAttributes, spanName, dbSystem, dbNamespace, dbOperationName, serverAddress, collectionName)
		return toHash(fingerprintAttributes)
	}

	// Check HTTP request method
	httpRequestMethod := row.GetString(wkk.RowKeyAttrHTTPRequestMethod)
	if httpRequestMethod != "" {
		httpUrlTemplate := row.GetString(wkk.RowKeyAttrURLTemplate)
		fingerprintAttributes = append(fingerprintAttributes, httpRequestMethod, httpUrlTemplate)
		return toHash(fingerprintAttributes)
	}

	// Default: use span name
	sanitizedName := row.GetString(wkk.RowKeySpanName)
	fingerprintAttributes = append(fingerprintAttributes, sanitizedName)

	return toHash(fingerprintAttributes)
}
