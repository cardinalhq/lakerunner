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

package metricsprocessing

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"time"

	"github.com/cardinalhq/lakerunner/config"
	"github.com/cardinalhq/lakerunner/internal/cloudstorage"
	"github.com/cardinalhq/lakerunner/internal/fly/messages"
	"github.com/cardinalhq/lakerunner/internal/logctx"
	"github.com/cardinalhq/lakerunner/internal/storageprofile"
)

// CommonProcessorValidationError represents an error when messages in a group have inconsistent fields
type CommonProcessorValidationError struct {
	Field    string
	Expected any
	Got      any
	Message  string
}

func (e *CommonProcessorValidationError) Error() string {
	return fmt.Sprintf("common processor group validation failed - %s: expected %v, got %v (%s)", e.Field, e.Expected, e.Got, e.Message)
}

// CommonProcessorBase provides common functionality for all message processors
type CommonProcessorBase[M messages.GroupableMessage, K messages.CompactionKeyInterface] struct {
	storageProvider storageprofile.StorageProfileProvider
	cmgr            cloudstorage.ClientProvider
	cfg             *config.Config
}

// NewCommonProcessorBase creates a new base processing processor
func NewCommonProcessorBase[M messages.GroupableMessage, K messages.CompactionKeyInterface](
	storageProvider storageprofile.StorageProfileProvider,
	cmgr cloudstorage.ClientProvider,
	cfg *config.Config,
) *CommonProcessorBase[M, K] {
	return &CommonProcessorBase[M, K]{
		storageProvider: storageProvider,
		cmgr:            cmgr,
		cfg:             cfg,
	}
}

// ValidateGroupConsistency performs generic validation for processing groups
func (b *CommonProcessorBase[M, K]) ValidateGroupConsistency(
	group *accumulationGroup[K],
	expectedFields map[string]any,
) error {
	if len(group.Messages) == 0 {
		return &CommonProcessorValidationError{
			Field:   "message_count",
			Message: "group cannot be empty",
		}
	}

	// Validate each message against the expected values
	for i, accMsg := range group.Messages {
		// Use reflection to extract fields from the message
		msgValue := reflect.ValueOf(accMsg.Message)
		if msgValue.Kind() == reflect.Ptr {
			msgValue = msgValue.Elem()
		}

		for fieldName, expectedValue := range expectedFields {
			field := msgValue.FieldByName(fieldName)
			if !field.IsValid() {
				return &CommonProcessorValidationError{
					Field:   fieldName,
					Message: fmt.Sprintf("message %d missing field %s", i, fieldName),
				}
			}

			actualValue := field.Interface()
			if actualValue != expectedValue {
				return &CommonProcessorValidationError{
					Field:    fieldName,
					Expected: expectedValue,
					Got:      actualValue,
					Message:  fmt.Sprintf("message %d has inconsistent %s", i, fieldName),
				}
			}
		}
	}

	return nil
}

// LogCompactionStart logs the start of a processing operation with common fields
func (b *CommonProcessorBase[M, K]) LogCompactionStart(
	ctx context.Context,
	group *accumulationGroup[K],
	signalType string,
	extraFields ...slog.Attr,
) {
	ll := logctx.FromContext(ctx)

	groupAge := time.Since(group.CreatedAt)

	// Base log fields
	fields := []slog.Attr{
		slog.String("signal_type", signalType),
		slog.Int("messageCount", len(group.Messages)),
		slog.Duration("groupAge", groupAge),
	}

	// Add any extra fields
	fields = append(fields, extraFields...)

	args := []any{"Starting processing"}
	for _, field := range fields {
		args = append(args, field)
	}
	ll.Info(args[0].(string), args[1:]...)
}

// ExtractKeyFields extracts common fields from a grouping key using reflection
func (b *CommonProcessorBase[M, K]) ExtractKeyFields(key K) map[string]any {
	fields := make(map[string]any)

	keyValue := reflect.ValueOf(key)
	keyType := reflect.TypeOf(key)

	for i := 0; i < keyValue.NumField(); i++ {
		field := keyValue.Field(i)
		fieldType := keyType.Field(i)
		fields[fieldType.Name] = field.Interface()
	}

	return fields
}

// GetStorageProfileAndClient gets the storage profile and client for a key
func (b *CommonProcessorBase[M, K]) GetStorageProfileAndClient(ctx context.Context, key K) (storageprofile.StorageProfile, cloudstorage.Client, error) {
	orgUUID := key.GetOrgID()
	instNum := key.GetInstanceNum()

	storageProfile, err := b.storageProvider.GetStorageProfileForOrganizationAndInstance(ctx, orgUUID, instNum)
	if err != nil {
		return storageprofile.StorageProfile{}, nil, fmt.Errorf("get storage profile: %w", err)
	}

	storageClient, err := cloudstorage.NewClient(ctx, b.cmgr, storageProfile)
	if err != nil {
		return storageprofile.StorageProfile{}, nil, fmt.Errorf("create storage client: %w", err)
	}

	return storageProfile, storageClient, nil
}

// BuildExpectedFieldsFromKey builds the expected fields map from a grouping key
func (b *CommonProcessorBase[M, K]) BuildExpectedFieldsFromKey(key K) map[string]any {
	return b.ExtractKeyFields(key)
}

// ProcessCore provides the common processing workflow
func (b *CommonProcessorBase[M, K]) ProcessCore(
	ctx context.Context,
	group *accumulationGroup[K],
	kafkaCommitData *KafkaCommitData,
	processor CommonProcessor[M, K],
) error {
	ll := logctx.FromContext(ctx)

	// Validate group consistency
	expectedFields := b.BuildExpectedFieldsFromKey(group.Key)
	if err := b.ValidateGroupConsistency(group, expectedFields); err != nil {
		return fmt.Errorf("group validation failed: %w", err)
	}

	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "")
	if err != nil {
		return fmt.Errorf("create temporary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(tmpDir); cleanupErr != nil {
			ll.Warn("Failed to cleanup temporary directory", slog.String("tmpDir", tmpDir), slog.Any("error", cleanupErr))
		}
	}()

	// Get storage profile and client
	storageProfile, storageClient, err := b.GetStorageProfileAndClient(ctx, group.Key)
	if err != nil {
		return fmt.Errorf("get storage profile and client: %w", err)
	}

	// Delegate to processor-specific logic
	return processor.ProcessWork(ctx, tmpDir, storageClient, storageProfile, group, kafkaCommitData)
}

// CommonProcessor defines the interface for processor-specific logic
type CommonProcessor[M messages.GroupableMessage, K messages.CompactionKeyInterface] interface {
	// ProcessWork handles the processor-specific work logic
	ProcessWork(
		ctx context.Context,
		tmpDir string,
		storageClient cloudstorage.Client,
		storageProfile storageprofile.StorageProfile,
		group *accumulationGroup[K],
		kafkaCommitData *KafkaCommitData,
	) error

	// GetTargetRecordCount returns the target record count for a grouping key
	GetTargetRecordCount(ctx context.Context, groupingKey K) int64
}
