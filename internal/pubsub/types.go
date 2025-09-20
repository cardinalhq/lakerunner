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

package pubsub

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// Service defines the interface for pubsub services
type Service interface {
	Run(ctx context.Context) error
}

// BackendType represents supported pubsub backend types
type BackendType string

const (
	BackendTypeSQS       BackendType = "sqs"
	BackendTypeGCPPubSub BackendType = "gcp"
	BackendTypeAzure     BackendType = "azure"
)

// Backend defines the interface for different pubsub backends
type Backend interface {
	Service
	GetName() string
}

// IngestItem represents a work item for ingestion processing.
type IngestItem struct {
	OrganizationID uuid.UUID `json:"organization_id"`
	InstanceNum    int16     `json:"instance_num"`
	Bucket         string    `json:"bucket"`
	ObjectID       string    `json:"object_id"`
	Signal         string    `json:"signal"`
	FileSize       int64     `json:"file_size"`
	QueuedAt       time.Time `json:"queued_at"`
	CollectorName  string    `json:"collector_name"`
}
