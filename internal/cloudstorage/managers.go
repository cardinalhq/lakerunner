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

package cloudstorage

import (
	"context"
	"fmt"

	"github.com/cardinalhq/lakerunner/internal/awsclient"
	"github.com/cardinalhq/lakerunner/internal/azureclient"
)

// CloudManagers holds all cloud provider managers for unified access
type CloudManagers struct {
	AWS   *awsclient.Manager
	Azure *azureclient.Manager
}

// NewCloudManagers creates managers for all supported cloud providers
func NewCloudManagers(ctx context.Context) (*CloudManagers, error) {
	// Create AWS manager - required for S3-compatible storage (AWS, GCP)
	awsManager, err := awsclient.NewManager(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create AWS manager: %w", err)
	}

	// Create Azure manager
	azureManager, err := azureclient.NewManager(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure manager: %w", err)
	}

	return &CloudManagers{
		AWS:   awsManager,
		Azure: azureManager,
	}, nil
}
