// Copyright (C) 2025-2026 CardinalHQ, Inc
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

package adminclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"github.com/cardinalhq/lakerunner/adminproto"
)

var (
	apiKey        string
	endpoint      string
	insecureMode  bool
	tlsSkipVerify bool
	tlsCACert     string
)

// SetAPIKey configures the API key used for auth with the admin service.
func SetAPIKey(key string) {
	apiKey = key
}

// SetConnectionConfig configures the endpoint and TLS settings.
func SetConnectionConfig(ep string, insec, skipVerify bool, caCert string) {
	endpoint = ep
	insecureMode = insec
	tlsSkipVerify = skipVerify
	tlsCACert = caCert
}

// CreateClient creates a gRPC client for the admin service with TLS support.
func CreateClient() (adminproto.AdminServiceClient, func(), error) {
	var opts []grpc.DialOption

	if insecureMode {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if tlsSkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}

		if tlsCACert != "" {
			caCert, err := os.ReadFile(tlsCACert)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to read CA certificate: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return nil, nil, fmt.Errorf("failed to parse CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	conn, err := grpc.NewClient(endpoint, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to admin service: %w", err)
	}

	cleanup := func() { _ = conn.Close() }
	client := adminproto.NewAdminServiceClient(conn)
	return client, cleanup, nil
}

// AttachAPIKey adds the API key to the context for gRPC calls.
func AttachAPIKey(ctx context.Context) context.Context {
	if apiKey != "" {
		md := metadata.New(map[string]string{"authorization": "Bearer " + apiKey})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}
