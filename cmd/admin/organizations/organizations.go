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

package organizations

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/google/uuid"

	"github.com/cardinalhq/lakerunner/adminproto"
	"github.com/cardinalhq/lakerunner/configdb"
)

var (
	apiKey             string
	adminEndpoint      string
	adminInsecureMode  bool
	adminTLSSkipVerify bool
	adminTLSCACert     string
	useLocal           bool

	createDisabled bool
	updateName     string
	updateEnable   bool
	updateDisable  bool
)

// SetAPIKey configures the API key used for auth with the admin service.
func SetAPIKey(key string) {
	apiKey = key
}

// SetConnectionConfig configures the endpoint and TLS settings.
func SetConnectionConfig(ep string, insecure, skipVerify bool, caCert string) {
	adminEndpoint = ep
	adminInsecureMode = insecure
	adminTLSSkipVerify = skipVerify
	adminTLSCACert = caCert
}

// GetOrganizationsCmd provides commands for managing organizations.
func GetOrganizationsCmd() *cobra.Command {
	orgCmd := &cobra.Command{
		Use:   "organizations",
		Short: "Manage organizations",
	}

	orgCmd.PersistentFlags().BoolVar(&useLocal, "local", false, "Use local database connection instead of remote admin service")

	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List organizations",
		RunE: func(_ *cobra.Command, _ []string) error {
			return runOrganizationsList()
		},
	}
	orgCmd.AddCommand(listCmd)

	createCmd := &cobra.Command{
		Use:   "create <name>",
		Short: "Create a new organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			name := args[0]
			enabled := !createDisabled
			return runOrganizationsCreate(name, enabled)
		},
	}
	createCmd.Flags().BoolVar(&createDisabled, "disabled", false, "Create organization as disabled")
	orgCmd.AddCommand(createCmd)

	updateCmd := &cobra.Command{
		Use:   "update <id>",
		Short: "Update an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			var namePtr *string
			if updateName != "" {
				namePtr = &updateName
			}
			var enabledPtr *bool
			if updateEnable && updateDisable {
				return fmt.Errorf("cannot specify both --enable and --disable")
			}
			if updateEnable {
				b := true
				enabledPtr = &b
			} else if updateDisable {
				b := false
				enabledPtr = &b
			}
			if namePtr == nil && enabledPtr == nil {
				return fmt.Errorf("no updates specified")
			}
			return runOrganizationsUpdate(id, namePtr, enabledPtr)
		},
	}
	updateCmd.Flags().StringVar(&updateName, "name", "", "New organization name")
	updateCmd.Flags().BoolVar(&updateEnable, "enable", false, "Enable the organization")
	updateCmd.Flags().BoolVar(&updateDisable, "disable", false, "Disable the organization")
	orgCmd.AddCommand(updateCmd)

	enableCmd := &cobra.Command{
		Use:   "enable <id>",
		Short: "Enable an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			b := true
			return runOrganizationsUpdate(id, nil, &b)
		},
	}
	orgCmd.AddCommand(enableCmd)

	disableCmd := &cobra.Command{
		Use:   "disable <id>",
		Short: "Disable an organization",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			b := false
			return runOrganizationsUpdate(id, nil, &b)
		},
	}
	orgCmd.AddCommand(disableCmd)

	renameCmd := &cobra.Command{
		Use:   "rename <id> <new-name>",
		Short: "Rename an organization",
		Args:  cobra.ExactArgs(2),
		RunE: func(_ *cobra.Command, args []string) error {
			id := args[0]
			name := args[1]
			return runOrganizationsUpdate(id, &name, nil)
		},
	}
	orgCmd.AddCommand(renameCmd)

	// Add new subcommands for organization resources
	orgCmd.AddCommand(getAPIKeysCmd())
	orgCmd.AddCommand(getBucketsCmd())
	orgCmd.AddCommand(getPrefixMappingsCmd())
	orgCmd.AddCommand(getBucketConfigsCmd())
	orgCmd.AddCommand(getConfigCmd())

	return orgCmd
}

func runOrganizationsList() error {
	if useLocal {
		return runLocalOrganizationsList()
	}
	return runRemoteOrganizationsList()
}

func runLocalOrganizationsList() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	orgs, err := store.ListOrganizations(ctx)
	if err != nil {
		return fmt.Errorf("failed to list organizations: %w", err)
	}

	if len(orgs) == 0 {
		fmt.Println("No organizations found")
		return nil
	}

	printOrganizationsTable(orgs)
	return nil
}

func runRemoteOrganizationsList() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	ctx = createAuthContext(ctx)

	resp, err := client.ListOrganizations(ctx, &adminproto.ListOrganizationsRequest{})
	if err != nil {
		return fmt.Errorf("failed to list organizations: %w", err)
	}

	if len(resp.Organizations) == 0 {
		fmt.Println("No organizations found")
		return nil
	}

	orgs := make([]configdb.Organization, len(resp.Organizations))
	for i, o := range resp.Organizations {
		id, _ := uuid.Parse(o.Id)
		orgs[i] = configdb.Organization{ID: id, Name: o.Name, Enabled: o.Enabled}
	}

	printOrganizationsTable(orgs)
	return nil
}

func runOrganizationsCreate(name string, enabled bool) error {
	if useLocal {
		return runLocalOrganizationsCreate(name, enabled)
	}
	return runRemoteOrganizationsCreate(name, enabled)
}

func runLocalOrganizationsCreate(name string, enabled bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	org, err := store.UpsertOrganization(ctx, configdb.UpsertOrganizationParams{ID: uuid.New(), Name: name, Enabled: enabled})
	if err != nil {
		return fmt.Errorf("failed to create organization: %w", err)
	}

	fmt.Printf("Created organization %s (%s)\n", org.Name, org.ID.String())
	return nil
}

func runRemoteOrganizationsCreate(name string, enabled bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	ctx = createAuthContext(ctx)

	resp, err := client.CreateOrganization(ctx, &adminproto.CreateOrganizationRequest{Name: name, Enabled: enabled})
	if err != nil {
		return fmt.Errorf("failed to create organization: %w", err)
	}

	fmt.Printf("Created organization %s (%s)\n", resp.Organization.Name, resp.Organization.Id)
	return nil
}

func runOrganizationsUpdate(id string, name *string, enabled *bool) error {
	if useLocal {
		return runLocalOrganizationsUpdate(id, name, enabled)
	}
	return runRemoteOrganizationsUpdate(id, name, enabled)
}

func runLocalOrganizationsUpdate(id string, name *string, enabled *bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := configdb.ConfigDBStore(ctx)
	if err != nil {
		return fmt.Errorf("failed to connect to configdb: %w", err)
	}

	orgID, err := uuid.Parse(id)
	if err != nil {
		return fmt.Errorf("invalid organization ID: %w", err)
	}

	org, err := store.GetOrganization(ctx, orgID)
	if err != nil {
		return fmt.Errorf("failed to get organization: %w", err)
	}

	if name != nil {
		org.Name = *name
	}
	if enabled != nil {
		org.Enabled = *enabled
	}

	_, err = store.UpsertOrganization(ctx, configdb.UpsertOrganizationParams{ID: orgID, Name: org.Name, Enabled: org.Enabled})
	if err != nil {
		return fmt.Errorf("failed to update organization: %w", err)
	}

	fmt.Printf("Updated organization %s\n", id)
	return nil
}

func runRemoteOrganizationsUpdate(id string, name *string, enabled *bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	client, cleanup, err := createAdminClient()
	if err != nil {
		return err
	}
	defer cleanup()

	ctx = createAuthContext(ctx)

	req := &adminproto.UpdateOrganizationRequest{Id: id}
	if name != nil {
		req.Name = wrapperspb.String(*name)
	}
	if enabled != nil {
		req.Enabled = wrapperspb.Bool(*enabled)
	}

	_, err = client.UpdateOrganization(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to update organization: %w", err)
	}

	fmt.Printf("Updated organization %s\n", id)
	return nil
}

func createAdminClient() (adminproto.AdminServiceClient, func(), error) {
	var opts []grpc.DialOption

	if adminInsecureMode {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if adminTLSSkipVerify {
			tlsConfig.InsecureSkipVerify = true
		}

		if adminTLSCACert != "" {
			caCert, err := os.ReadFile(adminTLSCACert)
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

	conn, err := grpc.NewClient(adminEndpoint, opts...)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to admin service: %w", err)
	}

	cleanup := func() { _ = conn.Close() }
	client := adminproto.NewAdminServiceClient(conn)
	return client, cleanup, nil
}

func createAuthContext(ctx context.Context) context.Context {
	if apiKey != "" {
		md := metadata.Pairs("authorization", "Bearer "+apiKey)
		ctx = metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}

func printOrganizationsTable(orgs []configdb.Organization) {
	headers := []string{"ID", "Name", "Enabled"}
	widths := []int{len(headers[0]), len(headers[1]), len(headers[2])}

	for _, o := range orgs {
		if l := len(o.ID.String()); l > widths[0] {
			widths[0] = l
		}
		if l := len(o.Name); l > widths[1] {
			widths[1] = l
		}
		enabledStr := fmt.Sprintf("%t", o.Enabled)
		if l := len(enabledStr); l > widths[2] {
			widths[2] = l
		}
	}

	fmt.Print("┌")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┬")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┐")

	fmt.Printf("│ %-*s │ %-*s │ %-*s │\n",
		widths[0], headers[0],
		widths[1], headers[1],
		widths[2], headers[2],
	)

	fmt.Print("├")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┼")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┤")

	for _, o := range orgs {
		enabledStr := fmt.Sprintf("%t", o.Enabled)
		fmt.Printf("│ %-*s │ %-*s │ %-*s │\n",
			widths[0], o.ID.String(),
			widths[1], o.Name,
			widths[2], enabledStr,
		)
	}

	fmt.Print("└")
	for i, w := range widths {
		if i > 0 {
			fmt.Print("┴")
		}
		fmt.Print(strings.Repeat("─", w+2))
	}
	fmt.Println("┘")
}
