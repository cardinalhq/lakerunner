package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	bigrun "github.com/cardinalhq/lakerunner/bigquery"
	"github.com/spf13/cobra"
)

var (
	saDatasets string
	saTopN     int
	saProject  string
	saAddr     string
)

func init() {
	cmd := &cobra.Command{
		Use:   "serve-analyst",
		Short: "Start Analyst HTTP server (backing the MCP tools)",
		RunE: func(cmd *cobra.Command, args []string) error {
			project := strings.TrimSpace(saProject)
			if project == "" {
				project = os.Getenv("GOOGLE_CLOUD_PROJECT")
			}
			if project == "" {
				return fmt.Errorf("project is required (use --project or set GOOGLE_CLOUD_PROJECT)")
			}
			var datasets []string
			if s := strings.TrimSpace(saDatasets); s != "" {
				for _, p := range strings.Split(s, ",") {
					if v := strings.TrimSpace(p); v != "" {
						datasets = append(datasets, v)
					}
				}
			}

			ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			s, err := bigrun.NewAnalystServer(ctx, project, datasets, saTopN)
			if err != nil {
				return err
			}
			defer s.Close()

			if saAddr == "" {
				saAddr = ":8080"
			}
			return s.ServeHTTP(ctx, saAddr)
		},
	}

	cmd.Flags().StringVar(&saDatasets, "datasets", "", "Comma-separated dataset IDs")
	cmd.Flags().IntVar(&saTopN, "top", 50, "Catalog size")
	cmd.Flags().StringVar(&saProject, "project", "", "GCP Project ID (defaults to $GOOGLE_CLOUD_PROJECT)")
	cmd.Flags().StringVar(&saAddr, "addr", ":8080", "Listen address (host:port)")

	rootCmd.AddCommand(cmd)
}
