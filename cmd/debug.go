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

package cmd

import (
	"github.com/spf13/cobra"

	debugcmd "github.com/cardinalhq/lakerunner/cmd/debug"
)

var debugCmd = &cobra.Command{
	Use:   "debug",
	Short: "Debug commands for troubleshooting",
	Long:  `Debug commands for troubleshooting various components of lakerunner.`,
}

func init() {
	debugCmd.AddCommand(debugcmd.GetS3Cmd())
	debugCmd.AddCommand(debugcmd.GetDDBCmd())
	debugCmd.AddCommand(debugcmd.GetParquetCmd())
	debugCmd.AddCommand(debugcmd.GetScalingCmd())
	debugCmd.AddCommand(debugcmd.GetKafkaCmd())
	debugCmd.AddCommand(debugcmd.GetDownloadCmd())
}
