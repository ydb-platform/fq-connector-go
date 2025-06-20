package observation

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
)

const (
	endpointsFlag = "endpoints" // For aggregate command
	portFlag      = "port"
	periodFlag    = "period"
)

var Cmd = &cobra.Command{
	Use:   "observation",
	Short: "Observation GRPC API client",
}

// Track command
var trackCmd = &cobra.Command{
	Use:   "track",
	Short: "Track outgoing queries from multiple connectors",
	Run: func(cmd *cobra.Command, _ []string) {
		if err := startAggregationServer(cmd); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	// Add track command to the root command
	Cmd.AddCommand(trackCmd)

	// Add flags for track command
	trackCmd.Flags().String(endpointsFlag, "", "Comma-separated list of gRPC endpoints to monitor (required)")
	trackCmd.Flags().Int(portFlag, 8081, "Port to serve dashboard on")
	trackCmd.Flags().Duration(periodFlag, 5*time.Second, "Polling period")

	// Mark required flags
	if err := trackCmd.MarkFlagRequired(endpointsFlag); err != nil {
		panic(err)
	}
}
