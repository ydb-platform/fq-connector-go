package observation

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const (
	configFlag = "config"
)

var Cmd = &cobra.Command{
	Use:   "observation",
	Short: "Observation GRPC API client",
}

// Track command
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Run server tracking running queries from multiple connectors",
	Run: func(cmd *cobra.Command, _ []string) {
		if err := startAggregationServer(cmd); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	// Add track command to the root command
	Cmd.AddCommand(serverCmd)

	// Add flags for track command
	serverCmd.Flags().String(configFlag, "", "Path to configuration file")

	// Mark required flags
	if err := serverCmd.MarkFlagRequired(configFlag); err != nil {
		panic(err)
	}
}
