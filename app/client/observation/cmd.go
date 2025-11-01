package observation

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/fq-connector-go/api/observation"
)

const (
	endpointFlag   = "endpoint"  // For incoming/outgoing commands
	endpointsFlag  = "endpoints" // For dump commands
	outputFileFlag = "output"    // For dump commands
	formatFlag     = "format"    // For dump commands (csv or parquet)
)

var Cmd = &cobra.Command{
	Use:   "observation",
	Short: "Client for Observation GRPC API",
}

// Incoming queries commands
var incomingCmd = &cobra.Command{
	Use:   "incoming",
	Short: "Commands for incoming queries",
}

var incomingAllCmd = &cobra.Command{
	Use:   "all",
	Short: "List all incoming queries",
	Run: func(cmd *cobra.Command, args []string) {
		if err := listIncomingQueries(cmd, args, observation.QueryState_QUERY_STATE_UNSPECIFIED); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

var incomingRunningCmd = &cobra.Command{
	Use:   "running",
	Short: "List running incoming queries",
	Run: func(cmd *cobra.Command, args []string) {
		if err := listIncomingQueries(cmd, args, observation.QueryState_QUERY_STATE_RUNNING); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

// Outgoing queries commands
var outgoingCmd = &cobra.Command{
	Use:   "outgoing",
	Short: "Commands for outgoing queries",
}

var outgoingAllCmd = &cobra.Command{
	Use:   "all",
	Short: "List all outgoing queries",
	Run: func(cmd *cobra.Command, args []string) {
		if err := listOutgoingQueries(cmd, args, observation.QueryState_QUERY_STATE_UNSPECIFIED); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

var outgoingRunningCmd = &cobra.Command{
	Use:   "running",
	Short: "List running outgoing queries",
	Run: func(cmd *cobra.Command, args []string) {
		if err := listOutgoingQueries(cmd, args, observation.QueryState_QUERY_STATE_RUNNING); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

// Dump commands
var dumpCmd = &cobra.Command{
	Use:   "dump",
	Short: "Dump queries to a CSV file",
}

var dumpIncomingCmd = &cobra.Command{
	Use:   "incoming",
	Short: "Dump all incoming queries to a CSV file",
	Run: func(cmd *cobra.Command, _ []string) {
		if err := dumpIncomingQueries(cmd); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

var dumpOutgoingCmd = &cobra.Command{
	Use:   "outgoing",
	Short: "Dump all outgoing queries to a CSV file",
	Run: func(cmd *cobra.Command, _ []string) {
		if err := dumpOutgoingQueries(cmd); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	},
}

func init() {
	// Add incoming subcommands
	incomingCmd.AddCommand(incomingAllCmd)
	incomingCmd.AddCommand(incomingRunningCmd)

	// Add outgoing subcommands
	outgoingCmd.AddCommand(outgoingAllCmd)
	outgoingCmd.AddCommand(outgoingRunningCmd)

	// Add dump subcommands
	dumpCmd.AddCommand(dumpIncomingCmd)
	dumpCmd.AddCommand(dumpOutgoingCmd)

	// Add main subcommands to the root command
	Cmd.AddCommand(incomingCmd)
	Cmd.AddCommand(outgoingCmd)
	Cmd.AddCommand(dumpCmd)

	// Add flags for dump commands
	dumpCmd.PersistentFlags().String(endpointsFlag, "", "Comma-separated list of gRPC endpoints to fetch queries from (required)")
	dumpCmd.PersistentFlags().String(outputFileFlag, "queries.csv", "Output file path")
	dumpCmd.PersistentFlags().String(formatFlag, "csv", "Output format (csv only for now)")

	// Mark required flags
	if err := dumpCmd.MarkPersistentFlagRequired(endpointsFlag); err != nil {
		panic(err)
	}

	// Add endpoint flag to incoming/outgoing commands
	incomingCmd.PersistentFlags().StringP(endpointFlag, "e", "localhost:2135", "gRPC endpoint to connect to")
	outgoingCmd.PersistentFlags().StringP(endpointFlag, "e", "localhost:2135", "gRPC endpoint to connect to")
}

func getClient(cmd *cobra.Command) (observation.ObservationServiceClient, *grpc.ClientConn, error) {
	endpoint, err := cmd.Flags().GetString("endpoint")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get endpoint flag: %w", err)
	}

	// Set up a connection to the server
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	// Create a client
	client := observation.NewObservationServiceClient(conn)

	return client, conn, nil
}
