package observation

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/fq-connector-go/api/observation"
	"github.com/ydb-platform/fq-connector-go/api/service/protos"
)

const (
	endpointFlag = "endpoint"
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

func init() {
	// Add incoming subcommands
	incomingCmd.AddCommand(incomingAllCmd)
	incomingCmd.AddCommand(incomingRunningCmd)

	// Add outgoing subcommands
	outgoingCmd.AddCommand(outgoingAllCmd)
	outgoingCmd.AddCommand(outgoingRunningCmd)

	// Add main subcommands to the root command
	Cmd.AddCommand(incomingCmd)
	Cmd.AddCommand(outgoingCmd)

	// Add endpoint flag to the main command
	Cmd.PersistentFlags().StringP(endpointFlag, "e", "localhost:2135", "gRPC endpoint to connect to")

	// Propagate flags to all subcommands
	incomingCmd.PersistentFlags().AddFlagSet(Cmd.PersistentFlags())
	outgoingCmd.PersistentFlags().AddFlagSet(Cmd.PersistentFlags())
}

func getClient(cmd *cobra.Command) (observation.ObservationServiceClient, *grpc.ClientConn, error) {
	endpoint, err := cmd.Flags().GetString(endpointFlag)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get endpoint flag: %w", err)
	}

	// Set up a connection to the server
	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to server: %w", err)
	}

	// Create a client
	client := observation.NewObservationServiceClient(conn)

	return client, conn, nil
}

func listIncomingQueries(cmd *cobra.Command, _ []string, state observation.QueryState) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create the request
	req := &observation.ListIncomingQueriesRequest{
		State:  state,
		Limit:  1000,
		Offset: 0,
	}

	// Call the service
	stream, err := client.ListIncomingQueries(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to list incoming queries: %w", err)
	}

	fmt.Println("Incoming Queries:")
	fmt.Println("----------------")

	count := 0

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}

			return fmt.Errorf("error receiving response: %w", err)
		}

		if resp.Error != nil && resp.Error.Status != 0 {
			fmt.Printf("Error: %s\n", resp.Error.Message)
			continue
		}

		if resp.Query != nil {
			query := resp.Query
			finishedAt := ""

			if query.FinishedAt != nil {
				finishedAt = query.FinishedAt.AsTime().Format(time.RFC3339)
			}

			fmt.Println("Query:")
			fmt.Printf("  ID: %s\n", query.Id)
			fmt.Printf("  Data Source: %s\n", query.DataSourceKind)
			fmt.Printf("  Rows Read: %d\n", query.RowsRead)
			fmt.Printf("  Bytes Read: %d\n", query.BytesRead)
			fmt.Printf("  State: %s\n", query.State.String())
			fmt.Printf("  Created At: %s\n", query.CreatedAt.AsTime().Format(time.RFC3339))
			fmt.Printf("  Finished At: %s\n", finishedAt)
			fmt.Printf("  Error: %s\n", query.Error)
			fmt.Println("----------------")

			count++
		}
	}

	fmt.Printf("\nTotal: %d queries\n", count)

	return nil
}

func listOutgoingQueries(cmd *cobra.Command, _ []string, state observation.QueryState) error {
	client, conn, err := getClient(cmd)
	if err != nil {
		return err
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Create the request
	req := &observation.ListOutgoingQueriesRequest{
		State:  state,
		Limit:  1000,
		Offset: 0,
	}

	// Call the service
	stream, err := client.ListOutgoingQueries(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to list outgoing queries: %w", err)
	}

	fmt.Println("Outgoing Queries:")
	fmt.Println("----------------")

	count := 0

	for {
		resp, err := stream.Recv()
		if err != nil {
			if err.Error() == "EOF" {
				break
			}

			return fmt.Errorf("error receiving response: %w", err)
		}

		if resp.Error != nil && resp.Error.Status != 0 {
			fmt.Printf("Error: %s\n", resp.Error.Message)
			continue
		}

		if resp.Query != nil {
			query := resp.Query
			finishedAt := ""

			if query.FinishedAt != nil {
				finishedAt = query.FinishedAt.AsTime().Format(time.RFC3339)
			}

			fmt.Println("Query:")
			fmt.Printf("  ID: %s\n", query.Id)
			fmt.Printf("  Parent ID: %s\n", query.IncomingQueryId)
			fmt.Printf("  Database: %s\n", query.DatabaseName)
			fmt.Printf("  Endpoint: %s\n", query.DatabaseEndpoint)
			fmt.Printf("  Query Text: %s\n", query.QueryText)
			fmt.Printf("  Query Args: %s\n", query.QueryArgs)
			fmt.Printf("  Rows Read: %d\n", query.RowsRead)
			fmt.Printf("  State: %s\n", query.State.String())
			fmt.Printf("  Created At: %s\n", query.CreatedAt.AsTime().Format(time.RFC3339))
			fmt.Printf("  Finished At: %s\n", finishedAt)
			fmt.Printf("  Error: %s\n", query.Error)
			fmt.Println("----------------")

			count++
		}
	}

	fmt.Printf("\nTotal: %d queries\n", count)

	return nil
}

// nolint: unused
func formatError(err *protos.TError) string {
	if err == nil {
		return ""
	}

	return fmt.Sprintf("%s (status: %d)", err.Message, err.Status)
}
