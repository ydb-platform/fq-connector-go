package observation

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ydb-platform/fq-connector-go/api/observation"
	"github.com/ydb-platform/fq-connector-go/api/service/protos"
	"github.com/ydb-platform/fq-connector-go/common"
)

const (
	endpointFlag   = "endpoint"  // For incoming/outgoing commands
	endpointsFlag  = "endpoints" // For aggregate command and dump commands
	portFlag       = "port"
	periodFlag     = "period"
	outputFileFlag = "output" // For dump commands
	formatFlag     = "format" // For dump commands (csv or parquet)

	// Error constants
	eofError = "EOF"
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

// Aggregate command
var aggregateCmd = &cobra.Command{
	Use:   "aggregate",
	Short: "Aggregate outgoing queries from multiple connectors",
	Run: func(cmd *cobra.Command, _ []string) {
		if err := startAggregationServer(cmd); err != nil {
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
	Cmd.AddCommand(aggregateCmd)
	Cmd.AddCommand(dumpCmd)

	// Add flags for aggregate command
	aggregateCmd.Flags().String(endpointsFlag, "", "Comma-separated list of gRPC endpoints to monitor (required)")
	aggregateCmd.Flags().Int(portFlag, 8081, "Port to serve dashboard on")
	aggregateCmd.Flags().Duration(periodFlag, 5*time.Second, "Polling period")

	// Add flags for dump commands
	dumpCmd.PersistentFlags().String(endpointsFlag, "", "Comma-separated list of gRPC endpoints to fetch queries from (required)")
	dumpCmd.PersistentFlags().String(outputFileFlag, "queries.csv", "Output file path")
	dumpCmd.PersistentFlags().String(formatFlag, "csv", "Output format (csv only for now)")

	// Mark required flags
	if err := aggregateCmd.MarkFlagRequired(endpointsFlag); err != nil {
		panic(err)
	}

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

func startAggregationServer(cmd *cobra.Command) error {
	endpoints, err := cmd.Flags().GetString(endpointsFlag)
	if err != nil {
		return fmt.Errorf("failed to get endpoints: %w", err)
	}

	port, err := cmd.Flags().GetInt(portFlag)
	if err != nil {
		return fmt.Errorf("failed to get port: %w", err)
	}

	period, err := cmd.Flags().GetDuration(periodFlag)
	if err != nil {
		return fmt.Errorf("failed to get period: %w", err)
	}

	// Split endpoints
	endpointList := strings.Split(endpoints, ",")
	if len(endpointList) == 0 {
		return fmt.Errorf("no endpoints provided")
	}

	// Create server
	server := NewAggregationServer(endpointList, period)

	// Start HTTP server
	fmt.Printf("Starting aggregation server on :%d\n", port)

	return server.Start(port)
}

// dumpIncomingQueries fetches all incoming queries from multiple endpoints and writes them to a Parquet file
func dumpIncomingQueries(cmd *cobra.Command) error {
	// Get endpoints and output file path
	endpoints, outputFile, err := getDumpParams(cmd)
	if err != nil {
		return err
	}

	fmt.Printf("Fetching incoming queries from %d endpoints...\n", len(endpoints))

	// Create a logger
	logger := common.NewDefaultLogger()

	// Collect queries from all endpoints
	var allQueries []*IncomingQueryWithEndpoint

	for _, endpoint := range endpoints {
		queries, err := fetchIncomingQueries(endpoint, logger)
		if err != nil {
			fmt.Printf("Error fetching from %s: %v\n", endpoint, err)
			continue
		}

		fmt.Printf("Fetched %d incoming queries from %s\n", len(queries), endpoint)

		// Add endpoint information to each query
		for _, q := range queries {
			allQueries = append(allQueries, &IncomingQueryWithEndpoint{
				IncomingQuery: q,
				Endpoint:      endpoint,
			})
		}
	}

	if len(allQueries) == 0 {
		return fmt.Errorf("no incoming queries fetched from any endpoint")
	}

	// Write to CSV file
	if err := writeIncomingQueriesToCSV(allQueries, outputFile); err != nil {
		return fmt.Errorf("failed to write CSV file: %w", err)
	}

	fmt.Printf("Successfully wrote %d incoming queries to %s\n", len(allQueries), outputFile)

	return nil
}

// dumpOutgoingQueries fetches all outgoing queries from multiple endpoints and writes them to a Parquet file
func dumpOutgoingQueries(cmd *cobra.Command) error {
	// Get endpoints and output file path
	endpoints, outputFile, err := getDumpParams(cmd)
	if err != nil {
		return err
	}

	fmt.Printf("Fetching outgoing queries from %d endpoints...\n", len(endpoints))

	// Create a logger
	logger := common.NewDefaultLogger()

	// Collect queries from all endpoints
	var allQueries []*OutgoingQueryWithEndpoint

	for _, endpoint := range endpoints {
		queries, err := fetchOutgoingQueries(endpoint, logger)
		if err != nil {
			fmt.Printf("Error fetching from %s: %v\n", endpoint, err)
			continue
		}

		fmt.Printf("Fetched %d outgoing queries from %s\n", len(queries), endpoint)

		// Add endpoint information to each query
		for _, q := range queries {
			allQueries = append(allQueries, &OutgoingQueryWithEndpoint{
				OutgoingQuery: q,
				Endpoint:      endpoint,
			})
		}
	}

	if len(allQueries) == 0 {
		return fmt.Errorf("no outgoing queries fetched from any endpoint")
	}

	// Write to CSV file
	if err := writeOutgoingQueriesToCSV(allQueries, outputFile); err != nil {
		return fmt.Errorf("failed to write CSV file: %w", err)
	}

	fmt.Printf("Successfully wrote %d outgoing queries to %s\n", len(allQueries), outputFile)

	return nil
}

// getDumpParams extracts common parameters for dump commands
func getDumpParams(cmd *cobra.Command) ([]string, string, error) {
	// Get endpoints
	endpointsStr, err := cmd.Flags().GetString(endpointsFlag)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get endpoints flag: %w", err)
	}

	endpoints := strings.Split(endpointsStr, ",")
	if len(endpoints) == 0 {
		return nil, "", fmt.Errorf("no endpoints provided")
	}

	// Get output file path
	outputFile, err := cmd.Flags().GetString(outputFileFlag)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get output file flag: %w", err)
	}

	// Create directory if it doesn't exist
	dir := filepath.Dir(outputFile)
	if dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, "", fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	return endpoints, outputFile, nil
}

// fetchIncomingQueries retrieves all incoming queries from a single endpoint
func fetchIncomingQueries(endpoint string, logger *zap.Logger) ([]*observation.IncomingQuery, error) {
	logger.Info("connecting to endpoint for incoming queries", zap.String("endpoint", endpoint))

	// Connect to the endpoint
	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", endpoint, err)
	}
	defer conn.Close()

	client := observation.NewObservationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var allQueries []*observation.IncomingQuery

	offset := 0
	batchSize := 1000

	for {
		// Create the request with pagination
		req := &observation.ListIncomingQueriesRequest{
			State:  observation.QueryState_QUERY_STATE_UNSPECIFIED,
			Limit:  int32(batchSize),
			Offset: int32(offset),
		}

		// Call the service
		stream, err := client.ListIncomingQueries(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("failed to list incoming queries: %w", err)
		}

		var batchQueries []*observation.IncomingQuery

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err.Error() == eofError {
					break
				}

				return nil, fmt.Errorf("error receiving response: %w", err)
			}

			if resp.Error != nil && resp.Error.Status != 0 {
				logger.Warn("received error in stream", zap.String("message", resp.Error.Message))
				continue
			}

			if resp.Query != nil {
				batchQueries = append(batchQueries, resp.Query)
			}
		}

		// Add batch to all queries
		allQueries = append(allQueries, batchQueries...)

		// Log progress
		logger.Info("fetched batch of incoming queries",
			zap.String("endpoint", endpoint),
			zap.Int("batch_size", len(batchQueries)),
			zap.Int("total_so_far", len(allQueries)),
			zap.Int("offset", offset))

		// If we got fewer results than the batch size, we've reached the end
		if len(batchQueries) < batchSize {
			break
		}

		// Increment offset for next batch
		offset += len(batchQueries)
	}

	logger.Info("completed fetching all incoming queries",
		zap.String("endpoint", endpoint),
		zap.Int("total_count", len(allQueries)))

	return allQueries, nil
}

// fetchOutgoingQueries retrieves all outgoing queries from a single endpoint
func fetchOutgoingQueries(endpoint string, logger *zap.Logger) ([]*observation.OutgoingQuery, error) {
	logger.Info("connecting to endpoint for outgoing queries", zap.String("endpoint", endpoint))

	// Connect to the endpoint
	conn, err := grpc.Dial(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s: %w", endpoint, err)
	}
	defer conn.Close()

	client := observation.NewObservationServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	var allQueries []*observation.OutgoingQuery

	offset := 0
	batchSize := 1000

	for {
		// Create the request with pagination
		req := &observation.ListOutgoingQueriesRequest{
			State:  observation.QueryState_QUERY_STATE_UNSPECIFIED,
			Limit:  int32(batchSize),
			Offset: int32(offset),
		}

		// Call the service
		stream, err := client.ListOutgoingQueries(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("failed to list outgoing queries: %w", err)
		}

		var batchQueries []*observation.OutgoingQuery

		for {
			resp, err := stream.Recv()
			if err != nil {
				if err.Error() == eofError {
					break
				}

				return nil, fmt.Errorf("error receiving response: %w", err)
			}

			if resp.Error != nil && resp.Error.Status != 0 {
				logger.Warn("received error in stream", zap.String("message", resp.Error.Message))
				continue
			}

			if resp.Query != nil {
				batchQueries = append(batchQueries, resp.Query)
			}
		}

		// Add batch to all queries
		allQueries = append(allQueries, batchQueries...)

		// Log progress
		logger.Info("fetched batch of outgoing queries",
			zap.String("endpoint", endpoint),
			zap.Int("batch_size", len(batchQueries)),
			zap.Int("total_so_far", len(allQueries)),
			zap.Int("offset", offset))

		// If we got fewer results than the batch size, we've reached the end
		if len(batchQueries) < batchSize {
			break
		}

		// Increment offset for next batch
		offset += len(batchQueries)
	}

	logger.Info("completed fetching all outgoing queries",
		zap.String("endpoint", endpoint),
		zap.Int("total_count", len(allQueries)))

	return allQueries, nil
}

// IncomingQueryWithEndpoint extends IncomingQuery with endpoint information
type IncomingQueryWithEndpoint struct {
	*observation.IncomingQuery
	Endpoint string
}

// OutgoingQueryWithEndpoint extends OutgoingQuery with endpoint information
type OutgoingQueryWithEndpoint struct {
	*observation.OutgoingQuery
	Endpoint string
}

// writeIncomingQueriesToCSV writes incoming queries to a CSV file
func writeIncomingQueriesToCSV(queries []*IncomingQueryWithEndpoint, outputPath string) error {
	if len(queries) == 0 {
		return fmt.Errorf("no queries to write")
	}

	// Create CSV file
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"id",
		"data_source_kind",
		"rows_read",
		"bytes_read",
		"state",
		"created_at",
		"finished_at",
		"elapsed_time_ms",
		"error",
		"connector_endpoint",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write data
	for _, q := range queries {
		createdAt := ""
		if q.CreatedAt != nil {
			createdAt = q.CreatedAt.AsTime().Format(time.RFC3339Nano)
		}

		finishedAt := ""
		if q.FinishedAt != nil {
			finishedAt = q.FinishedAt.AsTime().Format(time.RFC3339Nano)
		}

		// Calculate elapsed time
		var elapsedTimeMs string

		if q.CreatedAt != nil && q.FinishedAt != nil {
			elapsedTime := q.FinishedAt.AsTime().Sub(q.CreatedAt.AsTime())
			elapsedTimeMs = strconv.FormatInt(elapsedTime.Milliseconds(), 10)
		} else {
			elapsedTimeMs = ""
		}

		row := []string{
			q.Id,
			q.DataSourceKind,
			strconv.FormatInt(q.RowsRead, 10),
			strconv.FormatInt(q.BytesRead, 10),
			q.State.String(), // Human-readable state
			createdAt,
			finishedAt,
			elapsedTimeMs,
			q.Error,
			q.Endpoint,
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	return nil
}

// writeOutgoingQueriesToCSV writes outgoing queries to a CSV file
func writeOutgoingQueriesToCSV(queries []*OutgoingQueryWithEndpoint, outputPath string) error {
	if len(queries) == 0 {
		return fmt.Errorf("no queries to write")
	}

	// Create CSV file
	file, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"id",
		"incoming_query_id",
		"database_name",
		"database_endpoint",
		"query_text",
		"query_args",
		"state",
		"created_at",
		"finished_at",
		"elapsed_time_ms",
		"rows_read",
		"error",
		"connector_endpoint",
	}
	if err := writer.Write(header); err != nil {
		return fmt.Errorf("failed to write header: %w", err)
	}

	// Write data
	for _, q := range queries {
		createdAt := ""
		if q.CreatedAt != nil {
			createdAt = q.CreatedAt.AsTime().Format(time.RFC3339Nano)
		}

		finishedAt := ""
		if q.FinishedAt != nil {
			finishedAt = q.FinishedAt.AsTime().Format(time.RFC3339Nano)
		}

		// Calculate elapsed time
		var elapsedTimeMs string

		if q.CreatedAt != nil && q.FinishedAt != nil {
			elapsedTime := q.FinishedAt.AsTime().Sub(q.CreatedAt.AsTime())
			elapsedTimeMs = strconv.FormatInt(elapsedTime.Milliseconds(), 10)
		} else {
			elapsedTimeMs = ""
		}

		row := []string{
			q.Id,
			q.IncomingQueryId,
			q.DatabaseName,
			q.DatabaseEndpoint,
			q.QueryText,
			q.QueryArgs,
			q.State.String(), // Human-readable state
			createdAt,
			finishedAt,
			elapsedTimeMs,
			strconv.FormatInt(q.RowsRead, 10),
			q.Error,
			q.Endpoint,
		}

		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write row: %w", err)
		}
	}

	return nil
}
