package observation

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
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
	"github.com/ydb-platform/fq-connector-go/common"
)

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
	var allQueries []*incomingQueryWithEndpoint

	for _, endpoint := range endpoints {
		queries, err := fetchIncomingQueries(endpoint, logger)
		if err != nil {
			fmt.Printf("Error fetching from %s: %v\n", endpoint, err)

			continue
		}

		fmt.Printf("Fetched %d incoming queries from %s\n", len(queries), endpoint)

		// Add endpoint information to each query
		for _, q := range queries {
			allQueries = append(allQueries, &incomingQueryWithEndpoint{
				IncomingQuery: q,
				Endpoint:      endpoint,
			})
		}
	}

	if len(allQueries) == 0 {
		return errors.New("no incoming queries fetched from any endpoint")
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
	var allQueries []*outgoingQueryWithEndpoint

	for _, endpoint := range endpoints {
		queries, err := fetchOutgoingQueries(endpoint, logger)
		if err != nil {
			fmt.Printf("Error fetching from %s: %v\n", endpoint, err)

			continue
		}

		fmt.Printf("Fetched %d outgoing queries from %s\n", len(queries), endpoint)

		// Add endpoint information to each query
		for _, q := range queries {
			allQueries = append(allQueries, &outgoingQueryWithEndpoint{
				OutgoingQuery: q,
				Endpoint:      endpoint,
			})
		}
	}

	if len(allQueries) == 0 {
		return errors.New("no outgoing queries fetched from any endpoint")
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
		return nil, "", errors.New("no endpoints provided")
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
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
				if errors.Is(err, io.EOF) {
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
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
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
				if errors.Is(err, io.EOF) {
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

// incomingQueryWithEndpoint extends IncomingQuery with endpoint information
type incomingQueryWithEndpoint struct {
	*observation.IncomingQuery
	Endpoint string
}

// outgoingQueryWithEndpoint extends OutgoingQuery with endpoint information
type outgoingQueryWithEndpoint struct {
	*observation.OutgoingQuery
	Endpoint string
}

// writeIncomingQueriesToCSV writes incoming queries to a CSV file
func writeIncomingQueriesToCSV(queries []*incomingQueryWithEndpoint, outputPath string) error {
	if len(queries) == 0 {
		return errors.New("no queries to write")
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
func writeOutgoingQueriesToCSV(queries []*outgoingQueryWithEndpoint, outputPath string) error {
	if len(queries) == 0 {
		return errors.New("no queries to write")
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
