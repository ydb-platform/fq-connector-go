package observation

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/ydb-platform/fq-connector-go/api/observation"
)

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
