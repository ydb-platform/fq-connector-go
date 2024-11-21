package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"google.golang.org/grpc"
)

const (
	dbEndpoint = "localhost:2136"
	dbName     = "/local"
	tableName  = "json_document"
	login      = "admin"
	password   = "password"
)

func main() {
	run()
}

func run() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ydbDriver, err := makeDriver(ctx, dbEndpoint, login, password)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if closeErr := ydbDriver.Close(ctx); closeErr != nil {
			log.Fatal(closeErr)
		}
	}()

	err = getData(ctx, ydbDriver)
	if err != nil {
		log.Fatal(err)
	}
}

func makeDriver(ctx context.Context, endpoint, login, password string) (*ydb.Driver, error) {
	ydbOptions := []ydb.Option{
		ydb.WithStaticCredentials(login, password),
		ydb.WithDialTimeout(5 * time.Second),
		ydb.WithBalancer(balancers.SingleConn()), // see YQ-3089
		ydb.With(config.WithGrpcOptions(grpc.WithDisableServiceConfig())),
	}

	dsn := sugar.DSN(endpoint, dbName, false)

	ydbDriver, err := ydb.Open(ctx, dsn, ydbOptions...)
	if err != nil {
		return nil, fmt.Errorf("open driver error: %w", err)
	}

	return ydbDriver, nil
}

func getData(ctx context.Context, ydbDriver *ydb.Driver) error {
	finalErr := ydbDriver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		queryText := `
		SELECT id, data FROM json_document;
		`

		result, err := s.Query(ctx, queryText)
		if err != nil {
			return fmt.Errorf("query error: %w", err)
		}

		rs, err := result.NextResultSet(ctx)
		if err != nil {
			return fmt.Errorf("next result set: %w", err)
		}

		for {
			row, err := rs.NextRow(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					fmt.Println("EOF")
					return nil
				}
				return fmt.Errorf("next row: %w", err)
			}

			var (
				id   int32
				data []byte
			)

			err = row.Scan(&id, &data)
			if err != nil {
				return fmt.Errorf("row scan: %w", err)
			}

			fmt.Printf("Row values: id=%d, data(binary)=%v, data(string)=%v\n", id, data, string(data))
		}
	})

	if finalErr != nil {
		return fmt.Errorf("get data: %w", finalErr)
	}

	return nil
}
