package main

import (
	"context"
	"fmt"
	"log"
	"path"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
	"google.golang.org/grpc"
)

const (
	dbEndpoint = "localhost:2136"
	dbName     = "/local"
	tableName  = "simple"
)

func main() {
	log.Println("Correct credentials")
	obtainTableDesciption(dbEndpoint, "admin", "password")

	log.Println("Invalid credentials")
	obtainTableDesciption(dbEndpoint, "admin2", "password")
}

func obtainTableDesciption(endpoint, login, password string) {
	ydbDriver, err := makeDriver(endpoint, login, password)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if closeErr := ydbDriver.Close(context.Background()); closeErr != nil {
			log.Fatal(closeErr)
		}
	}()

	desc, err := getTableDescription(ydbDriver)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Table description: %+v", desc)
}

func makeDriver(endpoint, login, password string) (*ydb.Driver, error) {
	ydbOptions := []ydb.Option{
		ydb.WithStaticCredentials(login, password),
		ydb.WithDialTimeout(5 * time.Second),
		ydb.WithBalancer(balancers.SingleConn()), // see YQ-3089
		ydb.With(config.WithGrpcOptions(grpc.WithDisableServiceConfig())),
	}

	dsn := sugar.DSN(endpoint, dbName, false)

	ydbDriver, err := ydb.Open(context.Background(), dsn, ydbOptions...)
	if err != nil {
		return nil, fmt.Errorf("open driver error: %w", err)
	}

	return ydbDriver, nil
}

func getTableDescription(ydbDriver *ydb.Driver) (*options.Description, error) {
	desc := options.Description{}
	filePath := path.Join(dbName, tableName)

	log.Println("Getting table description for table", filePath)

	err := ydbDriver.Table().Do(
		context.Background(),
		func(ctx context.Context, s table.Session) error {
			var errInner error
			desc, errInner = s.DescribeTable(ctx, filePath)
			if errInner != nil {
				return fmt.Errorf("describe table: %w", errInner)
			}

			return nil
		},
		table.WithIdempotent(),
	)

	if err != nil {
		return nil, fmt.Errorf("get table description: %w", err)
	}

	return &desc, nil
}
