package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"time"

	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"github.com/ydb-platform/ydb-go-sdk/v3/sugar"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

const (
	dbEndpoint = "localhost:2136"
	dbName     = "/local"
	tableName  = "simple"
)

func main() {
	// Define flags for login and password
	var (
		login    string
		password string
	)

	flag.StringVar(&login, "login", "", "Username for login")
	flag.StringVar(&password, "password", "", "Password for login")

	// Parse the command-line flags
	flag.Parse()

	// Check if mandatory flags are provided
	if login == "" || password == "" {
		fmt.Println("Usage: app -login=<login> -password=<password>")
		os.Exit(1)
	}

	run(dbEndpoint, login, password)
}

func run(endpoint, login, password string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ydbDriver, err := makeDriver(ctx, endpoint, login, password)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if closeErr := ydbDriver.Close(ctx); closeErr != nil {
			log.Fatal(closeErr)
		}
	}()

	desc, err := getTableDescription(ctx, ydbDriver)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("Table description: %+v\n", desc)

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

	dsn := sugar.DSN(endpoint, dbName)

	ydbDriver, err := ydb.Open(ctx, dsn, ydbOptions...)
	if err != nil {
		return nil, fmt.Errorf("open driver error: %w", err)
	}

	return ydbDriver, nil
}

func getTableDescription(ctx context.Context, ydbDriver *ydb.Driver) (*options.Description, error) {
	desc := options.Description{}
	filePath := path.Join(dbName, tableName)

	log.Println("Getting table description for table", filePath)

	err := ydbDriver.Table().Do(
		ctx,
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

func getData(ctx context.Context, ydbDriver *ydb.Driver) error {
	finalErr := ydbDriver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		queryText := `
		DECLARE $p0 AS Optional<Int32>;
		SELECT * FROM %s WHERE col2 = $p0;
		`

		paramsBuilder := ydb.ParamsBuilder()
		paramsBuilder = paramsBuilder.Param("$p0").BeginOptional().Int32(nil).EndOptional()

		result, err := s.Query(ctx, fmt.Sprintf(queryText, tableName), query.WithParameters(paramsBuilder.Build()))
		if err != nil {
			return fmt.Errorf("query error: %w", err)
		}

		fmt.Println(result)

		rs, err := result.NextResultSet(ctx)
		if err != nil {
			return fmt.Errorf("next result set: %w", err)
		}

		_, err = rs.NextRow(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				fmt.Println("EOF")
				return nil
			}

			return fmt.Errorf("next row: %w", err)
		}

		return nil
	})

	if finalErr != nil {
		return fmt.Errorf("get data: %w", finalErr)
	}

	return nil
}
