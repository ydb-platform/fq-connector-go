package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	ydb_config "github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/ydb-platform/fq-connector-go/common"
)

// Config holds the application configuration
type Config struct {
	Endpoint     string
	Database     string
	Table        string
	Token        string
	TabletID     string
	UseTLS       bool
	ResourcePool string
}

// parseFlags parses command-line flags and returns the configuration
func parseFlags() (*Config, error) {
	var (
		endpoint     string
		database     string
		table        string
		token        string
		tabletID     string
		resourcePool string
		useTLS       bool
	)

	flag.StringVar(&endpoint, "endpoint", "localhost:2136", "YDB endpoint")
	flag.StringVar(&database, "database", "/local", "YDB database path")
	flag.StringVar(&table, "table", "tpch/s100/lineitem", "YDB table name")
	flag.StringVar(&token, "token", "", "IAM token for authentication")
	flag.StringVar(&tabletID, "tablet-id", "72075186224064796", "Tablet ID to query")
	flag.StringVar(&resourcePool, "resource-pool", "", "Resource pool for YDB queries")
	flag.BoolVar(&useTLS, "tls", true, "Use TLS for YDB connection (grpcs:// instead of grpc://)")

	flag.Parse()

	if endpoint == "" || database == "" || table == "" || tabletID == "" {
		return nil, fmt.Errorf(
			"usage: app -endpoint=<endpoint> -database=<database> -table=<table-name> " +
				"-tablet-id=<tablet-id> [-resource-pool=<resource-pool>] " +
				"[-token=<token>] [-tls=<true|false>]")
	}

	return &Config{
		Endpoint:     endpoint,
		Database:     database,
		Table:        table,
		Token:        token,
		TabletID:     tabletID,
		UseTLS:       useTLS,
		ResourcePool: resourcePool,
	}, nil
}

// makeDriver creates and returns a YDB driver
func makeDriver(ctx context.Context, logger *zap.Logger, endpoint, database, token string, useTLS bool) (*ydb.Driver, error) {
	ydbOptions := []ydb.Option{
		ydb.WithDialTimeout(5 * time.Second),
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.With(ydb_config.WithGrpcOptions(grpc.WithDisableServiceConfig())),
	}

	// Add token credentials if provided
	if token != "" {
		ydbOptions = append(ydbOptions, ydb.WithAccessTokenCredentials(token))
	}

	var scheme string
	if useTLS {
		scheme = "grpcs"

		logger.Info("will use secure TLS connections")
	} else {
		scheme = "grpc"

		ydbOptions = append(ydbOptions, ydb.WithInsecure())

		logger.Warn("will use insecure connections")
	}

	dsn := fmt.Sprintf("%s://%s%s", scheme, endpoint, database)

	authMethod := "none"
	if token != "" {
		authMethod = "IAM token"
	}

	logger.Info("connecting to YDB",
		zap.String("dsn", dsn),
		zap.String("auth", authMethod),
		zap.Bool("tls", useTLS))

	ydbDriver, err := ydb.Open(ctx, dsn, ydbOptions...)
	if err != nil {
		return nil, fmt.Errorf("open driver error: %w", err)
	}

	return ydbDriver, nil
}

// executeQuery runs the query with the specified tablet ID
func executeQuery(ctx context.Context, logger *zap.Logger, ydbDriver *ydb.Driver, config *Config) error {
	// The query to execute
	queryText := fmt.Sprintf(
		"SELECT `l_comment`, `l_commitdate`, `l_discount`, `l_extendedprice`, `l_linenumber`, "+
			"`l_linestatus`, `l_orderkey`, `l_partkey`, `l_quantity`, `l_receiptdate`, "+
			"`l_returnflag`, `l_shipdate`, `l_shipinstruct`, `l_shipmode`, `l_suppkey`, `l_tax` "+
			"FROM `%s` WITH TabletId='%s'",
		config.Table, config.TabletID)

	logger.Info("executing query",
		zap.String("query", queryText),
		zap.String("tablet_id", config.TabletID),
		zap.String("table", config.Table))

	// Log the start time for tracking query duration
	queryStartTime := time.Now()

	rowCount := 0

	// Create variables to scan into with proper types outside the loop
	var (
		l_comment       string
		l_commitdate    time.Time // Date type
		l_discount      float64
		l_extendedprice float64
		l_linenumber    int64
		l_linestatus    string
		l_orderkey      int64
		l_partkey       int64
		l_quantity      float64
		l_receiptdate   time.Time // Date type
		l_returnflag    string
		l_shipdate      time.Time // Date type
		l_shipinstruct  string
		l_shipmode      string
		l_suppkey       int64
		l_tax           float64
	)

	err := ydbDriver.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		var queryOpts []query.ExecuteOption

		if config.ResourcePool != "" {
			queryOpts = append(queryOpts, query.WithResourcePool(config.ResourcePool))
		}

		result, err := session.Query(ctx, queryText, queryOpts...)
		if err != nil {
			return fmt.Errorf("query error: %w", err)
		}

		defer result.Close(ctx)

		for {
			resultSet, err := result.NextResultSet(ctx)
			if err != nil {
				if err == io.EOF {
					break
				}
				return fmt.Errorf("next result set: %w", err)
			}

			// No need to print column names, we're just counting rows

			// Log the start of processing this result set with more details
			logger.Info("processing result set",
				zap.String("tablet_id", config.TabletID),
				zap.String("table", config.Table),
				zap.Duration("elapsed_time", time.Since(queryStartTime)))

			// Initialize a counter for this result set
			resultSetRowCount := 0

			// Create a ticker for more frequent logging
			logTicker := time.NewTicker(5 * time.Second)
			defer logTicker.Stop()

			// Start a goroutine for periodic logging
			done := make(chan bool)
			go func() {
				for {
					select {
					case <-done:
						return
					case <-logTicker.C:
						logger.Info("query in progress",
							zap.Int("rows_processed_so_far", rowCount),
							zap.Duration("elapsed_time", time.Since(queryStartTime)),
							zap.String("tablet_id", config.TabletID))
					}
				}
			}()

			for {
				row, err := resultSet.NextRow(ctx)
				if err != nil {
					if err == io.EOF {
						break
					}
					return fmt.Errorf("next row: %w", err)
				}

				// Signal to stop the logging goroutine when we're done with this result set
				defer func() {
					done <- true
				}()

				// Scan the row into variables but don't use their values
				if err := row.Scan(
					&l_comment, &l_commitdate, &l_discount, &l_extendedprice, &l_linenumber,
					&l_linestatus, &l_orderkey, &l_partkey, &l_quantity, &l_receiptdate,
					&l_returnflag, &l_shipdate, &l_shipinstruct, &l_shipmode, &l_suppkey, &l_tax,
				); err != nil {
					return fmt.Errorf("scan row: %w", err)
				}

				rowCount++
				resultSetRowCount++

				// Log progress every 1,000 rows
				if rowCount > 0 && rowCount%1000 == 0 {
					logger.Info("query progress",
						zap.Int("rows_processed", rowCount),
						zap.Duration("elapsed_time", time.Since(queryStartTime)),
						zap.String("tablet_id", config.TabletID))
				}
			}
		}

		return nil
	}, query.WithIdempotent())

	if err != nil {
		return fmt.Errorf("execute query: %w", err)
	}

	queryDuration := time.Since(queryStartTime)
	logger.Info("query completed",
		zap.Int("total_rows", rowCount),
		zap.String("tablet_id", config.TabletID),
		zap.String("table", config.Table),
		zap.Duration("total_duration", queryDuration),
		zap.Float64("rows_per_second", float64(rowCount)/queryDuration.Seconds()))

	// Print the final count to stdout
	fmt.Printf("Total rows: %d\n", rowCount)
	return nil
}

func main() {
	config, err := parseFlags()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	logger := common.NewDefaultLogger()
	defer func() {
		_ = logger.Sync()
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ydbDriver, err := makeDriver(ctx, logger, config.Endpoint, config.Database, config.Token, config.UseTLS)
	if err != nil {
		logger.Fatal("failed to create YDB driver", zap.Error(err))
	}

	defer func() {
		if err = ydbDriver.Close(context.Background()); err != nil {
			logger.Error("error closing YDB driver", zap.Error(err))
		}
	}()

	logger.Info("starting query execution",
		zap.String("table", config.Table),
		zap.String("tablet_id", config.TabletID))

	if err := executeQuery(ctx, logger, ydbDriver, config); err != nil {
		logger.Fatal("failed to execute query", zap.Error(err))
	}

	logger.Info("query execution completed successfully")
}
