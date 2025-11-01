package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/ydb-platform/fq-connector-go/common"
)

// tabletRows implements a simplified version of the iterator for query results
type tabletRows struct {
	ctx context.Context
	err error

	streamResult  query.Result
	lastResultSet query.ResultSet
	lastRow       query.Row
}

func (r *tabletRows) Next() bool {
	var err error

	r.lastRow, err = r.lastResultSet.NextRow(r.ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.err = nil
		} else {
			r.err = fmt.Errorf("next row: %w", err)
		}

		return false
	}

	return true
}

func (r *tabletRows) NextResultSet() bool {
	var err error

	r.lastResultSet, err = r.streamResult.NextResultSet(r.ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			r.err = nil
		} else {
			fmt.Println("obtaining next result", err, r.ctx.Err())

			r.err = fmt.Errorf("next result set: %w", err)
		}

		return false
	}

	return true
}

func (r *tabletRows) Scan(dest ...any) error {
	if err := r.lastRow.Scan(dest...); err != nil {
		return fmt.Errorf("last row scan: %w", err)
	}

	return nil
}

func (r *tabletRows) Err() error {
	return r.err
}

func (r *tabletRows) Close() error {
	if err := r.streamResult.Close(r.ctx); err != nil {
		return fmt.Errorf("stream result close: %w", err)
	}

	return nil
}

// Config holds the application configuration
type Config struct {
	Endpoint     string
	Database     string
	Table        string
	Token        string
	TabletID     string // Required - tablet ID to query
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
	flag.StringVar(&tabletID, "tablet-id", "", "Tablet ID to query")
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
		ydb.With(config.WithGrpcOptions(grpc.WithDisableServiceConfig())),
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

// executeQuery runs the query with the specified tablet ID and returns an iterator over the results
func executeQuery(parentCtx context.Context, logger *zap.Logger, ydbDriver *ydb.Driver, cfg *Config) (*tabletRows, error) {
	// The query to execute - selecting just a constant instead of all columns
	queryText := fmt.Sprintf("SELECT 0 FROM `%s` WITH TabletId='%s'", cfg.Table, cfg.TabletID)

	logger.Info("executing query",
		zap.String("query", queryText),
		zap.String("table", cfg.Table))

	rowsChan := make(chan *tabletRows, 1)

	finalErr := ydbDriver.Query().Do(parentCtx, func(ctx context.Context, session query.Session) error {
		var queryOpts []query.ExecuteOption

		if cfg.ResourcePool != "" {
			queryOpts = append(queryOpts, query.WithResourcePool(cfg.ResourcePool))
		}

		result, err := session.Query(ctx, queryText, queryOpts...)
		if err != nil {
			return fmt.Errorf("query error: %w", err)
		}

		// Get the first result set to initialize the iterator
		resultSet, err := result.NextResultSet(parentCtx)
		if err != nil && !errors.Is(err, io.EOF) {
			if closeErr := result.Close(ctx); closeErr != nil {
				logger.Error("close stream result", zap.Error(closeErr))
			}

			return fmt.Errorf("next result set: %w", err)
		}

		rows := &tabletRows{
			ctx:           parentCtx,
			streamResult:  result,
			lastResultSet: resultSet,
		}

		select {
		case rowsChan <- rows:
			return nil
		case <-ctx.Done():
			if closeErr := result.Close(ctx); closeErr != nil {
				logger.Error("close stream result", zap.Error(closeErr))
			}

			return ctx.Err()
		}
	}, query.WithIdempotent())
	if finalErr != nil {
		return nil, fmt.Errorf("execute query: %w", finalErr)
	}

	select {
	case rows := <-rowsChan:
		return rows, nil
	case <-parentCtx.Done():
		return nil, parentCtx.Err()
	}
}

// processRows processes the rows from the iterator and returns the count
func processRows(logger *zap.Logger, rows *tabletRows) (int, error) {
	var (
		rowCount       = 0
		dummyValue     int
		queryStartTime = time.Now()
	)

	// Process all result sets
	for cont := true; cont; cont = rows.NextResultSet() {
		logger.Info("processing result set", zap.Duration("elapsed_time", time.Since(queryStartTime)))

		// Process all rows in the current result set
		for rows.Next() {
			if err := rows.Scan(&dummyValue); err != nil {
				return 0, fmt.Errorf("scan row: %w", err)
			}

			rowCount++

			// Log progress every 1 million rows
			if rowCount > 0 && rowCount%1000000 == 0 {
				logger.Info("query progress",
					zap.Int("rows_processed", rowCount),
					zap.Duration("elapsed_time", time.Since(queryStartTime)))
			}
		}

		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("rows error: %w", err)
		}
	}

	return rowCount, nil
}

func main() {
	cfg, err := parseFlags()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	logger := common.NewDefaultLogger()

	defer func() {
		_ = logger.Sync()
	}()

	ctx := context.Background()

	ydbDriver, err := makeDriver(ctx, logger, cfg.Endpoint, cfg.Database, cfg.Token, cfg.UseTLS)
	if err != nil {
		logger.Fatal("failed to create YDB driver", zap.Error(err))
	}

	defer func() {
		if err = ydbDriver.Close(context.Background()); err != nil {
			logger.Error("error closing YDB driver", zap.Error(err))
		}
	}()

	logger = logger.With(zap.String("tablet_id", cfg.TabletID))

	logger.Info("starting query execution",
		zap.String("table", cfg.Table),
		zap.String("tablet_id", cfg.TabletID))

	queryStartTime := time.Now()

	// Execute query and get iterator
	rows, err := executeQuery(ctx, logger, ydbDriver, cfg)
	if err != nil {
		logger.Fatal("failed to execute query", zap.Error(err))
	}

	defer func() {
		if err = rows.Close(); err != nil {
			logger.Error("error closing rows", zap.Error(err))
		}
	}()

	// Process rows and get count
	rowCount, err := processRows(logger, rows)
	if err != nil {
		logger.Fatal("failed to process rows", zap.Error(err))
	}

	queryDuration := time.Since(queryStartTime)
	logger.Info("query completed",
		zap.Int("total_rows", rowCount),
		zap.String("tablet_id", cfg.TabletID),
		zap.String("table", cfg.Table),
		zap.Duration("total_duration", queryDuration),
		zap.Float64("rows_per_second", float64(rowCount)/queryDuration.Seconds()))

	// Print the final count to stdout
	fmt.Printf("Total rows: %d\n", rowCount)
}
