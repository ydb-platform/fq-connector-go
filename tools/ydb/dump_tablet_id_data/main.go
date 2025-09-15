package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
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
	TabletID     string // Optional - if not provided, all tablet IDs will be queried
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
	flag.StringVar(&tabletID, "tablet-id", "", "Tablet ID to query (optional - if not provided, all tablet IDs will be queried)")
	flag.StringVar(&resourcePool, "resource-pool", "", "Resource pool for YDB queries")
	flag.BoolVar(&useTLS, "tls", true, "Use TLS for YDB connection (grpcs:// instead of grpc://)")

	flag.Parse()

	if endpoint == "" || database == "" || table == "" {
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

// executeQuery runs the query with the specified tablet ID and returns the number of rows processed
func executeQuery(ctx context.Context, logger *zap.Logger, ydbDriver *ydb.Driver, config *Config) (int, error) {
	// The query to execute - selecting just a constant instead of all columns
	queryText := fmt.Sprintf(
		"SELECT 0 FROM `%s` WITH TabletId='%s'",
		config.Table, config.TabletID)

	logger.Info("executing query",
		zap.String("query", queryText),
		zap.String("table", config.Table))

	// Log the start time for tracking query duration
	queryStartTime := time.Now()

	rowCount := 0

	// Create variable to scan into with proper type outside the loop
	var dummyValue int

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
				if errors.Is(err, io.EOF) {
					logger.Debug("NextResultSet EOF")
					break
				}
				return fmt.Errorf("next result set: %w", err)
			}

			// No need to print column names, we're just counting rows

			// Log the start of processing this result set with more details
			logger.Info("processing result set",
				zap.String("table", config.Table),
				zap.Duration("elapsed_time", time.Since(queryStartTime)))

			// Initialize a counter for this result set
			resultSetRowCount := 0

			// Create a ticker for more frequent logging
			logTicker := time.NewTicker(5 * time.Second)
			defer logTicker.Stop()

			for {
				row, err := resultSet.NextRow(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						logger.Debug("NextRow EOF")
						break
					}

					return fmt.Errorf("next row: %w", err)
				}

				// Scan the row into dummy variable - we only need to count rows
				if err := row.Scan(&dummyValue); err != nil {
					return fmt.Errorf("scan row: %w", err)
				}

				rowCount++
				resultSetRowCount++

				// Log progress every 1 million rows
				if rowCount > 0 && rowCount%1000000 == 0 {
					logger.Info("query progress",
						zap.Int("rows_processed", rowCount),
						zap.Duration("elapsed_time", time.Since(queryStartTime)))
				}
			}
		}

		return nil
	}, query.WithIdempotent())

	if err != nil {
		return 0, fmt.Errorf("execute query: %w", err)
	}

	// Print the final count to stdout
	fmt.Printf("Total rows: %d\n", rowCount)
	return rowCount, nil
}

// getTabletIDs retrieves all tablet IDs for a given table
func getTabletIDs(ctx context.Context, logger *zap.Logger, ydbDriver *ydb.Driver, config *Config) ([]uint64, error) {
	var tabletIDs []uint64

	prefix := path.Join(config.Database, config.Table)
	queryText := fmt.Sprintf("SELECT DISTINCT(TabletId) FROM `%s/.sys/primary_index_stats`", prefix)

	logger.Info("discovering tablet IDs", zap.String("query", queryText))

	err := ydbDriver.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		result, err := session.Query(ctx, queryText)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}

		defer result.Close(ctx)

		for {
			resultSet, err := result.NextResultSet(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("next result set: %w", err)
			}

			var tabletID uint64

			for {
				row, err := resultSet.NextRow(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}
					return fmt.Errorf("next row: %w", err)
				}

				if err := row.Scan(&tabletID); err != nil {
					return fmt.Errorf("row scan: %w", err)
				}

				tabletIDs = append(tabletIDs, tabletID)
			}
		}

		return nil
	}, query.WithIdempotent())

	if err != nil {
		return nil, fmt.Errorf("querying tablet IDs: %w", err)
	}

	logger.Info("discovered tablet IDs", zap.Int("count", len(tabletIDs)))
	return tabletIDs, nil
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

	var tabletIDs []uint64

	// If tablet ID is provided, use it; otherwise, query all tablet IDs
	if config.TabletID != "" {
		// Convert string tablet ID to uint64
		var tabletID uint64
		_, err := fmt.Sscanf(config.TabletID, "%d", &tabletID)
		if err != nil {
			logger.Fatal("invalid tablet ID format", zap.String("tablet_id", config.TabletID), zap.Error(err))
		}
		tabletIDs = []uint64{tabletID}
	} else {
		// Query all tablet IDs
		tabletIDs, err = getTabletIDs(ctx, logger, ydbDriver, config)
		if err != nil {
			logger.Fatal("failed to get tablet IDs", zap.Error(err))
		}

		if len(tabletIDs) == 0 {
			logger.Warn("no tablet IDs found for the table", zap.String("table", config.Table))
			return
		}
	}

	logger.Info("starting query execution",
		zap.String("table", config.Table),
		zap.Int("tablet_count", len(tabletIDs)))

	totalStartTime := time.Now()
	totalRowCount := 0

	// Execute query for each tablet ID
	for _, tabletID := range tabletIDs {
		// Create a tablet-specific config
		tabletConfig := *config
		tabletConfig.TabletID = fmt.Sprintf("%d", tabletID)

		// Create a logger with tablet ID context
		tabletLogger := logger.With(zap.String("tablet_id", tabletConfig.TabletID))

		tabletLogger.Info("processing tablet")

		queryStartTime := time.Now()
		rowCount, err := executeQuery(ctx, tabletLogger, ydbDriver, &tabletConfig)
		if err != nil {
			tabletLogger.Error("failed to execute query", zap.Error(err))
			continue
		}

		queryDuration := time.Since(queryStartTime)
		tabletLogger.Info("tablet query completed",
			zap.Int("rows", rowCount),
			zap.Duration("duration", queryDuration),
			zap.Float64("rows_per_second", float64(rowCount)/queryDuration.Seconds()))

		totalRowCount += rowCount
	}

	totalDuration := time.Since(totalStartTime)
	logger.Info("all queries completed",
		zap.Int("total_rows", totalRowCount),
		zap.Int("tablet_count", len(tabletIDs)),
		zap.String("table", config.Table),
		zap.Duration("total_duration", totalDuration),
		zap.Float64("rows_per_second", float64(totalRowCount)/totalDuration.Seconds()))
}
