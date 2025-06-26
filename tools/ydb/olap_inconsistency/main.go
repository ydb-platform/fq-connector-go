package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/balancers"
	"github.com/ydb-platform/ydb-go-sdk/v3/config"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/ydb-platform/fq-connector-go/common"
)

// Query template with TabletId placeholder
const queryTemplate = `
DECLARE $p0 AS Timestamp;
DECLARE $p1 AS Timestamp;

$build_labels = ($j) -> {
	$y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { return StartsWith($x.0, "labels.") });
	$g = ListMap($f, ($x) -> { return (substring($x.0, 7), $x.1) });
	return Yson::SerializeJson(Yson::From(ToDict($g)));
};

$build_pure_meta = ($j) -> {
	$y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { return StartsWith($x.0, "meta.")});
	$g = ListMap($f, ($x) -> { return (substring($x.0, 5), $x.1) });
    return $g;
};

$hostname_keys = AsList(
    "host", "hostname", "host.name"
);

$trace_id_keys = AsList(
    "trace.id", "trace_id", "traceId", "traceID",
);

$span_id_keys = AsList(
    "span.id", "span_id", "spanId", "spanID",
);

$excluded_from_meta = ListExtend(
    $hostname_keys,
    $trace_id_keys,
    $span_id_keys
);

$build_other_meta = ($j) -> {
	$y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { 
        return 
            NOT StartsWith($x.0, "labels.") 
                AND 
            NOT StartsWith($x.0, "meta.")
                AND
            $x.0 NOT IN $excluded_from_meta
    });
	$g = ListMap($f, ($x) -> { return ($x.0, $x.1) });
    return $g;
};

$build_meta = ($j) -> {
    $pure = $build_pure_meta($j);
    $other = $build_other_meta($j);
    return Yson::SerializeJson(Yson::From(ToDict(ListExtend($pure, $other))));
};

$build_hostname = ($j) -> {
    $y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { return $x.0 IN $hostname_keys });
    return CAST(Yson::ConvertToString($f[0].1) AS Utf8);
};

$build_span_id = ($j) -> {
    $y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { return $x.0 IN $span_id_keys });
    return CAST(Yson::ConvertToString($f[0].1) AS Utf8);
};

$build_trace_id = ($j) -> {
    $y = Yson::ParseJson(CAST ($j as STRING));
	$a = DictItems(Yson::ConvertToDict($y));
	$f = ListFilter($a, ($x) -> { return $x.0 IN $trace_id_keys });
    return CAST(Yson::ConvertToString($f[0].1) AS Utf8);
};

$build_level = ($src) -> {
    RETURN CAST(
        CASE $src
            WHEN 1 THEN "TRACE"
            WHEN 2 THEN "DEBUG"
            WHEN 3 THEN "INFO"
            WHEN 4 THEN "WARN"
            WHEN 5 THEN "ERROR"
            WHEN 6 THEN "FATAL"
            ELSE "UNKNOWN"
        END AS Utf8
    );
};

SELECT
    CAST("aoe3cidh5dfee2s6cqu5" AS Utf8) AS cluster,
    $build_hostname(json_payload) AS hostname,
    json_payload,
    $build_labels(json_payload) AS labels,
    $build_level(level) AS level,
    message,
    CAST("aoeoqusjtbo4m549jrom" AS Utf8) AS project,
    CAST("af3p40c4vf9jqpb81qvm" AS Utf8) AS service,
    $build_span_id(json_payload) AS span_id,
    timestamp,
    $build_trace_id(json_payload) AS trace_id
FROM` + "`%s`" + `
WITH TabletId='%s'
WHERE (COALESCE((timestamp >= $p0), false) AND COALESCE((timestamp < $p1), false))
`

// getTabletIDs queries YDB to get the tablet IDs for a specific table
func getTabletIDs(ctx context.Context, logger *zap.Logger, ydbDriver *ydb.Driver, tablePath string) ([]string, error) {
	var tabletIDs []string

	queryText := fmt.Sprintf("SELECT DISTINCT(TabletId) FROM `%s/.sys/primary_index_stats`", tablePath)

	logger.Info("discovering tablet IDs", zap.String("query", queryText))

	err := ydbDriver.Query().Do(ctx, func(ctx context.Context, session query.Session) error {
		result, err := session.Query(ctx, queryText)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}

		for {
			resultSet, err := result.NextResultSet(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return fmt.Errorf("next result set: %w", err)
			}

			var tabletId uint64

			for {
				r, err := resultSet.NextRow(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return fmt.Errorf("next row: %w", err)
				}

				if err := r.Scan(&tabletId); err != nil {
					return fmt.Errorf("row scan: %w", err)
				}

				tabletIDs = append(tabletIDs, fmt.Sprintf("%d", tabletId))
			}
		}

		return nil
	}, query.WithIdempotent())

	if err != nil {
		return nil, fmt.Errorf("querying tablet IDs: %w", err)
	}

	logger.Info("discovered tablet IDs", zap.Int("total", len(tabletIDs)), zap.Strings("tablet_ids", tabletIDs))

	return tabletIDs, nil
}

// Config holds the application configuration
type Config struct {
	Endpoint     string
	Database     string
	Table        string
	Token        string
	Interval     time.Duration
	StartTime    time.Time
	EndTime      time.Time
	ResourcePool string
}

// parseFlags parses command-line flags and returns the configuration
func parseFlags() (*Config, error) {
	var (
		endpoint     string
		database     string
		table        string
		token        string
		interval     int
		startTimeStr string
		endTimeStr   string
		resourcePool string
	)

	flag.StringVar(&endpoint, "endpoint", "localhost:2136", "YDB endpoint")
	flag.StringVar(&database, "database", "/local", "YDB database path")
	flag.StringVar(&table, "table", "", "YDB table name")
	flag.StringVar(&token, "token", "", "IAM token for authentication")
	flag.IntVar(&interval, "interval", 5, "Query interval in seconds")
	flag.StringVar(&startTimeStr, "start", "", "Start time for query in RFC3339 format")
	flag.StringVar(&endTimeStr, "end", "", "End time for query in RFC3339 format")
	flag.StringVar(&resourcePool, "resource-pool", "", "Resource pool for YDB queries")

	flag.Parse()

	if token == "" || endpoint == "" || database == "" || table == "" || startTimeStr == "" || endTimeStr == "" || resourcePool == "" {
		return nil, fmt.Errorf(
			"usage: app -endpoint=<endpoint> -database=<database> -table=<table-name> -token=<token> " +
				"-interval=<interval> -start=<start-time> -end=<end-time> -resource-pool=<resource-pool>")
	}

	startTime, err := time.Parse(time.RFC3339, startTimeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid start time format: %w", err)
	}

	endTime, err := time.Parse(time.RFC3339, endTimeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid end time format: %w", err)
	}

	if endTime.Before(startTime) {
		return nil, fmt.Errorf("end time must be after start time")
	}

	return &Config{
		Endpoint:     endpoint,
		Database:     database,
		Table:        table,
		Token:        token,
		Interval:     time.Duration(interval) * time.Second,
		StartTime:    startTime,
		EndTime:      endTime,
		ResourcePool: resourcePool,
	}, nil
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigChan
		logger.Info("received termination signal, shutting down...")
		cancel()
	}()

	ydbDriver, err := makeDriver(ctx, logger, cfg.Endpoint, cfg.Database, cfg.Token)
	if err != nil {
		logger.Fatal("failed to create YDB driver", zap.Error(err))
	}

	defer func() {
		if err = ydbDriver.Close(context.Background()); err != nil {
			logger.Error("error closing YDB driver", zap.Error(err))
		}
	}()

	// Get the full table path
	tablePath := path.Join(cfg.Database, cfg.Table)

	// Dynamically get tablet IDs
	tabletIDs, err := getTabletIDs(ctx, logger, ydbDriver, tablePath)
	if err != nil {
		logger.Fatal("failed to get tablet IDs", zap.Error(err))
	}

	if len(tabletIDs) == 0 {
		logger.Fatal("no tablet IDs found for the specified table")
	}

	logger.Info("starting to monitor tablet IDs",
		zap.Strings("tablet_ids", tabletIDs),
		zap.String("resource_pool", cfg.ResourcePool))

	inconsistencyFound := make(chan string, 1)

	var wg sync.WaitGroup

	for _, tabletID := range tabletIDs {
		wg.Add(1)

		monitorFunc := func(id string) {
			defer wg.Done()
			monitorTabletID(
				ctx, logger, ydbDriver, id, cfg.Interval,
				inconsistencyFound, cfg.StartTime, cfg.EndTime, cfg.Table, cfg.ResourcePool,
			)
		}

		go monitorFunc(tabletID)
	}

	select {
	case <-ctx.Done():
		logger.Info("context canceled, waiting for goroutines to finish...")
	case tabletID := <-inconsistencyFound:
		logger.Warn("inconsistency found in tablet ID", zap.String("tablet_id", tabletID))
		cancel()
	}

	wg.Wait()
	logger.Info("all goroutines finished, exiting...")
}

// makeDriver creates and returns a YDB driver
func makeDriver(ctx context.Context, logger *zap.Logger, endpoint, database, token string) (*ydb.Driver, error) {
	ydbOptions := []ydb.Option{
		ydb.WithAccessTokenCredentials(token),
		ydb.WithDialTimeout(5 * time.Second),
		ydb.WithBalancer(balancers.SingleConn()),
		ydb.With(config.WithGrpcOptions(grpc.WithDisableServiceConfig())),
	}

	dsn := fmt.Sprintf("grpcs://%s%s", endpoint, database)

	logger.Info("connecting to YDB",
		zap.String("dsn", dsn),
		zap.String("auth", "IAM token"))

	ydbDriver, err := ydb.Open(ctx, dsn, ydbOptions...)
	if err != nil {
		return nil, fmt.Errorf("open driver error: %w", err)
	}

	return ydbDriver, nil
}

// monitorTabletID runs the query periodically for a specific tablet ID and checks for inconsistencies
//
//nolint:revive
func monitorTabletID(
	ctx context.Context,
	logger *zap.Logger,
	ydbDriver *ydb.Driver,
	tabletID string,
	interval time.Duration,
	inconsistencyFound chan<- string,
	startTime, endTime time.Time,
	table string,
	resourcePool string,
) {
	logger.Info("starting monitoring for tablet ID", zap.String("tablet_id", tabletID))

	var (
		err                    error
		queryCounter           int
		firstRun               = true
		lastResult, currResult *executeQueryResult
	)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("context canceled for tablet ID",
				zap.String("tablet_id", tabletID),
				zap.Int("total_queries", queryCounter))

			return
		case <-ticker.C:
			queryCounter++

			currResult, err = executeQuery(ctx, ydbDriver, tabletID, startTime, endTime, table, resourcePool)
			if err != nil {
				logger.Error("error executing query",
					zap.String("tablet_id", tabletID),
					zap.Int("query_num", queryCounter),
					zap.Error(err))

				continue
			}

			logger.Info("query executed",
				zap.String("tablet_id", tabletID),
				zap.Int("query_num", queryCounter),
				zap.Int("row_count", currResult.rowCount))

			if firstRun {
				lastResult = currResult
				firstRun = false

				continue
			}

			if currResult.rowCount != lastResult.rowCount {
				logger.Warn("inconsistency detected",
					zap.String("tablet_id", tabletID),
					zap.Int("query_num", queryCounter),
					zap.Int("last_row_count", lastResult.rowCount),
					zap.Int("curr_row_count", currResult.rowCount),
					zap.Time("last_start_time", lastResult.queryStartTime),
					zap.Time("curr_start_time", currResult.queryStartTime),
				)

				if err := dumpQueryPlanToFile(tabletID, lastResult.queryStartTime, lastResult.queryPlan); err != nil {
					logger.Error("failed to dump last query plan", zap.Error(err))
				}

				if err := dumpQueryPlanToFile(tabletID, currResult.queryStartTime, currResult.queryPlan); err != nil {
					logger.Error("failed to dump current query plan", zap.Error(err))
				}

				select {
				case inconsistencyFound <- tabletID:
				default:
				}

				return
			}

			lastResult = currResult
		}
	}
}

type executeQueryResult struct {
	queryStartTime time.Time
	queryPlan      string
	rowCount       int
}

// executeQuery runs the query with a specific tablet ID and returns the number of rows
func executeQuery(
	ctx context.Context,
	ydbDriver *ydb.Driver,
	tabletID string,
	startTime, endTime time.Time,
	table string,
	resourcePool string,
) (*executeQueryResult, error) {
	queryText := fmt.Sprintf(queryTemplate, table, tabletID)

	paramsBuilder := ydb.ParamsBuilder()
	paramsBuilder = paramsBuilder.Param("$p0").Timestamp(startTime)
	paramsBuilder = paramsBuilder.Param("$p1").Timestamp(endTime)

	queryStartTime := time.Now()
	rowCount := 0

	var queryPlan string

	err := ydbDriver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		result, err := s.Query(
			ctx,
			queryText,
			query.WithParameters(paramsBuilder.Build()),
			query.WithResourcePool(resourcePool),
			query.WithStatsMode(query.StatsModeProfile, func(stats query.Stats) {
				queryPlan = stats.QueryPlan()
			}),
		)
		if err != nil {
			return fmt.Errorf("query error: %w", err)
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

			for {
				_, err := resultSet.NextRow(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return fmt.Errorf("next row: %w", err)
				}

				rowCount++
			}
		}

		return nil
	}, query.WithIdempotent())

	if err != nil {
		return nil, fmt.Errorf("execute query: %w", err)
	}

	return &executeQueryResult{
		queryStartTime: queryStartTime,
		queryPlan:      queryPlan,
		rowCount:       rowCount,
	}, nil
}

// dumpQueryPlanToFile writes the query plan to a file
func dumpQueryPlanToFile(tabletID string, queryStartTime time.Time, queryPlan string) error {
	fileName := fmt.Sprintf("tablet_id_%s_start_time_%s.txt", tabletID, queryStartTime.Format("20060102_150405.000"))

	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("create file: %w", err)
	}
	defer file.Close()

	_, err = file.WriteString(queryPlan)
	if err != nil {
		return fmt.Errorf("write to file: %w", err)
	}

	return nil
}
