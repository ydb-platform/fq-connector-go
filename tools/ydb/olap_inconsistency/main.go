package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
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
FROM ` +
	"`logs/origin/aoeoqusjtbo4m549jrom/aoe3cidh5dfee2s6cqu5/af3p40c4vf9jqpb81qvm`" +
	`
WITH TabletId='%s' 
WHERE (COALESCE((timestamp >= $p0), false) AND COALESCE((timestamp < $p1), false))
`

// List of all possible TabletIds
var tabletIDs = []string{
	"72075186235526786",
	"72075186235526433",
	"72075186235526695",
	"72075186235526773",
	"72075186235526069",
	"72075186235526677",
	"72075186235526756",
	"72075186235526324",
	"72075186235526828",
	"72075186235526818",
}

func main() {
	var (
		endpoint  string
		database  string
		token     string
		interval  int
		startTime string
		endTime   string
	)

	flag.StringVar(&endpoint, "endpoint", "localhost:2136", "YDB endpoint")
	flag.StringVar(&database, "database", "/local", "YDB database path")
	flag.StringVar(&token, "token", "", "IAM token for authentication")
	flag.IntVar(&interval, "interval", 5, "Query interval in seconds")
	flag.StringVar(&startTime, "start", "", "Start time for query in RFC3339 format")
	flag.StringVar(&endTime, "end", "", "End time for query in RFC3339 format")

	flag.Parse()

	if token == "" || endpoint == "" || database == "" {
		fmt.Println(
			"Usage: " +
				"app -endpoint=<endpoint> -database=<database> -token=<token> " +
				"-interval=<interval> -start=<start-time> -end=<end-time>")
		os.Exit(1)
	}

	start, err := time.Parse(time.RFC3339, startTime)
	if err != nil {
		fmt.Printf("Invalid start time format: %v\n", err)
		os.Exit(1)
	}

	end, err := time.Parse(time.RFC3339, endTime)
	if err != nil {
		fmt.Printf("Invalid end time format: %v\n", err)
		os.Exit(1)
	}

	if end.Before(start) {
		fmt.Println("End time must be after start time")
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

	ydbDriver, err := makeDriver(ctx, logger, endpoint, database, token)
	if err != nil {
		logger.Fatal("failed to create YDB driver", zap.Error(err))
	}

	defer func() {
		if err := ydbDriver.Close(context.Background()); err != nil {
			logger.Error("error closing YDB driver", zap.Error(err))
		}
	}()

	inconsistencyFound := make(chan string, 1)

	var wg sync.WaitGroup

	for _, tabletID := range tabletIDs {
		wg.Add(1)

		monitorFunc := func(id string) {
			defer wg.Done()
			monitorTabletID(ctx, logger, ydbDriver, id, time.Duration(interval)*time.Second, inconsistencyFound, start, end)
		}

		go monitorFunc(tabletID)
	}

	select {
	case <-ctx.Done():
		logger.Info("context canceled, waiting for goroutines to finish...")
	case tabletID := <-inconsistencyFound:
		logger.Info("inconsistency found in tablet ID", zap.String("tablet_id", tabletID))
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

	dsn := fmt.Sprintf("grpc://%s%s", endpoint, database)

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
func monitorTabletID(
	ctx context.Context,
	logger *zap.Logger,
	ydbDriver *ydb.Driver,
	tabletID string,
	interval time.Duration,
	inconsistencyFound chan<- string,
	startTime, endTime time.Time,
) {
	logger.Info("starting monitoring for tablet ID", zap.String("tablet_id", tabletID))

	var lastRowCount int

	var firstRun = true

	var queryCounter int

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

			rowCount, err := executeQuery(ctx, ydbDriver, tabletID, startTime, endTime)
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
				zap.Int("row_count", rowCount))

			if firstRun {
				lastRowCount = rowCount
				firstRun = false

				continue
			}

			if rowCount != lastRowCount {
				logger.Info("inconsistency detected",
					zap.String("tablet_id", tabletID),
					zap.Int("query_num", queryCounter),
					zap.Int("previous_count", lastRowCount),
					zap.Int("current_count", rowCount))

				select {
				case inconsistencyFound <- tabletID:
				default:
				}

				return
			}

			lastRowCount = rowCount
		}
	}
}

// executeQuery runs the query with a specific tablet ID and returns the number of rows
func executeQuery(ctx context.Context, ydbDriver *ydb.Driver, tabletID string, startTime, endTime time.Time) (int, error) {
	queryText := fmt.Sprintf(queryTemplate, tabletID)

	paramsBuilder := ydb.ParamsBuilder()
	paramsBuilder = paramsBuilder.Param("$p0").Timestamp(startTime)
	paramsBuilder = paramsBuilder.Param("$p1").Timestamp(endTime)

	rowCount := 0

	err := ydbDriver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		result, err := s.Query(
			ctx,
			queryText,
			query.WithParameters(paramsBuilder.Build()),
			query.WithResourcePool("yandex_query_pool"),
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
		return 0, fmt.Errorf("execute query: %w", err)
	}

	return rowCount, nil
}
