package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"time"

	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"

	"github.com/ydb-platform/fq-connector-go/app/client/utils"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	rdbms_ydb "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
)

type columnShardBenchmarkSelectResult struct {
	shardID     uint64
	rows        uint64
	elapsedTime time.Duration
}

func columnShardBenchmarkSelect(cmd *cobra.Command, _ []string) error {
	preset, err := utils.MakePreset(cmd)
	if err != nil {
		return fmt.Errorf("make preset: %w", err)
	}

	defer preset.Close()

	ctx := context.Background()

	connManager := rdbms_ydb.NewConnectionManager(ydbConfig, rdbms_utils.ConnectionManagerBase{})
	cs, err := connManager.Make(&rdbms_utils.ConnectionParams{
		Ctx:                ctx,
		Logger:             preset.Logger,
		DataSourceInstance: preset.Cfg.DataSourceInstance,
		MaxConnections:     1,
	})

	if err != nil {
		return fmt.Errorf("make connection: %v", err)
	}

	defer connManager.Release(ctx, preset.Logger, cs)

	databaseName, _ := cs[0].From()
	prefix := path.Join(databaseName, preset.TableName)
	driver := cs[0].(rdbms_ydb.Connection).Driver()

	shardIDs, err := getColumnShardIDs(ctx, driver, prefix)
	if err != nil {
		return fmt.Errorf("get column shard ids: %w", err)
	}

	var results []*columnShardBenchmarkSelectResult

	logger := preset.Logger

	for i, shardID := range shardIDs {
		logger.Debug(
			"column shard benchmarking started",
			zap.Int("shard_index", i),
			zap.Int("shards_total", len(shardIDs)),
			zap.Uint64("shard_id", shardID),
		)

		result, err := columnShardBenchmarkSelectSingleShard(ctx, driver, prefix, shardID)
		if err != nil {
			return fmt.Errorf("benchmark single shard: %w", err)
		}

		logger.Debug(
			"column shard benchmarking finished",
			zap.Int("shard_index", i),
			zap.Int("shards_total", len(shardIDs)),
			zap.Uint64("shard_id", shardID),
			zap.Duration("elapsed_time", result.elapsedTime),
			zap.Uint64("rows", result.rows),
		)

		results = append(results, result)
	}

	// sort by elapsed time
	sort.Slice(results, func(i, j int) bool {
		return results[i].elapsedTime < results[j].elapsedTime
	})

	for i, result := range results {
		fmt.Println(i, result.shardID, result.rows, result.elapsedTime)
	}

	return nil
}

func columnShardBenchmarkSelectSingleShard(
	ctx context.Context,
	driver *ydb.Driver,
	tablePrefix string,
	shardID uint64,
) (*columnShardBenchmarkSelectResult, error) {
	var benchResult columnShardBenchmarkSelectResult

	err := driver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		benchResult.rows = 0 // clean up in case of retry

		start := time.Now()

		queryText := fmt.Sprintf("SELECT * FROM `%s` WITH (ShardId=\"%d\")", tablePrefix, shardID)

		result, err := s.Query(ctx, queryText)
		if err != nil {
			return fmt.Errorf("query: %w", err)
		}

		for {
			rs, err := result.NextResultSet(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}

				return fmt.Errorf("next result set: %w", err)
			}

			for {
				_, err := rs.NextRow(ctx)
				if err != nil {
					if errors.Is(err, io.EOF) {
						break
					}

					return fmt.Errorf("next row: %w", err)
				}

				benchResult.rows++
			}
		}

		benchResult.elapsedTime = time.Since(start)

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("query do: %w", err)
	}

	return &benchResult, nil
}
