package ydb

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"

	"github.com/spf13/cobra"
	"github.com/ydb-platform/ydb-go-sdk/v3/query"
	"go.uber.org/zap"
	"gonum.org/v1/gonum/stat"

	"github.com/ydb-platform/fq-connector-go/app/config"
	rdbms_utils "github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/utils"
	"github.com/ydb-platform/fq-connector-go/app/server/datasource/rdbms/ydb"
	"github.com/ydb-platform/fq-connector-go/common"
)

//nolint:gocyclo
func columnShardsDataDistribution(cmd *cobra.Command, _ []string) error {
	configPath, err := cmd.Flags().GetString(configFlag)
	if err != nil {
		return fmt.Errorf("get config flag: %v", err)
	}

	tableName, err := cmd.Flags().GetString(tableFlag)
	if err != nil {
		return fmt.Errorf("get table flag: %v", err)
	}

	var cfg config.TClientConfig

	if err = common.NewConfigFromPrototextFile[*config.TClientConfig](configPath, &cfg); err != nil {
		return fmt.Errorf("unknown instance: %w", err)
	}

	logger := common.NewDefaultLogger()
	defer func() {
		if err = logger.Sync(); err != nil {
			fmt.Println("failed to sync logger", err)
		}
	}()

	// override credentials if IAM-token provided
	common.MaybeInjectTokenToDataSourceInstance(cfg.DataSourceInstance)

	logger = common.AnnotateLoggerWithDataSourceInstance(logger, cfg.DataSourceInstance)

	ctx := context.Background()

	ydbConfig := &config.TYdbConfig{
		Mode:                  config.TYdbConfig_MODE_QUERY_SERVICE_NATIVE,
		OpenConnectionTimeout: "5s",
		PingConnectionTimeout: "5s",
	}

	connManager := ydb.NewConnectionManager(ydbConfig, rdbms_utils.ConnectionManagerBase{})
	cs, err := connManager.Make(&rdbms_utils.ConnectionParams{
		Ctx:                ctx,
		Logger:             logger,
		DataSourceInstance: cfg.DataSourceInstance,
		MaxConnections:     1,
	})

	if err != nil {
		return fmt.Errorf("make connection: %v", err)
	}

	defer connManager.Release(ctx, logger, cs)

	databaseName, _ := cs[0].From()
	prefix := path.Join(databaseName, tableName)
	driver := cs[0].(ydb.Connection).Driver()

	var shardIDs []uint64

	err = driver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
		queryText := fmt.Sprintf("SELECT DISTINCT(TabletId) FROM `%s/.sys/primary_index_stats`", prefix)

		result, errInner := s.Query(ctx, queryText)
		if errInner != nil {
			return fmt.Errorf("query: %w", errInner)
		}

		for {
			resultSet, errInner := result.NextResultSet(ctx)
			if errInner != nil {
				if errors.Is(errInner, io.EOF) {
					break
				}

				return fmt.Errorf("next result set: %w", errInner)
			}

			var tabletId uint64

			for {
				r, errInner := resultSet.NextRow(ctx)
				if errInner != nil {
					if errors.Is(errInner, io.EOF) {
						break
					}

					return fmt.Errorf("next result set: %w", errInner)
				}

				if errInner := r.Scan(&tabletId); errInner != nil {
					return fmt.Errorf("row scan: %w", errInner)
				}

				shardIDs = append(shardIDs, tabletId)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("query: %v", err)
	}

	type rowsCountResult struct {
		shardId   uint64
		totalRows uint64
		err       error
	}

	resultChan := make(chan rowsCountResult, len(shardIDs))

	for _, shardId := range shardIDs {
		go func(shardId uint64) {
			err := driver.Query().Do(ctx, func(ctx context.Context, s query.Session) error {
				queryText := fmt.Sprintf("SELECT COUNT(*) FROM `%s` WITH (ShardId=\"%d\")", prefix, shardId)

				result, errInner := s.Query(ctx, queryText)
				if errInner != nil {
					return fmt.Errorf("query: %w", errInner)
				}

				for {
					resultSet, errInner := result.NextResultSet(ctx)
					if errInner != nil {
						if errors.Is(errInner, io.EOF) {
							break
						}

						return fmt.Errorf("next result set: %w", errInner)
					}

					for {
						r, errInner := resultSet.NextRow(ctx)
						if errInner != nil {
							if errors.Is(errInner, io.EOF) {
								break
							}

							return fmt.Errorf("next row: %w", errInner)
						}

						var records uint64

						if errInner := r.Scan(&records); errInner != nil {
							return fmt.Errorf("row scan: %w", errInner)
						}

						logger.Info("rows count result", zap.Uint64("shard_id", shardId), zap.Uint64("records", records))

						resultChan <- rowsCountResult{shardId: shardId, totalRows: records}
					}
				}

				return nil
			})

			if err != nil {
				resultChan <- rowsCountResult{shardId: shardId, err: err}
			}
		}(shardId)
	}

	// how many totalRowsPerShard in each shard
	var totalRowsPerShard []float64

	for range shardIDs {
		result := <-resultChan

		if result.err != nil {
			return fmt.Errorf("query: %v", result.err)
		}

		totalRowsPerShard = append(totalRowsPerShard, float64(result.totalRows))
	}

	mean := stat.Mean(totalRowsPerShard, nil)
	if mean == 0 {
		return fmt.Errorf("coefficient of variation is undefined for mean = 0")
	}

	stdDev := stat.StdDev(totalRowsPerShard, nil)
	cv := (stdDev / mean) * 100 // CV as a percentage

	fmt.Printf("Mean: %.2f\n", mean)
	fmt.Printf("Standard Deviation: %.2f\n", stdDev)
	fmt.Printf("Coefficient of Variation: %.2f%%\n", cv)

	return nil
}
